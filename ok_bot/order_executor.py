import asyncio
import concurrent
import logging
import pprint

from . import constants, singleton


class OrderExecutionResult:
    def __init__(self, order_id=None, amount=None, fulfilled_quantity=None):
        self.order_id = order_id
        self.amount = amount
        self.fulfilled_quantity = fulfilled_quantity

    @property
    def succeeded(self):
        return ((self.order_id is not None) and
                (self.amount > 0) and
                (self.fulfilled_quantity == self.amount))

    def __str__(self):
        return f'{self.fulfilled_quantity}/{self.amount} ({self.order_id})'


class OrderRevoker:
    def __init__(self, order_id, instrument_id, logger):
        self._order_id = order_id
        self._instrument_id = instrument_id
        self._logger = logger

    async def revoke_guaranteed(self):
        """Returns fulfilled quantity"""
        while True:
            await self._send_revoke_request()

            order_info = await singleton.rest_api.get_order_info(
                self._order_id, self._instrument_id)
            self._logger.info(
                '[ORDER INFO AFTER REVOKE]\n%s',
                pprint.pformat(order_info))

            final_status = int(order_info.get('status', None))
            if final_status == constants.ORDER_STATUS_CODE__CANCELLED:
                return 0
            elif final_status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
                return int(order_info.get('filled_qty', None))
            elif final_status == constants.ORDER_STATUS_CODE__FULFILLED:
                return int(order_info.get('filled_qty', None))
            elif final_status == constants.ORDER_STATUS_CODE__CANCEL_IN_PROCESS:
                logging.info('[CANCEL IN PROCESS]: sleep 1 sec')
                await asyncio.sleep(1)
            else:
                logging.warning(
                    '[UNKNOWN ORDER STATUS] %s, sleep 1 sec', final_status)
                await asyncio.sleep(1)

    async def _send_revoke_request(self):
        """Returns True if the http response confirms the request was done."""
        self._logger.info('[REVOKE PENDING ORDER] %s', self._order_id)
        ret = await singleton.rest_api.revoke_order(
            self._instrument_id, self._order_id)
        if ret.get('result', False) is True:
            assert int(ret.get('order_id', None)) == self._order_id
            self._logger.info('[REVOKE SUCCESSFUL] %s', self._order_id)
            return True
        elif int(ret.get('error_code', -1)) ==\
                constants.REST_API_ERROR_CODE__PENDING_ORDER_NOT_EXIST:
            self._logger.warning('[REVOKE ORDER NOT EXIST] %s', self._order_id)
            return False
        else:
            self._logger.warning(
                f'unexpected revoking order response:\n{pprint.pformat(ret)}')
            return False


class OrderAwaiter:
    def __init__(self, order_id, logger, timeout_sec, transaction_id=None):
        """Returns None if timeout otherwise fulfilled quantity."""
        self._order_id = order_id
        self._future = singleton.loop.create_future()
        self._logger = logger
        self._timeout_sec = timeout_sec
        self._transaction_id = transaction_id

    async def __aenter__(self):
        singleton.order_listener.subscribe(self._order_id, self)
        try:
            res = await asyncio.wait_for(
                self._future, timeout=self._timeout_sec)
        except concurrent.futures.TimeoutError:
            return None
        else:
            return res

    async def __aexit__(self, type, value, traceback):
        singleton.order_listener.unsubscribe(self._order_id, self)

        if type is not None:
            self._logger.critical(
                'exception within OrderAwaiter', exc_info=True)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        self._logger.info('[WEBSOCKET] %s order_pending', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        if self._future.done():
            return
        self._future.set_result(0)
        self._logger.info('[WEBSOCKET] %s order_cancelled', order_id)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        if self._future.done():
            return
        self._future.set_result(filled_qty)
        self._logger.info(
            '[WEBSOCKET] %s order_fulfilled, '
            'price: %s, price_avg: %s, size: %s, filled_qty: %s, fee: %s',
            order_id, price, price_avg, size, filled_qty, fee)
        singleton.db.async_update_order(
            order_id=order_id,
            transaction_id=self._transaction_id,
            comment='websocket_fulfilled',
            status=constants.ORDER_STATUS_CODE__FULFILLED,
            size=int(size),
            filled_qty=int(filled_qty),
            price=str(price),
            price_avg=str(price_avg),
            fee=str(fee),
            type=None,
            timestamp=None
        )

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        assert self._order_id == order_id
        self._logger.info(
            '[WEBSOCKET] %s order_partially_filled\n'
            'price_avg: %s, size: %s, filled_qty: %s',
            order_id, price_avg, size, filled_qty)
        singleton.db.async_update_order(
            order_id=order_id,
            transaction_id=self._transaction_id,
            comment='websocket_partially_filled',
            status=constants.ORDER_STATUS_CODE__PARTIALLY_FILLED,
            size=int(size),
            filled_qty=int(filled_qty),
            price=str(price),
            price_avg=str(price_avg),
            fee=str(fee),
            type=None,
            timestamp=None
        )


class OrderExecutor:
    def __init__(self,
                 instrument_id,
                 amount,
                 price,
                 timeout_sec,
                 is_market_order,
                 logger,
                 transaction_id=None):
        self._instrument_id = instrument_id
        self._amount = int(amount)
        self._price = price
        self._timeout_sec = timeout_sec
        self._is_market_order = is_market_order
        self._logger = logger
        self._transaction_id = transaction_id
        self._order_id = None

    def open_long_position(self) -> OrderExecutionResult:
        """Returns Future[OrderExecutionResult]"""
        return self._place_order(singleton.rest_api.open_long_order)

    def open_short_position(self) -> OrderExecutionResult:
        """Returns Future[OrderExecutionResult]"""
        return self._place_order(singleton.rest_api.open_short_order)

    def close_long_order(self) -> OrderExecutionResult:
        """Returns Future[OrderExecutionResult]"""
        return self._place_order(singleton.rest_api.close_long_order)

    def close_short_order(self) -> OrderExecutionResult:
        """Returns Future[OrderExecutionResult]"""
        return self._place_order(singleton.rest_api.close_short_order)

    async def _place_order(self, rest_request_functor) -> OrderExecutionResult:
        # TODO: add timeout_sec for rest api wait() as well.
        self._order_id, error_code = await rest_request_functor(
            self._instrument_id,
            self._amount,
            self._price,
            is_market_order=self._is_market_order
        )

        if self._order_id is None:
            self._logger.error(f'Failed to place order via REST API, '
                               f'error code: {error_code}')
            if error_code == constants.REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH:
                # Margin not enough, cool down
                singleton.trader.cool_down()
            return OrderExecutionResult()

        self._logger.info(
            f'{self._order_id} ({self._instrument_id}) order was created '
            f'({rest_request_functor.__name__})')
        singleton.db.async_update_order(
            order_id=self._order_id,
            transaction_id=self._transaction_id,
            comment='request_sent',
            status=None,
            size=self._amount,
            filled_qty=None,
            price=str(self._price),
            price_avg=None,
            fee=None,
            type=None,
            timestamp=None
        )

        order_awaiter = OrderAwaiter(
            order_id=self._order_id,
            logger=self._logger,
            timeout_sec=self._timeout_sec,
            transaction_id=self._transaction_id)
        async with order_awaiter as websocket_reported_fulfilled_quantity:
            if websocket_reported_fulfilled_quantity is None:
                self._logger.info(
                    f'[TIMEOUT] {self._order_id} ({self._instrument_id}) '
                    'cancelling the pending order')
                fulfilled_quantity = await OrderRevoker(
                    order_id=self._order_id,
                    instrument_id=self._instrument_id,
                    logger=self._logger).revoke_guaranteed()
                assert 0 <= fulfilled_quantity <= self._amount
                if fulfilled_quantity == self._amount:
                    self._logger.info(
                        f'[TIMEOUT -> FULFILLED] {fulfilled_quantity}, '
                        f'{self._order_id} ({self._instrument_id}) '
                        'order was resolved as fulfilled by REST API')
                elif fulfilled_quantity == 0:
                    self._logger.info(
                        f'[TIMEOUT CONFIRMED] '
                        f'{self._order_id} ({self._instrument_id}) '
                        'order was confirmed as cancelled by REST API, '
                        'will return timeout')
                else:
                    self._logger.info(
                        f'[TIMEOUT -> PARTIALLY FULFILLED] {fulfilled_quantity}, '
                        f'{self._order_id} ({self._instrument_id}) ')
            else:
                fulfilled_quantity = websocket_reported_fulfilled_quantity

        return OrderExecutionResult(
            order_id=self._order_id,
            amount=self._amount,
            fulfilled_quantity=fulfilled_quantity)


async def _testing_coroutine(instrument_id):
    await singleton.websocket.ready
    logging.info('start')

    executor = OrderExecutor(instrument_id,
                             amount=1,
                             price=140,
                             timeout_sec=0.1,
                             is_market_order=False,
                             logger=logging,
                             transaction_id='fake_transaction_id')

    logging.info('open_long_position has been called')
    order_status = await executor.open_long_position()
    logging.info('execution result: %s', order_status)


def _testing():
    from .logger import init_global_logger
    init_global_logger(log_level=logging.INFO, log_to_stderr=True)
    singleton.initialize_objects_with_mock_trader_and_dev_db(currency='ETH')
    singleton.websocket.book_listener = None  # test heartbeat in websocket_api
    singleton.loop.create_task(_testing_coroutine(
        instrument_id=singleton.schema.all_instrument_ids[0]))
    singleton.start_loop()


if __name__ == '__main__':
    _testing()
