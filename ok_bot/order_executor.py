import asyncio
import concurrent
import logging
import pprint
from collections import namedtuple

from . import constants, singleton

OpenPositionStatus = namedtuple('OpenPositionStatus',
                                ['result',  # boolean, successful or not.
                                 # detailed error message if failed.
                                 'message',
                                 ])

OPEN_POSITION_STATUS__SUCCEEDED = OpenPositionStatus(
    result=True, message='order fulfilled')
OPEN_POSITION_STATUS__UNKNOWN = OpenPositionStatus(
    result=False, message='unknown')
OPEN_POSITION_STATUS__REST_API = OpenPositionStatus(
    result=False, message='rest api http error')
OPEN_POSITION_STATUS__TIMEOUT = OpenPositionStatus(
    result=False, message='failed to fulfill in time')
OPEN_POSITION_STATUS__CANCELLED = OpenPositionStatus(
    result=False, message='order cancelled')


ORDER_AWAIT_STATUS__FULFILLED = 'fulfilled'
ORDER_AWAIT_STATUS__CANCELLED = 'cancelled'


def make_open_position_status(
        result: bool,
        message
) -> OpenPositionStatus:
    return OpenPositionStatus(result, message)


class OrderAwaiter:
    def __init__(self, order_id, logger, timeout_sec, transaction_id=None):
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
            self._logger.fatal('exception within OrderAwaiter', exc_info=True)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        self._logger.info('[WEBSOCKET] %s order_pending', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        if self._future.done():
            return
        self._future.set_result(ORDER_AWAIT_STATUS__CANCELLED)
        self._logger.info('[WEBSOCKET] %s order_cancelled', order_id)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        if self._future.done():
            return
        self._future.set_result(ORDER_AWAIT_STATUS__FULFILLED)
        assert self._order_id == order_id
        self._logger.info(
            '[WEBSOCKET] %s order_fulfilled\n'
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
        self._amount = amount
        self._price = price
        self._timeout_sec = timeout_sec
        self._is_market_order = is_market_order
        self._logger = logger
        self._transaction_id = transaction_id
        self._order_id = None

    def open_long_position(self):
        """Returns Future[OpenPositionStatus]"""
        return self._place_order(singleton.rest_api.open_long_order)

    def open_short_position(self):
        """Returns Future[OpenPositionStatus]"""
        return self._place_order(singleton.rest_api.open_short_order)

    def close_long_order(self):
        """Returns Future[OpenPositionStatus]"""
        return self._place_order(singleton.rest_api.close_long_order)

    def close_short_order(self):
        """Returns Future[OpenPositionStatus]"""
        return self._place_order(singleton.rest_api.close_short_order)

    async def _place_order(self, rest_request_functor):
        result = await self._async_place_order_and_await(rest_request_functor)
        if self._order_id:
            # Always postmortem order final status for any cases. This will make
            # sure the final order info written into DB is accurate and final.
            asyncio.create_task(self._post_log_order_final_status())
        return result

    async def _async_place_order_and_await(self, rest_request_functor):
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
            return make_open_position_status(
                False, {'type': 'API', 'error_code': error_code})

        self._logger.info(
            f'{self._order_id} ({self._instrument_id}) order was created '
            f'via {rest_request_functor.__name__}')
        singleton.db.async_update_order(
            order_id=self._order_id,
            transaction_id=self._transaction_id,
            comment='request_sent',
            status=None,
            size=int(self._amount),
            filled_qty=None,
            price=str(self._price),
            price_avg=None,
            fee=None,
            type=None,
            timestamp=None
        )

        async with OrderAwaiter(
                order_id=self._order_id,
                logger=self._logger,
                timeout_sec=self._timeout_sec,
                transaction_id=self._transaction_id) as status:
            if status is None:
                self._logger.info(
                    f'[TIMEOUT] {self._order_id} ({self._instrument_id}) '
                    'cancelling the pending order')

                # The resolved status is either FULFILLED(True) or
                # CANCELLED(False) successfully.
                result = await self._revoke_order_and_resolve_final_status_guaranteed()
                if result:
                    self._logger.info(
                        f'[TIMEOUT -> FULFILLED] {self._order_id} ({self._instrument_id}) '
                        'after resovling status via rest api, we found the order was '
                        'actually fulfilled (in the very last minute)')
                    return OPEN_POSITION_STATUS__SUCCEEDED
                else:
                    self._logger.info(
                        f'[TIMEOUT -> TIMEOUT] {self._order_id} ({self._instrument_id}) '
                        'after resovling status via rest api, we confirmed the '
                        'pending order has been revoked successful and return '
                        'timeout')
                    return OPEN_POSITION_STATUS__TIMEOUT
            elif status == ORDER_AWAIT_STATUS__CANCELLED:
                self._logger.info(
                    f'[CANCELLED] {self._order_id} ({self._instrument_id}) '
                    'pending order has been canceled')
                return OPEN_POSITION_STATUS__CANCELLED
            elif status == ORDER_AWAIT_STATUS__FULFILLED:
                self._logger.info(
                    f'[FULFILLED] {self._order_id} ({self._instrument_id}) '
                    ' pending order has been fulfilled')
                return OPEN_POSITION_STATUS__SUCCEEDED
            else:
                self._logger.fatal(
                    f'[EXCEPTION] {self._order_id} ({self._instrument_id}) '
                    'pending order encountered unexpected ORDER_AWAIT_STATUS '
                    f'{result}')
                raise Exception(f'unexpected ORDER_AWAIT_STATUS: {result}')

    async def _revoke_order(self):
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
            self._logger.fatal(
                'unexpected revoking order response:\n%s', pprint.pformat(ret))
            return False

    async def _revoke_order_and_resolve_final_status_guaranteed(self):
        """Returns either Ture: fulfilled; False: cancelled."""
        while True:
            result = await self._revoke_order()
            if result:
                # Cancelled successfully.
                return False
            final_status = await self._resolve_final_status_after_revoke()
            if final_status == constants.ORDER_STATUS_CODE__CANCELLED:
                return False
            elif final_status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
                # TODO: handle partial position.
                pass
            elif final_status == constants.ORDER_STATUS_CODE__FULFILLED:
                return True
            elif final_status == constants.ORDER_STATUS_CODE__CANCEL_IN_PROCESS:
                pass

            await asyncio.sleep(1)

    async def _resolve_final_status_after_revoke(self):
        ret = await singleton.rest_api.get_order_info(
            self._order_id, self._instrument_id)
        self._logger.info(
            '[RESOLVE AFTER REVOKE] order info from rest api:\n%s',
            pprint.pformat(ret))
        status = int(ret.get('status', None))
        if status == constants.ORDER_STATUS_CODE__CANCELLED:
            self._logger.info(
                '[RESOLVE AFTER REVOKE] %s order has been cancelled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PENDING:
            self._logger.fatal(
                '[RESOLVE AFTER REVOKE] %s THIS SHOULD NOT HAPPEN', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
            self._logger.info(
                '[RESOLVE AFTER REVOKE] %s is partially filled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__FULFILLED:
            self._logger.info(
                '[RESOLVE AFTER REVOKE] %s order is fulfilled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__CANCEL_IN_PROCESS:
            self._logger.info(
                '[RESOLVE AFTER REVOKE] %s order is being cancelled in progress',
                self._order_id)
        else:
            self._logger.fatal('unknown status code: %s', status)
        return status

    async def _post_log_order_final_status(self):
        ret = await singleton.rest_api.get_order_info(
            self._order_id, self._instrument_id)
        self._logger.info(
            '[POSTMORTEM] order info from rest api:\n%s', pprint.pformat(ret))

        status = int(ret.get('status', None))
        if status == constants.ORDER_STATUS_CODE__CANCELLED:
            self._logger.info(
                '[POSTMORTEM] %s order has been cancelled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PENDING:
            self._logger.info(
                '[POSTMORTEM] %s order is still pending', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
            self._logger.info(
                '[POSTMORTEM] %s order is partially filled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__FULFILLED:
            self._logger.info(
                '[POSTMORTEM] %s order is fulfilled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__CANCEL_IN_PROCESS:
            self._logger.info(
                '[POSTMORTEM] %s order is being cancelled in progress',
                order_id)
        else:
            self._logger.fatal('unknown status code: %s', status)

        assert int(self._order_id) == int(ret.get('order_id'))
        singleton.db.async_update_order(
            order_id=ret.get('order_id'),
            transaction_id=self._transaction_id,
            comment='final',
            status=ret.get('status'),
            size=ret.get('size'),
            filled_qty=ret.get('filled_qty'),
            price=ret.get('price'),
            price_avg=ret.get('price_avg'),
            fee=ret.get('fee'),
            type=ret.get('type'),
            timestamp=ret.get('timestamp')
        )


async def _testing_coroutine(instrument_id):
    await singleton.websocket.ready
    logging.info('start')

    executor = OrderExecutor(instrument_id,
                             amount=1,
                             price=106,
                             timeout_sec=5,
                             is_market_order=False,
                             logger=logging,
                             transaction_id='fake_transaction_id')

    logging.info('open_long_position has been called')
    order_status = await executor.open_long_position()
    logging.info('execution result: %s', order_status)


def _testing():
    from .logger import init_global_logger
    init_global_logger()
    singleton.initialize_objects_with_mock_trader_and_dev_db(currency='ETH')
    singleton.websocket.book_listener = None  # test heartbeat in websocket_api
    asyncio.ensure_future(_testing_coroutine(
        instrument_id=singleton.schema.all_instrument_ids[0]))
    singleton.start_loop()


if __name__ == '__main__':
    _testing()
