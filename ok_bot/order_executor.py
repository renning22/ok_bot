import pprint
from collections import namedtuple

from absl import app, logging

from . import constants, singleton
from .future import Future

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


class OrderAwaiter:
    def __init__(self, order_id, logger):
        self._order_id = order_id
        self._future = Future()
        self._logger = logger

    def __enter__(self):
        singleton.order_listener.subscribe(self._order_id, self)
        return self._future

    def __exit__(self, type, value, traceback):
        singleton.order_listener.unsubscribe(self._order_id, self)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        self._logger.info('[WEBSOCKET] %s order_pending', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        self._logger.info('[WEBSOCKET] %s order_cancelled', order_id)
        self._future.set(ORDER_AWAIT_STATUS__CANCELLED)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        self._logger.info(
            '[WEBSOCKET] %s order_fulfilled\n'
            'price: %s, price_avg: %s, size: %s, filled_qty: %s, fee: %s',
            order_id, price, price_avg, size, filled_qty, fee)
        self._future.set(ORDER_AWAIT_STATUS__FULFILLED)

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
    def __init__(self, instrument_id, amount, price, timeout_sec, is_market_order, logger):
        self._instrument_id = instrument_id
        self._amount = amount
        self._price = price
        self._timeout_sec = timeout_sec
        self._is_market_order = is_market_order
        self._logger = logger

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

    def _place_order(self, rest_request_functor):
        """Returns Future[OpenPositionStatus]"""
        future = Future()
        singleton.green_pool.spawn_n(
            self._async_place_order_and_await, rest_request_functor, future)
        return future

    def _async_place_order_and_await(self, rest_request_functor, future):
        # TODO: add timeout_sec for rest api wait() as well.
        order_id, error_code = singleton.green_pool.spawn(
            rest_request_functor,
            self._instrument_id,
            self._amount,
            self._price,
            is_market_order=self._is_market_order
        ).wait()

        if order_id is None:
            self._logger.error(f'Failed to place order via REST API, '
                               f'error code: {error_code}')
            if error_code == constants.REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH:
                # Margin not enough, cool down
                singleton.trader.cool_down()
            future.set(OPEN_POSITION_STATUS__REST_API)
            return

        self._logger.info(
            f'{order_id} ({self._instrument_id}) order was created '
            f'via {rest_request_functor.__name__}')

        with OrderAwaiter(order_id, logger=self._logger) as await_status_future:
            status = await_status_future.get(self._timeout_sec)
            if status is None:
                self._logger.info(
                    f'[TIMEOUT] {order_id} ({self._instrument_id}) '
                    'pending order failed to fulfill in time and was canceled')
                future.set(OPEN_POSITION_STATUS__TIMEOUT)
                self._revoke_order(order_id)
            elif status == ORDER_AWAIT_STATUS__CANCELLED:
                self._logger.info(
                    f'[CANCELLED] {order_id} ({self._instrument_id}) '
                    'pending order has been canceled')
                future.set(OPEN_POSITION_STATUS__CANCELLED)
            elif status == ORDER_AWAIT_STATUS__FULFILLED:
                self._logger.info(
                    f'[FULFILLED] {order_id} ({self._instrument_id}) '
                    ' pending order has been fulfilled')
                future.set(OPEN_POSITION_STATUS__SUCCEEDED)
            else:
                self._logger.info(
                    f'[EXCEPTION] {order_id} ({self._instrument_id}) '
                    'pending order encountered unexpected ORDER_AWAIT_STATUS '
                    f'{result}')
                raise Exception(f'unexpected ORDER_AWAIT_STATUS: {result}')

        self._post_log_order_final_status(order_id)

    def _revoke_order(self, order_id):
        self._logger.info('[REVOKE PENDING ORDER] %s', order_id)
        ret = singleton.rest_api.revoke_order(
            self._instrument_id, order_id)
        if ret.get('result', False) is True:
            assert int(ret.get('order_id', None)) == order_id
            self._logger.info('[REVOKE SUCCESSFUL] %s', order_id)
            return
        elif int(ret.get('error_code', -1)) ==\
                constants.REST_API_ERROR_CODE__PENDING_ORDER_NOT_EXIST:
            self._logger.warning('[REVOKE ORDER NOT EXIST] %s', order_id)
        else:
            self._logger.error(
                'unexpected revoking order response:\n%s', pprint.pformat(ret))

    def _post_log_order_final_status(self, order_id):
        ret = singleton.rest_api.get_order_info(order_id, self._instrument_id)
        self._logger.info(
            '[POSTMORTEM] order info from rest api:\n%s', pprint.pformat(ret))

        status = int(ret.get('status', None))
        if status == constants.ORDER_STATUS_CODE__CANCELLED:
            self._logger.info(
                '[POSTMORTEM] %s order has been cancelled', order_id)
        elif status == constants.ORDER_STATUS_CODE__PENDING:
            self._logger.info(
                '[POSTMORTEM] %s order is still pending', order_id)
        elif status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
            self._logger.info(
                '[POSTMORTEM] %s order is partially filled', order_id)
        elif status == constants.ORDER_STATUS_CODE__FULFILLED:
            self._logger.info(
                '[POSTMORTEM] %s order is fulfilled', order_id)
        else:
            self._logger.error('unknown status code: %s', status)


def _testing_thread(instrument_id):
    singleton.websocket.ready.get()
    logging.info('start')

    executor = OrderExecutor(instrument_id,
                             amount=1,
                             price=115.5,
                             timeout_sec=10,
                             is_market_order=False,
                             logger=logging)
    order_status_future = executor.open_long_position()

    logging.info('open_long_position has been called')
    result = order_status_future.get()
    logging.info('execution result: %s', result)


def _testing(_):
    singleton.initialize_objects_with_mock_trader(currency='ETH')
    singleton.websocket.book_listener = None  # test heartbeat in websocket_api
    singleton.websocket.start_read_loop()
    singleton.green_pool.spawn_n(
        _testing_thread,
        instrument_id=singleton.schema.all_instrument_ids[0])
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
