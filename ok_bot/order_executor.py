from collections import namedtuple
from functools import partial

import eventlet
from absl import app, logging

from . import singleton
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
    def __init__(self, order_id, logger=None):
        self._order_id = order_id
        self._future = Future()
        self._logger = logger if logger else logging

    def __enter__(self):
        singleton.order_listener.subscribe(self._order_id, self)
        return self._future

    def __exit__(self, type, value, traceback):
        singleton.order_listener.unsubscribe(self._order_id, self)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        self._logger.info('websocket event: order_pending %s', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        self._logger.info('websocket event: order_cancelled %s', order_id)
        self._future.set(ORDER_AWAIT_STATUS__CANCELLED)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        self._logger.info('websocket event: order_fulfilled %s', order_id)
        self._future.set(ORDER_AWAIT_STATUS__FULFILLED)

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        assert self._order_id == order_id
        self._logger.info(
            'websocket event: order_partially_filled %s', order_id)


class OrderExecutor:
    def __init__(self, instrument_id, amount, price, timeout_sec, is_market_order=False, logger=None):
        self._instrument_id = instrument_id
        self._amount = amount
        self._price = price
        self._timeout_sec = timeout_sec
        self._is_market_order = is_market_order
        self._logger = logger if logger else logging

    def open_long_position(self):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(singleton.rest_api.open_long_order)

    def open_short_position(self):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(singleton.rest_api.open_short_order)

    def close_long_order(self):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(singleton.rest_api.close_long_order)

    def close_short_order(self):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(singleton.rest_api.close_short_order)

    def _open_position(self, rest_request_functor):
        """Returns Future[OpenPositionStatus]"""
        future = Future()
        singleton.green_pool.spawn_n(
            self._async_place_order_and_await, rest_request_functor, future)
        return future

    def _async_place_order_and_await(self, rest_request_functor, future):
        # TODO: add timeout_sec for rest api wait() as well.
        # TODO: passing logger down to rest_api_v3 for lower granularity error
        #       mesasge in transaction log.
        order_id = singleton.green_pool.spawn(
            rest_request_functor,
            self._instrument_id,
            self._amount,
            self._price,
            is_market_order=self._is_market_order
        ).wait()

        if order_id is None:
            self._logger.error('failed when requesting open order via http')
            future.set(OPEN_POSITION_STATUS__REST_API)
            return
        self._logger.info(
            f'new order {order_id} was created successfully via '
            f'http ({self._instrument_id})')

        with OrderAwaiter(order_id, logger=self._logger) as await_status_future:
            status = await_status_future.get(self._timeout_sec)
            if status is None:
                # timeout, cancel the pending order
                # TODO: add retry logic
                singleton.green_pool.spawn_n(singleton.rest_api.cancel_order,
                                             self._instrument_id,
                                             order_id)
                self._logger.info(
                    f'pending order {order_id} ({self._instrument_id}) '
                    'failed to fulfill in time and was canceled')
                future.set(OPEN_POSITION_STATUS__TIMEOUT)
            elif status == ORDER_AWAIT_STATUS__CANCELLED:
                future.set(OPEN_POSITION_STATUS__CANCELLED)
            elif status == ORDER_AWAIT_STATUS__FULFILLED:
                future.set(OPEN_POSITION_STATUS__SUCCEEDED)
            else:
                raise Exception(f'unexpected ORDER_AWAIT_STATUS: {result}')


def _testing_thread(instrument_id):
    singleton.websocket.ready.get()
    logging.info('start')

    executor = OrderExecutor(instrument_id, amount=1,
                             price=100.0, timeout_sec=10)
    order_status_future = executor.open_long_position()

    logging.info('open_long_position has been called')
    result = order_status_future.get()
    logging.info('result: %s', result)


def _testing(_):
    singleton.initialize_objects_monkey_patch(currency='ETH')
    # singleton.websocket._book_listener = None  # test heartbeat in websocket_api
    singleton.websocket.start_read_loop()
    singleton.green_pool.spawn_n(
        _testing_thread,
        instrument_id=singleton.schema.all_instrument_ids[0])
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
