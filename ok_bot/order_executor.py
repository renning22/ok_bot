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
    def __init__(self, order_id):
        self._order_id = order_id
        self._future = Future()

    def __enter__(self):
        singleton.order_listener.subscribe(self._order_id, self)
        return self._future

    def __exit__(self, type, value, traceback):
        singleton.order_listener.unsubscribe(self._order_id, self)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        logging.info('order_pending: %s', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        logging.info('order_cancelled: %s', order_id)
        self._future.set(ORDER_AWAIT_STATUS__CANCELLED)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        logging.info('order_fulfilled: %s', order_id)
        self._future.set(ORDER_AWAIT_STATUS__FULFILLED)

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        assert self._order_id == order_id
        logging.info('order_partially_filled: %s', order_id)


class OrderExecutor:
    def open_long_position(self, instrument_id, amount, price, timeout_sec):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(
            instrument_id,
            partial(singleton.rest_api.open_long_order,
                    instrument_id,
                    amount,
                    price),
            timeout_sec)

    def open_short_position(self, instrument_id, amount, price, timeout_sec):
        """Returns Future[OpenPositionStatus]"""
        return self._open_position(
            instrument_id,
            partial(singleton.rest_api.open_short_order,
                    instrument_id,
                    amount,
                    price),
            timeout_sec)

    def _open_position(self, instrument_id, rest_request_functor, timeout_sec):
        """Returns Future[OpenPositionStatus]"""
        future = Future()
        singleton.green_pool.spawn_n(self._async_place_order_and_await,
                                     instrument_id,
                                     rest_request_functor,
                                     timeout_sec,
                                     future)
        return future

    def _async_place_order_and_await(self,
                                     instrument_id,
                                     rest_request_functor,
                                     timeout_sec,
                                     future):
        # TODO: add timeout_sec for rest api wait() as well.
        order_id = singleton.green_pool.spawn(rest_request_functor).wait()
        if order_id is None:
            logging.error('failure from rest api')
            future.set(OPEN_POSITION_STATUS__REST_API)
            return
        logging.info(f'Order {order_id} created for {instrument_id}')

        with OrderAwaiter(order_id) as await_status_future:
            status = await_status_future.get(timeout_sec)
            if status is None:
                # timeout, cancel the pending order
                singleton.green_pool.spawn_n(singleton.rest_api.cancel_order,
                                             instrument_id,
                                             order_id)
                logging.info(f'{order_id} for {instrument_id} failed to fulfill in time'
                             ' and is canceled')
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

    executor = OrderExecutor()
    order_status_future = executor.open_long_position(
        instrument_id, amount=1, price=100.0, timeout_sec=10)

    logging.info('open_long_position has been called')
    result = order_status_future.get()
    logging.info('result: %s', result)


def _testing(_):
    singleton.initialize_objects(currency='ETH')
    singleton.websocket._book_listener = None  # test heartbeat in websocket_api
    singleton.websocket.start_read_loop()
    singleton.green_pool.spawn_n(_testing_thread,
                                 instrument_id=singleton.schema.all_instrument_ids[0])
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
