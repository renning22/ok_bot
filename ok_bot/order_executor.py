from collections import namedtuple
from functools import partial

import eventlet
from absl import app, logging

OpenPositionStatus = namedtuple('OpenPositionStatus',
                                ['result',  # boolean, successful or not.
                                 # Detailed error message if failed.
                                 'message',
                                 ])

OPEN_POSITION_STATUS__SUCCEEDED = OpenPositionStatus(result=True, message=None)
OPEN_POSITION_STATUS__UNKNOWN = OpenPositionStatus(
    result=False, message='unknown')
OPEN_POSITION_STATUS__REST_API = OpenPositionStatus(
    result=False, message='rest api http error')
OPEN_POSITION_STATUS__TIMEOUT = OpenPositionStatus(
    result=False, message='no websocket updates and timed out')
OPEN_POSITION_STATUS__CANCELLED = OpenPositionStatus(
    result=False, message='order cancelled')


ORDER_AWAIT_STATUS__FULFILLED = 'fulfilled'
ORDER_AWAIT_STATUS__CANCELLED = 'cancelled'


class OrderAwaiter:
    def __init__(self, order_id, order_listener):
        self._order_id = order_id
        self._order_listener = order_listener
        self._await_status = eventlet.event.Event()

    def __enter__(self):
        self._order_listener.subscribe(self._order_id, self)
        return self._await_status

    def __exit__(self, type, value, traceback):
        self._order_listener.unsubscribe(self._order_id, self)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        logging.info('order_pending: %s', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        logging.info('order_cancelled: %s', order_id)
        self._await_status.send(ORDER_AWAIT_STATUS__CANCELLED)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        logging.info('order_fulfilled: %s', order_id)
        self._await_status.send(ORDER_AWAIT_STATUS__FULFILLED)

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        assert self._order_id == order_id
        logging.info('order_partially_filled: %s', order_id)


class OrderExecutor:
    def __init__(self, pool, rest_api_v3, order_listener):
        self._pool = pool
        self._rest_api_v3 = rest_api_v3
        self._order_listener = order_listener

    def open_long_position(self, instrument_id, amount, price, timeout_sec):
        """Returns eventlet.event.Event[OpenPositionStatus]"""
        return self._open_position(
            partial(self._rest_api_v3.open_long_order,
                    instrument_id,
                    amount,
                    price),
            timeout_sec)

    def open_short_position(self, instrument_id, amount, price, timeout_sec):
        """Returns eventlet.event.Event[OpenPositionStatus]"""
        return self._open_position(
            partial(self._rest_api_v3.open_short_order,
                    instrument_id,
                    amount,
                    price),
            timeout_sec)

    def _open_position(self, rest_request_functor, timeout_sec):
        """Returns eventlet.event.Event[OpenPositionStatus]"""
        future = eventlet.event.Event()
        self._pool.spawn_n(self._async_place_order_and_await,
                           rest_request_functor,
                           timeout_sec,
                           future)
        return future

    def _async_place_order_and_await(self,
                                     rest_request_functor,
                                     timeout_sec,
                                     future):
        # TODO: add timeout_sec for rest api wait() as well.
        order_id = self._pool.spawn(rest_request_functor).wait()
        if order_id is None:
            logging.error('failure from rest api')
            future.send(OPEN_POSITION_STATUS__REST_API)
            return

        with OrderAwaiter(order_id, self._order_listener) as await_status:
            result = await_status.wait(timeout_sec)
            if result is None:
                # timeout
                future.send(OPEN_POSITION_STATUS__TIMEOUT)
            elif reulst == 'cancelled':
                future.send(OPEN_POSITION_STATUS__CANCELLED)
            elif result == 'fulfilled':
                future.send(OPEN_POSITION_STATUS__SUCCEEDED)
            else:
                future.send(OPEN_POSITION_STATUS__UNKNOWN)


def _testing_thread(instrument_id, pool, order_listener):
    from .rest_api_v3 import RestApiV3

    eventlet.sleep(5)
    logging.info('start')

    executor = OrderExecutor(
        pool=pool,
        rest_api_v3=RestApiV3(),
        order_listener=order_listener)
    future = executor.open_long_position(
        instrument_id, amount=1, price=50, timeout_sec=30)

    logging.info('open_long_position has been called')
    result = future.wait()
    logging.info('result: %s', result)


def _testing(_):
    from .order_listener import OrderListener
    from .schema import Schema
    from .websocket_api import WebsocketApi

    pool = eventlet.GreenPool()
    schema = Schema('ETH')
    order_listener = OrderListener()
    ws_api = WebsocketApi(pool, schema=schema, order_listener=order_listener)
    ws_api.start_read_loop()

    pool.spawn_n(_testing_thread,
                 instrument_id=schema.all_instrument_ids[0],
                 pool=pool,
                 order_listener=order_listener)
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
