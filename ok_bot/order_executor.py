from functools import partial

import eventlet
from absl import app, logging


class OrderAwaiter:
    def __init__(self, order_id, order_listener):
        self._order_id = order_id
        self._order_listener = order_listener
        self._future = eventlet.event.Event()

    def __enter__(self):
        self._order_listener.subscribe(self._order_id, self)
        return self._future

    def __exit__(self, type, value, traceback):
        self._order_listener.unsubscribe(self._order_id, self)

    def order_pending(self, order_id):
        assert self._order_id == order_id
        logging.info('order_pending: %s', order_id)

    def order_cancelled(self, order_id):
        assert self._order_id == order_id
        logging.info('order_cancelled: %s', order_id)
        self._future.send('cancelled')

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        assert self._order_id == order_id
        logging.info('order_fulfilled: %s', order_id)
        self._future.send('fulfilled')

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

    def open_long_position(self, instrument_id, amount, price, timeout):
        return self._open_position(
            partial(self._rest_api_v3.open_long_order,
                    instrument_id,
                    amount,
                    price),
            timeout)

    def open_short_position(self, instrument_id, amount, price, timeout):
        return self._open_position(
            partial(self._rest_api_v3.open_short_order,
                    instrument_id,
                    amount,
                    price),
            timeout)

    def _open_position(self, rest_request_functor, timeout):
        future = eventlet.event.Event()
        self._pool.spawn_n(self._async_place_order_and_await,
                           rest_request_functor,
                           timeout,
                           future)
        return future

    def _async_place_order_and_await(self,
                                     rest_request_functor,
                                     timeout,
                                     future):
        # TODO: add timeout for rest api wait() as well.
        order_id = self._pool.spawn(rest_request_functor).wait()
        if order_id is None:
            logging.error('failure from rest api')
            future.send('no, rest_api_error')
            return

        with OrderAwaiter(order_id, self._order_listener) as awaiter:
            result = awaiter.wait(timeout)
            if result is None:
                # timeout
                future.send('no, timeout')
            elif reulst == 'cancelled':
                future.send('no, cancelled')
            elif result == 'fulfilled':
                future.send(True)
            else:
                future.send('no, unknown')


def _testing_thread(instrument_id, pool, order_listener):
    from .rest_api_v3 import RestApiV3

    eventlet.sleep(5)
    logging.info('start')

    executor = OrderExecutor(
        pool=pool,
        rest_api_v3=RestApiV3(),
        order_listener=order_listener)
    future = executor.open_long_position(
        instrument_id, amount=1, price=50, timeout=30)

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
