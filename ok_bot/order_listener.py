from collections import defaultdict
from decimal import Decimal

import eventlet
from absl import app, logging

from .rest_api_v3 import RestApiV3
from .schema import Schema
from .websocket_api import WebsocketApi


class OrderListener:
    def __init__(self):
        logging.info('OrderListener initiated')
        self._subscribers = defaultdict(set)

        # There is no guarantee Websocket order notification always comes
        # after REST API http responses (for the same order). The buffer makes
        # sure no Websocket order notification is missed for the subscriber.
        self._buffer = defaultdict(list)

    def subscribe(self, order_id, responder):
        '''
        :param order_id:
        :param responder: responder needs to implement:
                      order_pending(order_id)
                      order_cancelled(order_id)
                      order_fulfilled(order_id,
                                      size,
                                      filled_qty,
                                      fee,
                                      price,
                                      price_avg)
                      order_partially_filled(order_id,
                                             size,
                                             filled_qty)
        :return: None
        '''
        order_id = int(order_id)
        assert hasattr(responder, 'order_pending')
        assert hasattr(responder, 'order_cancelled')
        assert hasattr(responder, 'order_fulfilled')
        assert hasattr(responder, 'order_partially_filled')
        self._subscribers[order_id].add(responder)
        self._dispatch_buffer(order_id)

    def unsubscribe(self, order_id, responder):
        self._subscribers[order_id].discard(responder)
        if len(self._subscribers[order_id]) == 0:
            del self._subscribers[order_id]

    def _received_futures_order(self,
                                leverage,
                                size,
                                filled_qty,
                                price,
                                fee,
                                contract_val,
                                price_avg,
                                type,
                                instrument_id,
                                order_id,
                                timestamp,
                                status):
        order_id = int(order_id)
        # Order Status:
        #   -1 cancelled
        #   0: pending
        #   1: partially filled
        #   2: fully filled
        if status == -1:
            self._buffer[order_id].append(
                lambda trader: trader.order_cancelled(order_id))
        elif status == 0:
            self._buffer[order_id].append(
                lambda trader: trader.order_pending(order_id))
        elif status == 1:
            self._buffer[order_id].append(
                lambda trader: trader.order_partially_filled(order_id,
                                                             size,
                                                             filled_qty))
        elif status == 2:
            self._buffer[order_id].append(
                lambda trader: trader.order_fulfilled(order_id,
                                                      size,
                                                      filled_qty,
                                                      fee,
                                                      price,
                                                      price_avg))
        else:
            raise ValueException(
                f'unknown order update message type: {status}')

        self._dispatch_buffer(order_id)

    def _dispatch_buffer(self, order_id):
        if not self._subscribers[order_id]:
            return

        for func in self._buffer[order_id]:
            for responder in self._subscribers[order_id]:
                func(responder)
        self._buffer[order_id].clear()


class MockTrader:
    def order_pending(self, order_id):
        logging.info('order_pending: %s', order_id)

    def order_cancelled(self, order_id):
        logging.info('order_cancelled: %s', order_id)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        logging.info('order_fulfilled: %s', order_id)

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        logging.info('order_partially_filled: %s', order_id)


def _testing_thread(instrument_id, order_listener):
    eventlet.sleep(5)

    api = RestApiV3()
    order_id = api.open_long_order(instrument_id, amount=1, price=50)
    logging.info('order has been placed order_id: %s', order_id)

    eventlet.sleep(5)

    trader = MockTrader()
    order_listener.subscribe(order_id, trader)


def _testing(_):
    pool = eventlet.GreenPool()
    schema = Schema('ETH')
    logging.info('instruments: %s', schema.all_instrument_ids)
    instrument_id = schema.all_instrument_ids[0]

    ws_api = WebsocketApi(pool, schema=schema, order_listener=singleton_order_listener)
    ws_api.start_read_loop()

    pool.spawn_n(_testing_thread,
                 instrument_id=instrument_id,
                 order_listener=order_listener)
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
