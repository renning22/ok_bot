from collections import defaultdict
from decimal import Decimal

import eventlet
from absl import app, logging


class BookListener:
    def __init__(self):
        logging.info('BookListener initiated')
        self.subscribers = defaultdict(set)

    def subscribe(self, instrument_id, responder):
        '''
        :param instrument_id:
        :param responder: responder need to implement
                      tick_received(instrument_id,
                                    ask_prices,
                                    ask_vols,
                                    bid_prices,
                                    bid_vols,
                                    timestamp)
        :return: None
        '''
        if not hasattr(responder, 'tick_received'):
            raise Exception(
                f"{type(responder)} doesn't have tick_received method")
        self.subscribers[instrument_id].add(responder)

    def unsubscribe(self, instrument_id, responder):
        if responder in self.subscribers[instrument_id]:
            self.subscribers.remove(responder)

    def received_futures_depth5(self,
                                asks,
                                bids,
                                instrument_id,
                                timestamp):
        asks = sorted(asks)
        bids = sorted(bids, reverse=True)
        ask_prices = [Decimal(str(i[0])) for i in asks]
        ask_vols = [int(i[1]) for i in asks]
        bid_prices = [Decimal(str(i[0])) for i in bids]
        bid_vols = [int(i[1]) for i in bids]
        for responder in self.subscribers[instrument_id]:
            responder.tick_received(instrument_id,
                                    ask_prices,
                                    ask_vols,
                                    bid_prices,
                                    bid_vols,
                                    timestamp)


def _testing(_):
    from .websocket_api import WebsocketApi
    from .schema import Schema

    class MockTrader:
        def tick_received(self,
                          instrument_id,
                          ask_prices,
                          ask_vols,
                          bid_prices,
                          bid_vols,
                          timestamp):
            logging.info('mock trader got new tick: %s, best ask: %.3f@%d,'
                         'best bid: %.3f@%d',
                         instrument_id,
                         ask_prices[0], ask_vols[0],
                         bid_prices[0], bid_vols[0])

    pool = eventlet.GreenPool()
    schema = Schema('BTC')
    book_listener = BookListener()
    trader = MockTrader()
    book_listener.subscribe('BTC-USD-190118', trader)
    api = WebsocketApi(pool, schema=schema, book_listener=book_listener)
    api.start_read_loop()
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
