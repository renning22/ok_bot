import pprint
from decimal import Decimal

import eventlet
from absl import app, logging


class BookReader:
    def __init__(self, order_book):
        self.order_book = order_book
        logging.info('BookReader initiated')

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
        self.order_book.update_book(
            instrument_id, ask_prices, ask_vols, bid_prices, bid_vols)


def _testing(_):
    from .order_book import OrderBook
    from .websocket_api import WebsocketApi
    from .schema import Schema

    class MockTrader:
        def new_tick_received(self, order_book):
            logging.info('mock trader got new tick: %d, %s',
                         order_book.row_num,
                         order_book.recent_tick_source())

    pool = eventlet.GreenPool()

    schema = Schema('BTC')
    trader = MockTrader()
    order_book = OrderBook(schema, trader)
    reader = BookReader(order_book)
    api = WebsocketApi(pool, reader, schema)
    api.start_read_loop()

    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
