import json
import pprint
import traceback
import zlib
from decimal import Decimal
from typing import Dict

import eventlet
from absl import app, logging


class BookReader:
    def __init__(self, order_book, trader):
        self.order_book = order_book
        self.trader = trader
        logging.info('BookReader initiated')

    def received_futures_depth5(self,
                                asks,
                                bids,
                                instrument_id,
                                timestamp):
        asks = sorted(asks)
        bids = sorted(bids, reverse=True)
        asks_bids = {
            f'{instrument_id}_ask_price': Decimal(str(asks[0][0])),
            f'{instrument_id}_ask_vol': int(asks[0][1]),
            f'{instrument_id}_bid_price': Decimal(str(bids[0][0])),
            f'{instrument_id}_bid_vol': int(bids[0][1]),
            f'{instrument_id}_ask2_price': Decimal(str(asks[1][0])),
            f'{instrument_id}_ask2_vol': int(asks[1][1]),
            f'{instrument_id}_bid2_price': Decimal(str(bids[1][0])),
            f'{instrument_id}_bid2_vol': int(bids[1][1]),
            f'{instrument_id}_ask3_price': Decimal(str(asks[2][0])),
            f'{instrument_id}_ask3_vol': int(asks[2][1]),
            f'{instrument_id}_bid3_price': Decimal(str(bids[2][0])),
            f'{instrument_id}_bid3_vol': int(bids[2][1]),
        }
        logging.info('new tick:\n %s', pprint.pformat(asks_bids))
        self.order_book.update_book(instrument_id, asks_bids)
        self.trader.new_tick_received(self.order_book)


def _testing(_):
    from .order_book import MockOrderBook
    from .websocket_api import WebsocketApi

    order_book = MockOrderBook()

    class MockTrader:
        def new_tick_received(*argv):
            logging.info('mock trader got new tick')

    reader = BookReader(order_book, MockTrader())

    pool = eventlet.GreenPool()
    api = WebsocketApi(pool, reader, 'BTC')
    api.start_read_loop()

    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
