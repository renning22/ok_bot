import absl
import eventlet
from absl import logging

import constants
from book_reader import BookReader
from order_book import OrderBook
from order_canceller import OrderCanceller
from order_executor import OrderExecutor
from position_syncer import PositionSyncer
from trader import Trader

rest_api = eventlet.import_patched("rest_api")


def main(_):
    symbol = absl.flags.FLAGS.symbol
    logging.info(f'starting program with {symbol}')

    # initialize components
    green_pool = eventlet.GreenPool(1000)
    order_book = OrderBook()
    ok_rest_api = rest_api.OKRest(symbol)
    order_canceller = OrderCanceller(green_pool, ok_rest_api)
    position_syncer = PositionSyncer(
        green_pool, symbol, ok_rest_api, order_book)
    order_executor = OrderExecutor(ok_rest_api)
    trader = Trader(order_executor,
                    constants.SPREAD_DEVIATION_THRESHOLD)
    reader = BookReader(green_pool,
                        order_book,
                        trader,
                        symbol)
    # start the three loops
    green_pool.spawn_n(reader.read_loop)
    green_pool.spawn_n(order_canceller.read_cancel_loop)
    green_pool.spawn_n(position_syncer.read_loop)
    green_pool.waitall()


if __name__ == '__main__':
    absl.flags.DEFINE_string(
        'symbol', 'btc', 'symbol for crypto-currency in under case')
    absl.app.run(main)
