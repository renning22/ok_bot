from book_reader import BookReader
from order_book import OrderBook
from order_executor import OrderExecutor
from position_syncer import PositionSyncer
from rest_api import OKRest
from trader import Trader
import constants

from absl import logging
import absl
import asyncio


def main(_):
    symbol = absl.flags.FLAGS.symbol
    logging.get_absl_logger()
    logging.info(f'starting program with {symbol}')

    # initialize components
    order_book = OrderBook()
    ok_rest_api = OKRest(symbol)
    position_syncer = PositionSyncer(symbol, ok_rest_api, order_book)
    order_executor = OrderExecutor(ok_rest_api)
    trader = Trader(order_executor,
                    constants.SPREAD_DEVIATION_THRESHOLD)
    reader = BookReader(order_book,
                        trader,
                        symbol)
    asyncio.ensure_future(position_syncer.read_loop()),
    asyncio.ensure_future(reader.read_loop())
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    absl.flags.DEFINE_string('symbol', 'btc', 'symbol for crypto-currency in under case')
    absl.app.run(main)
