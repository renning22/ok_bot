import os

import eventlet
from absl import flags, logging

from . import constants
from .book_reader import BookReader
from .order_book import OrderBook
from .order_canceller import OrderCanceller
from .order_executor import OrderExecutor
from .position_syncer import PositionSyncer
from .rest_api import RestApi
from .schema import Schema
from .slack import SlackLoggingHandler
from .trader import Trader
from .websocket_api import WebsocketApi


def config_logging():
    if flags.FLAGS.logtofile:
        os.makedirs('log', exist_ok=True)
        logging.get_absl_handler().use_absl_log_file('ok_bot', 'log')
    if flags.FLAGS.alsologtoslack:
        logging.get_absl_logger().addHandler(SlackLoggingHandler('INFO'))


def main(_):
    config_logging()

    symbol = flags.FLAGS.symbol
    logging.info('starting program with %s', symbol)

    # initialize components
    green_pool = eventlet.GreenPool(1000)

    schema = Schema(symbol)
    rest_api = RestApi(symbol)
    order_executor = OrderExecutor(rest_api)
    trader = Trader(schema,
                    order_executor,
                    constants.SPREAD_DEVIATION_THRESHOLD)
    order_book = OrderBook(schema, trader)
    order_canceller = OrderCanceller(green_pool, rest_api)
    position_syncer = PositionSyncer(
        green_pool, symbol, rest_api, order_book)
    reader = BookReader(order_book)
    ws_api = WebsocketApi(green_pool, reader, schema)

    # start the three loops
    # green_pool.spawn_n(order_canceller.read_cancel_loop)
    # green_pool.spawn_n(position_syncer.read_loop)
    ws_api.start_read_loop()
    green_pool.waitall()
