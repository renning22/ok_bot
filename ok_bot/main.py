import os

import eventlet
from absl import flags, logging

from . import constants
from .book_listener import BookListener
from .order_book import OrderBook
from .order_executor import OrderExecutor
from .order_listener import OrderListener
from .rest_api_v3 import RestApiV3
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
    rest_api_v3 = RestApiV3()
    order_executor = OrderExecutor(
        pool=green_pool,
        rest_api_v3=rest_api_v3,
        order_listener=OrderListener())
    trader = Trader(schema,
                    order_executor,
                    constants.SPREAD_DEVIATION_THRESHOLD)
    # TODO: integrate order_book.
    order_book = OrderBook(schema, trader)

    book_listener = BookListener()
    ws_api = WebsocketApi(green_pool=green_pool,
                          schema=schema,
                          book_listener=book_listener)
    ws_api.start_read_loop()
    green_pool.waitall()
