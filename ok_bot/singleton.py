import eventlet

book_listener = None
db = None
green_pool = None
order_book = None
order_listener = None
rest_api = None
schema = None
trader = None
websocket = None


def initialize_objects(currency):
    from .book_listener import BookListener
    from .db import ProdDb
    from .order_book import OrderBook
    from .order_listener import OrderListener
    from .rest_api_v3 import RestApiV3
    from .schema import Schema
    from .trader import Trader
    from .websocket_api import WebsocketApi

    global book_listener
    global db
    global green_pool
    global order_book
    global order_listener
    global rest_api
    global schema
    global trader
    global websocket

    db = ProdDb()
    green_pool = eventlet.GreenPool()
    rest_api = RestApiV3()
    book_listener = BookListener()
    order_listener = OrderListener()
    schema = Schema(currency)
    trader = Trader()
    order_book = OrderBook()
    websocket = WebsocketApi(
        green_pool=green_pool,
        schema=schema,
        book_listener=book_listener,
        order_listener=order_listener)


# For unit testing, by this way we test could initialize_objects as a whole.
def initialize_objects_with_mock_trader_and_dev_db(currency):
    from .db import DevDb
    from .mock import MockTrader
    from unittest.mock import patch
    with patch('ok_bot.trader.Trader', new=MockTrader),\
            patch('ok_bot.db.ProdDb', new=DevDb):
        initialize_objects(currency)


def start_loop():
    websocket.start_read_loop()
    green_pool.waitall()
