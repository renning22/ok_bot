import asyncio

book_listener = None
coin_currency = None
db = None
loop = None
order_book = None
order_listener = None
rest_api = None
schema = None
trader = None
websocket = None


def initialize_objects(
        currency,
        simple_strategy=False,
        max_parallel_transaction_num=int(1e9)):
    from .book_listener import BookListener
    from .db import ProdDb
    from .order_book import OrderBook
    from .order_listener import OrderListener
    from .rest_api_v3 import RestApiV3
    from .schema import Schema
    from .trader import Trader
    from .websocket_api import WebsocketApi

    global book_listener
    global coin_currency
    global db
    global loop
    global order_book
    global order_listener
    global rest_api
    global schema
    global trader
    global websocket

    coin_currency = currency

    loop = asyncio.get_event_loop()

    db = ProdDb()
    db.create_tables_if_not_exist()

    rest_api = RestApiV3()
    book_listener = BookListener()
    order_listener = OrderListener()
    schema = Schema(currency)
    trader = Trader(
        simple_strategy=simple_strategy,
        max_parallel_transaction_num=max_parallel_transaction_num
    )
    order_book = OrderBook()
    websocket = WebsocketApi(
        schema=schema,
        book_listener=book_listener,
        order_listener=order_listener)


# For unit testing only.
# By this way we could test 'initialize_objects' as a whole.
def initialize_objects_with_mock_trader_and_dev_db(currency):
    from .db import DevDb
    from .mock import MockTrader
    from unittest.mock import patch

    with patch('ok_bot.trader.Trader', new=MockTrader),\
            patch('ok_bot.db.ProdDb', new=DevDb):
        initialize_objects(currency=currency)


def initialize_objects_with_dev_db(currency):
    from .db import DevDb
    from unittest.mock import patch

    with patch('ok_bot.db.ProdDb', new=DevDb):
        initialize_objects(currency=currency)


def start_loop():
    loop.run_until_complete(websocket._read_loop())
