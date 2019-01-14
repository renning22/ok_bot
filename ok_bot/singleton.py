import eventlet

from . import book_listener as book_listener_module
from . import order_listener as order_listener_module
from . import rest_api_v3 as rest_api_v3_module
from . import websocket_api as websocket_api_module
from . import schema as schema_module
from .order_book import OrderBook
from .trader import Trader

green_pool = None
rest_api = None
book_listener = None
order_listener = None
order_book = None
trader = None
schema = None
websocket = None


def initialize_objects(currency):
    global green_pool
    global rest_api
    global book_listener
    global order_listener
    global order_book
    global trader
    global schema
    global websocket

    green_pool = eventlet.GreenPool()
    rest_api = rest_api_v3_module.RestApiV3()
    book_listener = book_listener_module.BookListener()
    order_listener = order_listener_module.OrderListener()
    schema = schema_module.Schema(currency)
    trader = Trader()
    order_book = OrderBook()
    websocket = websocket_api_module.WebsocketApi(
        green_pool=green_pool,
        schema=schema,
        book_listener=book_listener,
        order_listener=order_listener)


def start_loop():
    websocket.start_read_loop()
    green_pool.waitall()
