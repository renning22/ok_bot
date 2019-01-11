import eventlet

from . import book_listener as book_listener_module
from . import order_listener as order_listener_module
from . import rest_api_v3 as rest_api_v3_module
from . import websocket_api as websocket_api_module
from . import schema as schema_module

green_pool = eventlet.GreenPool()
rest_api = rest_api_v3_module.RestApiV3()
book_listener = book_listener_module.BookListener()
order_listener = order_listener_module.OrderListener()
schema = None
websocket = None


def initialize_objects(currency):
    global schema
    global websocket
    schema = schema_module.Schema(currency)
    websocket = websocket_api_module.WebsocketApi(
        green_pool=green_pool,
        schema=schema,
        book_listener=book_listener,
        order_listener=order_listener)

