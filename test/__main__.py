import unittest

from .test_arbitrage_execution import *
from .test_order_executor import *
from .test_trader import *
from .test_websocket_non_blocking import *


# unittest.main will automatically run through all TestCase belong to this
# module.
unittest.main()
