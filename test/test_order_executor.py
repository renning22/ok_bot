import unittest
from decimal import Decimal
from unittest.mock import ANY, MagicMock

import eventlet

from ok_bot.order_executor import OrderExecutor


class TestOrderExecutor(unittest.TestCase):

    def setUp(self):
        self._mock_api = MagicMock()
        self._order_executor = OrderExecutor(self._mock_api)

    def test_open_arbitrage_position(self):
        self._order_executor.open_arbitrage_position(
            'this_week', 30, 'next_week', 300, 1)


if __name__ == '__main__':
    unittest.main()
