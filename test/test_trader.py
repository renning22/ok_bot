from unittest import TestCase
from unittest.mock import MagicMock

import eventlet
from absl import logging

from ok_bot import singleton, constants
from ok_bot.order_executor import OrderExecutor


class TestTrader(TestCase):
    def setUp(self):
        singleton.initialize_objects('ETH')
        singleton.rest_api = MagicMock()

        logging.get_absl_logger().setLevel(logging.DEBUG)

    def test_cool_down(self):
        order_exe = OrderExecutor(
            singleton.schema.all_instrument_ids[0],
            amount=1,
            price=10000,
            timeout_sec=10,
            is_market_order=False,
            logger=logging.get_absl_logger()
        )
        constants.INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 10
        singleton.rest_api.open_long_order.return_value = \
            (None, constants.REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH)
        order_exe.open_long_position().get()
        singleton.websocket.start_read_loop()
        eventlet.sleep(5)
        self.assertTrue(singleton.trader.is_in_cooldown)
        eventlet.sleep(8)  # wait for cool down to finish
        self.assertFalse(singleton.trader.is_in_cooldown)


if __name__ == '__main__':
    unittest.main()
