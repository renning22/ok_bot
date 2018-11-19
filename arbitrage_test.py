import asyncio
import unittest
from decimal import *
from unittest.mock import ANY, MagicMock, patch

import arbitrage


class ArbitrageTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    @patch('order.open_short_order')
    @patch('order.open_long_order')
    @patch('slack.send_unblock')
    @patch('cooldown.trigger_arbitrage_cooldown', return_value=True)
    def test_trigger_arbitrage__basic(self,
                                      mock_trigger_arbitrage_cooldown,
                                      mock_send_unblock,
                                      mock_open_long_order,
                                      mock_open_short_order):
        last_record = {
            'this_week_ask_price': Decimal('6000'),
            'this_week_ask_vol': Decimal('10'),
            'quarter_bid_price': Decimal('5900'),
            'quarter_bid_vol': Decimal('8'),
        }

        arbitrage.trigger_arbitrage('this_week', 'quarter', last_record)
        self.loop.stop()
        self.loop.run_forever()
        self.loop.close()

        mock_send_unblock.assert_has_calls([ANY, ANY])

        # Decimal('2') i.e. max_order_amount is defined in arbitrage.py.
        mock_open_long_order.assert_called_once_with(
            'this_week', Decimal('2'), Decimal('6000'))
        mock_open_short_order.assert_called_once_with(
            'quarter', Decimal('2'), Decimal('5900'))


if __name__ == '__main__':
    unittest.main()
