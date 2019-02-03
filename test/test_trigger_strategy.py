import logging
import unittest
from unittest import TestCase

from ok_bot import constants, logger, singleton, trigger_strategy
from ok_bot.arbitrage_execution import LONG, SHORT


class TestPercentageTriggerStrategy(TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
        singleton.coin_currency = 'ETH'

    def test_spot_profit(self):
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trigger_strategy.spot_profit(100, 110, 200, 200))
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trigger_strategy.spot_profit(100, 100, 200, 190))
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trigger_strategy.spot_profit(100, 105, 200, 195))
        self.assertGreater(constants.MIN_ESTIMATE_PROFIT,
                           trigger_strategy.spot_profit(100, 100.1, 200, 200))
        self.assertAlmostEqual(
            -(10 / 100 * 2 + 10 / 200 * 2) * constants.FEE_RATE,
            trigger_strategy.spot_profit(100, 100, 200, 200))

    def test_estimate_profit(self):
        self.assertLess(
            trigger_strategy.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=50
            ),
            -(10 / 100 * 2 + 10 / 150 * 2) * constants.FEE_RATE
        )

        self.assertGreater(
            trigger_strategy.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=10
            ),
            constants.MIN_ESTIMATE_PROFIT
        )

        self.assertLess(
            trigger_strategy.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=48
            ),
            constants.MIN_ESTIMATE_PROFIT
        )


if __name__ == '__main__':
    unittest.main()
