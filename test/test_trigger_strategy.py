import asyncio
import logging
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import TestCase
from unittest.mock import Mock, patch

import numpy as np

from ok_bot import constants, logger, singleton, trigger_strategy
from ok_bot.arbitrage_execution import LONG, SHORT
from ok_bot.trigger_strategy import PercentageTriggerStrategy


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
                gap_threshold=49.9
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
                    SHORT: 101,
                },
                gap_threshold=20
            ),
            constants.MIN_ESTIMATE_PROFIT
        )


class FeatTestPercentageTriggerStrategy(TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
        singleton.initialize_objects_with_dev_db('ETH')
        self.strategy = PercentageTriggerStrategy()

    def tearDown(self):
        singleton.db.shutdown(wait=True)
        for task in asyncio.Task.all_tasks():
            task.cancel()

    async def query_plan_after_ready(self):
        singleton.trader.min_time_window = np.timedelta64(5, 's')
        await singleton.trader.ready
        long_instrument_id, short_instrument_id, product = \
            singleton.schema.markets_cartesian_product[0]
        return self.strategy.is_there_a_plan(
            long_instrument_id, short_instrument_id, product)

    @patch('ok_bot.trigger_strategy.estimate_profit', return_value=10000)
    @patch('ok_bot.util.amount_margin', return_value=10000)
    @patch('ok_bot.arbitrage_execution.ArbitrageTransaction')
    def test_strategy_trigger_in_basic_cases(
            self, arbitrage, amount_margin, estimate_profit):
        # Produce plan when there's enough profit estimation and amount margin
        amount_margin.return_value = 10000
        estimate_profit.return_value = 10000
        asyncio.ensure_future(singleton.websocket._read_loop())
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNotNone(plan)
        # No plan when not enough amount margin
        amount_margin.return_value = 0
        estimate_profit.return_value = 10000
        asyncio.ensure_future(singleton.websocket._read_loop())
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNone(plan)
        # No plan when profit estimation is negative
        amount_margin.return_value = 10000
        estimate_profit.return_value = -1
        asyncio.ensure_future(singleton.websocket._read_loop())
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNone(plan)


if __name__ == '__main__':
    unittest.main()
