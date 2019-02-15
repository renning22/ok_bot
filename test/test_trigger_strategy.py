import asyncio
import logging
import unittest
from unittest import TestCase
from unittest.mock import Mock, patch

import numpy as np

from ok_bot import constants, logger, singleton, trigger_strategy
from ok_bot.arbitrage_execution import LONG, SHORT
from ok_bot.mock import AsyncMock
from ok_bot.trigger_strategy import (PercentageTriggerStrategy,
                                     calculate_amount_margin,
                                     make_arbitrage_plan)


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
        logger.init_global_logger(log_level=logging.INFO, log_to_stderr=False)
        singleton.initialize_objects_with_dev_db('ETH')
        singleton.rest_api = AsyncMock()
        singleton.order_book.zscore = Mock(return_value=5.0)
        self.strategy = PercentageTriggerStrategy()
        singleton.loop.create_task(singleton.websocket.read_loop())

    def tearDown(self):
        singleton.db.shutdown(wait=True)
        for task in asyncio.all_tasks(singleton.loop):
            task.cancel()

    async def query_plan_after_ready(self):
        singleton.trader.min_time_window = np.timedelta64(5, 's')
        await singleton.trader.ready
        long_instrument_id, short_instrument_id, product = \
            singleton.schema.markets_cartesian_product[0]
        return self.strategy.is_there_a_plan(
            long_instrument_id, short_instrument_id, product)

    @patch('ok_bot.trigger_strategy.estimate_profit', return_value=10000)
    @patch('ok_bot.trigger_strategy.calculate_amount_margin',
           return_value=10000)
    @patch('ok_bot.arbitrage_execution.ArbitrageTransaction')
    def test_strategy_trigger_in_basic_cases(
            self, arbitrage, calculate_amount_margin, estimate_profit):
        # Produce plan when there's enough profit estimation and amount margin
        calculate_amount_margin.return_value = 10000
        estimate_profit.return_value = 10000
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNotNone(plan)
        # No plan when not enough amount margin
        calculate_amount_margin.return_value = 0
        estimate_profit.return_value = 10000
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNone(plan)
        # No plan when profit estimation is negative
        calculate_amount_margin.return_value = 10000
        estimate_profit.return_value = -1
        plan = singleton.loop.run_until_complete(
            self.query_plan_after_ready()
        )
        self.assertIsNone(plan)

    @patch('ok_bot.trigger_strategy.estimate_profit', return_value=10000)
    def test_make_arbitrage_plan(self, _):
        async def _test():
            await singleton.order_book.ready
            week = singleton.schema.all_instrument_ids[0]
            bi_week = singleton.schema.all_instrument_ids[1]
            gap = singleton.order_book.market_depth(bi_week).best_bid_price() -\
                singleton.order_book.market_depth(week).best_ask_price()
            plan = make_arbitrage_plan(
                slow_instrument_id=week,
                fast_instrument_id=bi_week,
                slow_side=LONG,
                fast_side=SHORT,
                open_price_gap=gap,
                close_price_gap=gap * 0.5
            )
            self.assertGreaterEqual(plan.slow_price,
                                    singleton.order_book.market_depth(
                                        week).best_ask_price())

            gap = singleton.order_book.market_depth(week).best_bid_price() -\
                singleton.order_book.market_depth(bi_week).best_ask_price()
            plan = make_arbitrage_plan(
                slow_instrument_id=week,
                fast_instrument_id=bi_week,
                slow_side=SHORT,
                fast_side=LONG,
                open_price_gap=gap,
                close_price_gap=gap * 0.5
            )
            self.assertLessEqual(plan.slow_price,
                                 singleton.order_book.market_depth(
                                     week).best_bid_price())
        singleton.loop.run_until_complete(_test())

    def test_calculate_amount_margin(self):
        async def _test():
            await singleton.order_book.ready
            week = singleton.schema.all_instrument_ids[0]
            market_depth = singleton.order_book.market_depth(week)
            margin = calculate_amount_margin(market_depth.ask(),
                                             market_depth.bid(),
                                             lambda ask, bid: ask < bid)
            self.assertEqual(margin, 0)
            margin = calculate_amount_margin(market_depth.ask(),
                                             market_depth.bid(),
                                             lambda ask, bid: ask >= bid)
            self.assertEqual(margin, min(
                sum([order.volume for order in market_depth.ask()]),
                sum([order.volume for order in market_depth.bid()])
            ))

        singleton.loop.run_until_complete(_test())


if __name__ == '__main__':
    unittest.main()
