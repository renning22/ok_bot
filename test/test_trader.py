import asyncio
import logging
import unittest
from unittest import TestCase
from unittest.mock import Mock, patch

from ok_bot import constants, logger, singleton
from ok_bot.mock import AsyncMock
from ok_bot.order_executor import OrderExecutor
from ok_bot.trigger_strategy import ArbitragePlan
from ok_bot.order_book import AvailableOrder


class TestTrader(TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO, log_to_stderr=True)
        singleton.initialize_objects_with_dev_db('ETH')
        singleton.rest_api = AsyncMock()

    def test_cool_down(self):
        async def _testing_coroutine():
            order_exe = OrderExecutor(
                singleton.schema.all_instrument_ids[0],
                amount=1,
                price=10000,
                timeout_sec=10,
                is_market_order=False,
                logger=logging.getLogger()
            )
            constants.INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 10
            singleton.rest_api.open_long_order.return_value = \
                (None, constants.REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH)
            result = await order_exe.open_long_position()
            logging.info('open_long_position result: %s', result)
            logging.info('start sleeping')
            await asyncio.sleep(5)
            self.assertTrue(singleton.trader.is_in_cooldown)
            logging.info('start another sleeping')
            await asyncio.sleep(8)  # wait for cool down to finish
            self.assertFalse(singleton.trader.is_in_cooldown)

        singleton.loop.run_until_complete(_testing_coroutine())

    def test_concurrent_trans_on_single_tick(self):
        singleton.trader.kick_off_arbitrage = Mock()
        singleton.trader.max_parallel_transaction_num = 15
        long_instrument, short_instrument, product = \
            singleton.schema.markets_cartesian_product[0]
        singleton.trader.trigger_strategy.is_there_a_plan = Mock(
            return_value=ArbitragePlan(
                volume=1,
                slow_instrument_id=long_instrument,
                fast_instrument_id=short_instrument,
                slow_side=constants.LONG,
                fast_side=constants.SHORT,
                slow_price=100,
                fast_price=200,
                close_price_gap=50,
                estimate_net_profit=10,
            )
        )
        market_depth_mock = Mock()
        singleton.order_book.market_depth = Mock(return_value=market_depth_mock)
        market_depth_mock.ask = Mock(
            return_value=[AvailableOrder(100, 500), ]
        )
        market_depth_mock.bid = Mock(
            return_value=[AvailableOrder(100, 500), ]
        )

        singleton.trader.process_pair(
            long_instrument, short_instrument, product)
        self.assertEquals(singleton.trader.kick_off_arbitrage.call_count, 15)


if __name__ == '__main__':
    unittest.main()
