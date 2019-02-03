import asyncio
import logging
import unittest
from unittest import TestCase
from unittest.mock import MagicMock

from ok_bot import constants, logger, singleton, trader
from ok_bot.arbitrage_execution import LONG, SHORT
from ok_bot.order_executor import OrderExecutor


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class TestTrader(TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
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

    def test_spot_profit(self):
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trader.spot_profit(100, 110, 200, 200))
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trader.spot_profit(100, 100, 200, 190))
        self.assertLess(constants.MIN_ESTIMATE_PROFIT,
                        trader.spot_profit(100, 105, 200, 195))
        self.assertGreater(constants.MIN_ESTIMATE_PROFIT,
                           trader.spot_profit(100, 100.1, 200, 200))
        self.assertAlmostEqual(
            -(10 / 100 * 2 + 10 / 200 * 2) * constants.FEE_RATE,
            trader.spot_profit(100, 100, 200, 200))

    def test_estimate_profit(self):
        self.assertLess(
            trader.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=50
            ),
            -(10 / 100 * 2 + 10 / 150 * 2) * constants.FEE_RATE
        )

        self.assertGreater(
            trader.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=10
            ),
            constants.MIN_ESTIMATE_PROFIT
        )

        self.assertLess(
            trader.estimate_profit(
                {
                    LONG: 100,
                    SHORT: 150,
                },
                gap_threshold=48
            ),
            constants.MIN_ESTIMATE_PROFIT
        )


if __name__ == '__main__':
    import unittest
    unittest.main()
