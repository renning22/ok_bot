import asyncio
from unittest.mock import MagicMock

from absl import logging
from absl.testing import absltest

from ok_bot import constants, singleton
from ok_bot.order_executor import OrderExecutor


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class TestTrader(absltest.TestCase):
    def setUp(self):
        singleton.initialize_objects_with_dev_db('ETH')
        singleton.rest_api = AsyncMock()

    def tearDown(self):
        singleton.db.shutdown(wait=True)

    def test_cool_down(self):
        async def _testing_coroutine(test_class):
            order_exe = OrderExecutor(
                singleton.schema.all_instrument_ids[0],
                amount=1,
                price=10000,
                timeout_sec=10,
                is_market_order=False,
                logger=logging
            )
            constants.INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 10
            singleton.rest_api.open_long_order.return_value = \
                (None, constants.REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH)
            result = await order_exe.open_long_position()
            logging.info('open_long_position result: %s', result)
            logging.info('start sleeping')
            await asyncio.sleep(5)
            test_class.assertTrue(singleton.trader.is_in_cooldown)
            logging.info('start another sleeping')
            await asyncio.sleep(8)  # wait for cool down to finish
            test_class.assertFalse(singleton.trader.is_in_cooldown)

        singleton.loop.run_until_complete(_testing_coroutine(self))


if __name__ == '__main__':
    absltest.main()
