import asyncio
import logging
import unittest
from unittest.mock import MagicMock

from ok_bot import constants, db, logger, order_executor, singleton

_FAKE_ORDER_ID = 12345
_SIZE = 1
_PRICE = 100.0


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MockOrderListerner_cancelImmediately:
    def __init__(self):
        self.last_subscribed_order_id = None

    def subscribe(self, order_id, subscriber):
        self.last_subscribed_order_id = order_id
        singleton.loop.call_later(
            1, lambda: subscriber.order_cancelled(order_id))

    def unsubscribe(self, order_id, subscriber):
        pass


class TestOrderExecutor(unittest.TestCase):

    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
        singleton.db = db.DevDb()
        singleton.db.create_tables_if_not_exist()
        singleton.rest_api = AsyncMock()
        singleton.rest_api.open_long_order.__name__ = 'fake_open_long_order'
        singleton.rest_api.open_long_order.return_value = (
            _FAKE_ORDER_ID, None)
        singleton.rest_api.get_order_info.return_value = {
            'status': constants.ORDER_STATUS_CODE__CANCELLED,
            'order_id': _FAKE_ORDER_ID,
            'size': _SIZE,
            'filled_qty': 0,
            'price': _PRICE,
            'price_avg': 0,
            'fee': 0,
            'type': 1,
            'timestamp': '2019-01-27 15:38:24',
        }
        singleton.order_listener = MockOrderListerner_cancelImmediately()

    def test_order_cancelled(self):
        async def _testing_coroutine(test_class):
            executor = order_executor.OrderExecutor(
                instrument_id='instrument_id',
                amount=_SIZE,
                price=_PRICE,
                timeout_sec=20,
                is_market_order=False,
                logger=logging)

            logging.info('open_long_position has been called')
            order_status = await executor.open_long_position()
            logging.info('result: %s', order_status)
            test_class.assertIs(
                order_status, order_executor.OPEN_POSITION_STATUS__CANCELLED)

        singleton.loop = asyncio.get_event_loop()
        singleton.loop.run_until_complete(_testing_coroutine(self))

        self.assertEqual(
            singleton.order_listener.last_subscribed_order_id, _FAKE_ORDER_ID)


if __name__ == '__main__':
    unittest.main()
