import logging
import unittest
from unittest.mock import Mock

from ok_bot import constants, db, logger, order_book, order_executor, singleton
from ok_bot.constants import MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE
from ok_bot.mock import AsyncMock, MockBookListerner_constantPriceGenerator

_FAKE_MARKET_PRICE = 100.0
_FAKE_MARKET_VOL = MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE
_FAKE_ORDER_ID = 12345
_PRICE = 100.0
_SIZE = 1


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
        logger.init_global_logger(log_level=logging.INFO, log_to_stderr=False)
        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        singleton.rest_api = AsyncMock()
        singleton.rest_api.open_long_order.__name__ = Mock(
            return_value='open_long_order')
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

        singleton.book_listener = MockBookListerner_constantPriceGenerator(
            price=_FAKE_MARKET_PRICE,
            vol=MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE)

        # OrderBook will re-subscribe on all_instrument_ids from schema
        # automatically. (see __init__)
        singleton.order_book = order_book.OrderBook()

    def tearDown(self):
        singleton.loop.run_until_complete(
            singleton.book_listener.shutdown_broadcast_loop())
        singleton.db.shutdown(wait=True)

    def test_order_cancelled(self):
        async def _testing_coroutine():
            await singleton.order_book.ready
            logging.info('Orderbook ramping up finished')

            executor = order_executor.OrderExecutor(
                instrument_id=singleton.schema.all_instrument_ids[0],
                amount=_SIZE,
                price=_PRICE,
                timeout_sec=20,
                is_market_order=False,
                logger=logging)

            logging.info('open_long_position has been called')
            order_status = await executor.open_long_position()
            logging.info('result: %s', order_status)

            self.assertEqual(order_status.order_id, _FAKE_ORDER_ID)
            self.assertEqual(order_status.amount, _SIZE)
            self.assertEqual(order_status.fulfilled_quantity, 0)

        singleton.loop.run_until_complete(_testing_coroutine())

        self.assertEqual(
            singleton.order_listener.last_subscribed_order_id, _FAKE_ORDER_ID)


if __name__ == '__main__':
    unittest.main()
