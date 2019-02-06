import asyncio
import logging
import time
import unittest
from unittest.mock import MagicMock, call, patch

from ok_bot import logger, singleton
from ok_bot.arbitrage_execution import ArbitrageLeg, ArbitrageTransaction
from ok_bot.constants import (CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                              FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, LONG,
                              MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                              SHORT, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
from ok_bot.mock import AsyncMock, MockBookListerner_constantPriceGenerator
from ok_bot.order_executor import (OPEN_POSITION_STATUS__SUCCEEDED,
                                   OrderExecutor)

_FAKE_MARKET_PRICE = 100.0
_FAKE_MARKET_VOL = MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE


@patch('uuid.uuid4', return_value='11111111-1111-1111-1111-111111111111')
@patch('ok_bot.arbitrage_execution.OrderExecutor')
class TestArbitrageExecution(unittest.TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        singleton.rest_api = None
        singleton.book_listener = MockBookListerner_constantPriceGenerator(
            price=_FAKE_MARKET_PRICE,
            vol=MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE)
        singleton.order_listener = None
        singleton.order_book = None
        singleton.trader = MagicMock()
        singleton.websocket = None

    def tearDown(self):
        singleton.loop.run_until_complete(
            singleton.book_listener.shutdown_broadcast_loop())
        singleton.db.shutdown(wait=True)

    def test_arbitrage_converge(self, MockOrderExecutor, mock_uuid4):
        mock_order_executor = AsyncMock()
        MockOrderExecutor.return_value = mock_order_executor
        mock_order_executor.open_long_position.return_value = (
            OPEN_POSITION_STATUS__SUCCEEDED(10001))
        mock_order_executor.open_short_position.return_value = (
            OPEN_POSITION_STATUS__SUCCEEDED(10002))
        mock_order_executor.close_long_order.return_value = (
            OPEN_POSITION_STATUS__SUCCEEDED(10003))
        mock_order_executor.close_short_order.return_value = (
            OPEN_POSITION_STATUS__SUCCEEDED(10004))

        async def _testing_coroutine():
            week_instrument = 'ETH-USD-190201'
            quarter_instrument = 'ETH-USD-190329'
            transaction = ArbitrageTransaction(
                slow_leg=ArbitrageLeg(instrument_id=quarter_instrument,
                                      side=SHORT,
                                      volume=1,
                                      price=100.0),
                fast_leg=ArbitrageLeg(instrument_id=week_instrument,
                                      side=LONG,
                                      volume=1,
                                      price=80.0),
                close_price_gap_threshold=1,
            )
            result = await transaction.process()
            self.assertTrue(result)
            self.assertEqual(quarter_instrument,
                             transaction.report.slow_instrument_id)
            self.assertEqual(week_instrument,
                             transaction.report.fast_instrument_id)
            self.assertEqual(10002, transaction.report.slow_open_order_id)
            self.assertEqual(10004, transaction.report.slow_close_order_id)
            self.assertEqual(10001, transaction.report.fast_open_order_id)
            self.assertEqual(10003, transaction.report.fast_close_order_id)

            MockOrderExecutor.assert_has_calls([
                call(instrument_id=quarter_instrument,
                     amount=1,
                     price=100.0,
                     timeout_sec=SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call().open_short_position(),
                call(instrument_id=week_instrument,
                     amount=1,
                     price=80.0,
                     timeout_sec=FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call().open_long_position(),
                call(instrument_id=week_instrument,
                     amount=1,
                     price=-1,
                     timeout_sec=CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                     is_market_order=True,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call().close_long_order(),
                call(instrument_id=quarter_instrument,
                     amount=1,
                     price=-1,
                     timeout_sec=CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                     is_market_order=True,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call().close_short_order(),
            ])

        singleton.loop.run_until_complete(_testing_coroutine())


if __name__ == '__main__':
    unittest.main()
