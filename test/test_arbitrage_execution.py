import logging
import unittest
from unittest.mock import MagicMock, call, patch

from ok_bot import logger, order_book, singleton
from ok_bot.arbitrage_execution import ArbitrageLeg, ArbitrageTransaction
from ok_bot.constants import (CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                              FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, LONG,
                              MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                              SHORT, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
from ok_bot.mock import AsyncMock, MockBookListerner_constantPriceGenerator
from ok_bot.order_executor import OrderExecutionResult, OrderExecutor

_FAKE_MARKET_PRICE = 100.0
_FAKE_MARKET_VOL = MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE


@patch('uuid.uuid4', return_value='11111111-1111-1111-1111-111111111111')
@patch('ok_bot.arbitrage_execution.OrderExecutor')
@patch('ok_bot.arbitrage_execution.Report')
class TestArbitrageExecution(unittest.TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO, log_to_stderr=True)
        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        singleton.rest_api = None
        singleton.book_listener = MockBookListerner_constantPriceGenerator(
            price=_FAKE_MARKET_PRICE,
            vol=MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE)
        singleton.order_listener = None
        singleton.trader = MagicMock()
        singleton.websocket = None

        # OrderBook will re-subscribe on all_instrument_ids from schema
        # automatically. (see __init__)
        singleton.order_book = order_book.OrderBook()

        self.week_instrument = singleton.schema.all_instrument_ids[0]
        self.quarter_instrument = singleton.schema.all_instrument_ids[-1]

    def tearDown(self):
        singleton.loop.run_until_complete(
            singleton.book_listener.shutdown_broadcast_loop())
        singleton.db.shutdown(wait=True)

    def test_arbitrage_converge(self, MockReport, MockOrderExecutor,
                                mock_uuid4):
        mock_order_executor = AsyncMock()
        MockOrderExecutor.return_value = mock_order_executor
        mock_order_executor.open_long_position.return_value = (
            OrderExecutionResult(order_id=10001, amount=1,
                                 fulfilled_quantity=1))
        mock_order_executor.open_short_position.return_value = (
            OrderExecutionResult(order_id=10002, amount=1,
                                 fulfilled_quantity=1))
        mock_order_executor.close_long_order.return_value = (
            OrderExecutionResult(order_id=10003, amount=1,
                                 fulfilled_quantity=1))
        mock_order_executor.close_short_order.return_value = (
            OrderExecutionResult(order_id=10004, amount=1,
                                 fulfilled_quantity=1))

        mock_report = AsyncMock()
        mock_report.report_profit.return_value = 0.001  # net_profit
        mock_report.__str__ = MagicMock(return_value='mock_report')
        mock_report.slow_open_prices = MagicMock()
        mock_report.fast_open_prices = MagicMock()
        mock_report.slow_close_prices = MagicMock()
        mock_report.fast_close_prices = MagicMock()
        MockReport.return_value = mock_report

        async def _testing_coroutine():
            await singleton.order_book.ready
            logging.info('Orderbook ramping up finished')
            transaction = ArbitrageTransaction(
                slow_leg=ArbitrageLeg(instrument_id=self.quarter_instrument,
                                      side=SHORT,
                                      volume=1,
                                      price=100.0),
                fast_leg=ArbitrageLeg(instrument_id=self.week_instrument,
                                      side=LONG,
                                      volume=1,
                                      price=80.0),
                close_price_gap_threshold=1,
                estimate_net_profit=0.002,
                z_score=6.0
            )
            result = await transaction.process()
            self.assertTrue(result)

            # Assert reports
            mock_report.report_profit.assert_called_once()
            self.assertEqual(10002, mock_report.slow_open_order_id)
            self.assertEqual(10004, mock_report.slow_close_order_id)
            self.assertEqual(10001, mock_report.fast_open_order_id)
            self.assertEqual(10003, mock_report.fast_close_order_id)

            mock_report.slow_open_prices.append.assert_called_once_with(100.0)
            mock_report.fast_open_prices.append.assert_called_once_with(80.0)
            mock_report.slow_close_prices.append.assert_called_once_with(100.0)
            mock_report.fast_close_prices.append.assert_called_once_with(100.0)

            MockOrderExecutor.assert_has_calls([
                call(instrument_id=self.quarter_instrument,
                     amount=1,
                     price=100.0,
                     timeout_sec=SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id,
                     safe_price=False),
                call().open_short_position(),
                call(instrument_id=self.week_instrument,
                     amount=1,
                     price=80.0,
                     timeout_sec=FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id,
                     safe_price=True),
                call().open_long_position(),
                call(instrument_id=self.week_instrument,
                     amount=1,
                     price=100.0,
                     timeout_sec=CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id,
                     safe_price=True),
                call().close_long_order(),
                call(instrument_id=self.quarter_instrument,
                     amount=1,
                     price=100.0,
                     timeout_sec=CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                     is_market_order=False,
                     logger=transaction.logger,
                     transaction_id=transaction.id,
                     safe_price=True),
                call().close_short_order(),
            ])

        singleton.loop.run_until_complete(_testing_coroutine())


if __name__ == '__main__':
    unittest.main()
