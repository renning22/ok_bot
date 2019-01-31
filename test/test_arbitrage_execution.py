import asyncio
import time
from unittest.mock import MagicMock, call, patch

from absl import logging
from absl.testing import absltest

from ok_bot import singleton
from ok_bot.arbitrage_execution import ArbitrageLeg, ArbitrageTransaction
from ok_bot.constants import (FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, LONG,
                              MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                              SHORT, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
from ok_bot.order_executor import (ORDER_EXECUTION_STATUS__SUCCEEDED,
                                   OrderExecutionResult, OrderExecutor)

_FAKE_MARKET_PRICE = 100.0
_FAKE_MARKET_VOL = MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MockBookListerner_constantPriceGenerator:
    def __init__(self):
        self._subscribers = {}
        self._running = None
        self._broadcast_loop = None

    def start_broadcast_loop(self):
        assert self._broadcast_loop is None
        self._running = True
        self._broadcast_loop = asyncio.create_task(
            self._kick_off_broadcast_loop())

    async def shutdown_broadcast_loop(self):
        assert self._broadcast_loop is not None
        self._running = False
        logging.info('shutting down broadcast_loop.')
        await self._broadcast_loop
        logging.info('broadcast_loop has been shut down.')

    def subscribe(self, instrument_id, subscriber):
        self._subscribers[instrument_id] = (
            lambda: subscriber.tick_received(
                instrument_id=instrument_id,
                ask_prices=[_FAKE_MARKET_PRICE],
                ask_vols=[_FAKE_MARKET_VOL],
                bid_prices=[_FAKE_MARKET_PRICE],
                bid_vols=[_FAKE_MARKET_VOL],
                timestamp=int(time.time())
            )
        )

    def unsubscribe(self, instrument_id, subscriber):
        del self._subscribers[instrument_id]

    async def _kick_off_broadcast_loop(self):
        while self._running:
            for subscriber, callback in self._subscribers.items():
                logging.info('sending tick_received to %s', subscriber)
                callback()
            await asyncio.sleep(1)


@patch('uuid.uuid4', return_value='11111111-1111-1111-1111-111111111111')
@patch('ok_bot.arbitrage_execution.OrderExecutor')
class TestArbitrageExecution(absltest.TestCase):

    def setUp(self):
        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        singleton.rest_api = None
        singleton.book_listener = MockBookListerner_constantPriceGenerator()
        singleton.order_listener = None
        singleton.order_book = None
        singleton.trader = None
        singleton.websocket = None

    def tearDown(self):
        singleton.loop.run_until_complete(
            singleton.book_listener.shutdown_broadcast_loop())
        singleton.db.shutdown(wait=True)

    def test_arbitrage_converged(self, MockOrderExecutor, mock_uuid4):
        mock_order_executor = AsyncMock()
        MockOrderExecutor.return_value = mock_order_executor

        successful_execution_result = OrderExecutionResult(
            ORDER_EXECUTION_STATUS__SUCCEEDED)
        successful_execution_result.order_id = 12345
        successful_execution_result.filled_qty = 1
        successful_execution_result.fee = 0.001
        successful_execution_result.price_avg = 100
        successful_execution_result.has_post_order_info_collection_done = (
            asyncio.Future())
        successful_execution_result.has_post_order_info_collection_done.set_result(
            True)

        mock_order_executor.open_long_position.return_value = (
            successful_execution_result)
        mock_order_executor.open_short_position.return_value = (
            successful_execution_result)
        mock_order_executor.close_long_order.return_value = (
            successful_execution_result)
        mock_order_executor.close_short_order.return_value = (
            successful_execution_result)

        async def _testing_coroutine():
            singleton.book_listener.start_broadcast_loop()

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
                     timeout_sec=None,
                     is_market_order=True,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call(instrument_id=quarter_instrument,
                     amount=1,
                     price=-1,
                     timeout_sec=None,
                     is_market_order=True,
                     logger=transaction.logger,
                     transaction_id=transaction.id),
                call().close_long_order(),
                call().close_short_order(),
            ])

        singleton.loop.run_until_complete(_testing_coroutine())


if __name__ == '__main__':
    absltest.main()
