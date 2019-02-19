import asyncio
import collections
import concurrent
import datetime
import logging
import time
import uuid

from . import singleton
from .constants import (CLOSE_POSITION_ORDER_TIMEOUT_SECOND,
                        FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, LONG,
                        MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                        PRICE_CONVERGE_TIMEOUT_IN_SECOND, SHORT,
                        SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
from .logger import create_transaction_logger, init_global_logger
from .order_executor import OrderExecutionResult, OrderExecutor
from .report import Report
from .trigger_strategy import calculate_amount_margin

ArbitrageLeg = collections.namedtuple(
    'ArbitrageLeg',
    ['instrument_id', 'side', 'volume', 'price'])


class WaitingPriceConverge:
    def __init__(self, transaction, timeout_sec):
        self._transaction = transaction
        self._timeout_sec = timeout_sec
        self.slow_leg = transaction.slow_leg
        self.fast_leg = transaction.fast_leg
        if self.slow_leg.side == SHORT and self.fast_leg.side == LONG:
            self._ask_stack_instrument, self._bid_stack_instrument = \
                self.slow_leg.instrument_id,  self.fast_leg.instrument_id
        elif self.slow_leg.side == LONG and self.fast_leg.side == SHORT:
            self._ask_stack_instrument, self._bid_stack_instrument = \
                self.fast_leg.instrument_id, self.slow_leg.instrument_id
        else:
            raise Exception(f'Slow leg: {self.slow_leg.side}, '
                            f'fast leg: {self.fast_leg.side}')
        self.logger = transaction.logger
        self._future = singleton.loop.create_future()

    async def __aenter__(self):
        if self._timeout_sec <= 0:
            return None  # None means timeout

        singleton.book_listener.subscribe(
            self._transaction.slow_leg.instrument_id, self)
        singleton.book_listener.subscribe(
            self._transaction.fast_leg.instrument_id, self)

        try:
            res = await asyncio.wait_for(
                self._future, timeout=self._timeout_sec)
        except concurrent.futures.TimeoutError:
            return None
        else:
            return res

    async def __aexit__(self, type, value, traceback):
        singleton.book_listener.unsubscribe(
            self._transaction.slow_leg.instrument_id, self)
        singleton.book_listener.unsubscribe(
            self._transaction.fast_leg.instrument_id, self)

    def tick_received(self, instrument_id,
                      ask_prices, ask_vols, bid_prices, bid_vols,
                      timestamp):
        assert instrument_id in [self._ask_stack_instrument,
                                 self._bid_stack_instrument]

        if self._future.done():
            return

        cur_amount_margin = calculate_amount_margin(
            singleton.order_book.market_depth(
                self._ask_stack_instrument).ask(),
            singleton.order_book.market_depth(
                self._bid_stack_instrument).bid(),
            lambda ask_price, bid_price:
            ask_price - bid_price <= self._transaction.close_price_gap_threshold
        )

        self.logger.log_every_n_seconds(
            logging.INFO,
            '[WAITING PRICE CONVERGE] current_gap:%.3f, max_gap: %.3f, '
            'available_amount: %d',
            30,
            singleton.order_book.market_depth(
                self._ask_stack_instrument).best_ask_price()
            - singleton.order_book.market_depth(
                self._bid_stack_instrument).best_bid_price(),
            self._transaction.close_price_gap_threshold,
            cur_amount_margin
        )

        if cur_amount_margin >= MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE:
            self.logger.info(
                '[WAITING PRICE SUCCEEDED] current_gap:%.3f,'
                ' max_gap: %.3f, available_amount: %d',
                singleton.order_book.market_depth(
                    self._ask_stack_instrument).best_ask_price()
                - singleton.order_book.market_depth(
                    self._bid_stack_instrument).best_bid_price(),
                self._transaction.close_price_gap_threshold,
                cur_amount_margin
            )
            self.logger.info(
                'slow-side orderbook: %s%s',
                self.slow_leg.side,
                singleton.order_book.market_depth(self.slow_leg.instrument_id))
            self.logger.info(
                'fast-side orderbook: %s%s',
                self.fast_leg.side,
                singleton.order_book.market_depth(self.fast_leg.instrument_id))
            self._future.set_result(cur_amount_margin)


class ArbitrageTransaction:
    def __init__(self,
                 slow_leg,
                 fast_leg,
                 close_price_gap_threshold,
                 estimate_net_profit=None):
        assert slow_leg.volume == fast_leg.volume
        self.id = str(uuid.uuid4())
        self.slow_leg = slow_leg
        self.fast_leg = fast_leg
        self.close_price_gap_threshold = close_price_gap_threshold
        self.estimate_net_profit = estimate_net_profit
        self.logger = create_transaction_logger(self.id)
        self._start_time_sec = time.time()
        self.report = Report(transaction_id=self.id,
                             slow_instrument_id=slow_leg.instrument_id,
                             fast_instrument_id=fast_leg.instrument_id,
                             logger=self.logger)
        self._db_transaction_status_updater = (
            lambda status:
                singleton.db.async_update_transaction(
                    transaction_id=self.id,
                    vol=self.slow_leg.volume,
                    slow_price=self.slow_leg.price,
                    fast_price=self.fast_leg.price,
                    close_price_gap=close_price_gap_threshold,
                    start_time_sec=self._start_time_sec,
                    end_time_sec=time.time(),
                    estimate_net_profit=estimate_net_profit,
                    status=status)
        )

    def open_position(self, leg: ArbitrageLeg, timeout_in_sec: int)\
            -> OrderExecutionResult:
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor(
            instrument_id=leg.instrument_id,
            amount=leg.volume,
            price=leg.price,
            timeout_sec=timeout_in_sec,
            is_market_order=False,
            logger=self.logger,
            transaction_id=self.id)
        if leg.side == LONG:
            return order_executor.open_long_position()
        else:
            return order_executor.open_short_position()

    def close_position(self, leg: ArbitrageLeg, timeout_in_sec: int) \
            -> OrderExecutionResult:
        assert leg.side in [LONG, SHORT]
        if leg.side == LONG:
            price = singleton.order_book.market_depth(
                leg.instrument_id).best_bid_price()
        else:
            price = singleton.order_book.market_depth(
                leg.instrument_id).best_ask_price()

        order_executor = OrderExecutor(
            instrument_id=leg.instrument_id,
            amount=leg.volume,
            price=price,
            timeout_sec=timeout_in_sec,
            is_market_order=False,
            logger=self.logger,
            transaction_id=self.id)
        if leg.side == LONG:
            self.logger.info('[Close Attempt] closing long position with %.3f',
                             price)
            return order_executor.close_long_order()
        else:
            self.logger.info('[Close Attempt] closing short position with %.3f',
                             price)
            return order_executor.close_short_order()

    async def close_position_guaranteed(self, leg):
        while True:
            close_status = await self.close_position(
                leg, CLOSE_POSITION_ORDER_TIMEOUT_SECOND)
            if close_status.succeeded:
                self.logger.info(
                    '[CLOSE POSITION GUARANTEED] succeeded: %s', leg)
                return close_status
            else:
                self.logger.warning(
                    '[CLOSE POSITION GUARANTEED] failed with %s, will retry %s',
                    close_status, leg)

    async def process(self):
        self._db_transaction_status_updater('started')
        self.logger.info('=== arbitrage transaction started ===')
        self.logger.info(
            f'id: {self.id}, max gap: {self.close_price_gap_threshold:.4f}')
        self.logger.info(
            'slow leg:\n%s%s', self.slow_leg,
            singleton.order_book.market_depth(self.slow_leg.instrument_id))
        self.logger.info(
            'fast leg:\n%s%s', self.fast_leg,
            singleton.order_book.market_depth(self.fast_leg.instrument_id))
        result = await self._process()

        # We don't want to block new arbitrage spawned during report generating.
        singleton.trader.on_going_arbitrage_count -= 1
        net_profit = await self.report.report_profit()

        self.logger.info('[SUMMARY] id:%s\n'
                         'net_profit: %.8f %s (estimate: %.8f)',
                         self.id,
                         net_profit,
                         singleton.coin_currency,
                         self.estimate_net_profit)
        self.logger.info('=== arbitrage transaction ended ===')
        return result

    async def _process(self):
        self._db_transaction_status_updater('opening_slow_leg')
        self.logger.info('[OPENING SLOW]')
        slow_open_order = await self.open_position(
            self.slow_leg, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        )

        if not slow_open_order.succeeded:
            self.logger.info(f'[SLOW FAILED] {slow_open_order}')
            self._db_transaction_status_updater('ended_slow_leg_failed')
            return False
        else:
            self.logger.info(f'[SLOW FULFILLED] {slow_open_order.order_id}')
            self.report.slow_open_order_id = slow_open_order.order_id

        self._db_transaction_status_updater('opening_fast_leg')
        self.logger.info('[OPENING FAST]')
        fast_open_order = await self.open_position(
            self.fast_leg, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        )

        if not fast_open_order.succeeded:
            self.logger.info(f'[FAST FAILED] {fast_open_order}')
            self._db_transaction_status_updater('ended_fast_leg_failed')
            slow_close_order = await self.close_position_guaranteed(
                self.slow_leg)
            assert slow_close_order.succeeded
            self.logger.info(
                f'[SLOW POSITION CLOSED] {slow_close_order.order_id}')
            self.report.slow_close_order_id = slow_close_order.order_id
            return False
        else:
            self.logger.info(f'[FAST FULFILLED] {fast_open_order.order_id}')
            self.report.fast_open_order_id = fast_open_order.order_id

        self.logger.info(
            f'[BOTH FULFILLED] wait for '
            f'{PRICE_CONVERGE_TIMEOUT_IN_SECOND} seconds')
        self._db_transaction_status_updater('waiting_converge')

        async with WaitingPriceConverge(
                transaction=self,
                timeout_sec=PRICE_CONVERGE_TIMEOUT_IN_SECOND
        ) as converge:
            if converge is None:
                # timeout, close the position
                self.logger.info('[CONVERGE TIMEOUT]')
                self._db_transaction_status_updater('ended_converge_timeout')
            else:
                self.logger.info(f'[CONVERGED] margin: {converge}')
                self._db_transaction_status_updater('ended_normally')
                # Log current speed from each side. I suspect the slow/fast
                # will always reverse when converged.
                close_slow_speed = singleton.order_book.price_speed(
                    self.slow_leg.instrument_id,
                    {LONG: 'bid', SHORT: 'ask'}[self.slow_leg.side]
                )
                close_fast_speed = singleton.order_book.price_speed(
                    self.fast_leg.instrument_id,
                    {LONG: 'bid', SHORT: 'ask'}[self.fast_leg.side]
                )
                self.logger.info(f'Price speed reversed: '
                                 f'{close_slow_speed > close_fast_speed}, '
                                 f'current slow speed: {close_slow_speed:.3f}, '
                                 f'current fast speed: {close_fast_speed:.3f}')

        fast_close_order, slow_close_order = await asyncio.gather(
            self.close_position_guaranteed(self.fast_leg),
            self.close_position_guaranteed(self.slow_leg)
        )
        assert fast_close_order.succeeded
        assert slow_close_order.succeeded

        self.logger.info(f'[ALL POSITION CLOSED]')

        self.report.fast_close_order_id = fast_close_order.order_id
        self.report.slow_close_order_id = slow_close_order.order_id
        return True


async def _test_coroutine():
    from .quant import Quant
    await singleton.websocket.ready
    logging.info('WebSocket subscription finished')
    week_instrument = singleton.schema.all_instrument_ids[0]
    quarter_instrument = singleton.schema.all_instrument_ids[-1]
    transaction = ArbitrageTransaction(
        slow_leg=ArbitrageLeg(instrument_id=quarter_instrument,
                              side=SHORT,
                              volume=Quant(1),
                              price=Quant(99.0)),
        fast_leg=ArbitrageLeg(instrument_id=week_instrument,
                              side=LONG,
                              volume=1,
                              price=95.0),
        close_price_gap_threshold=1,
    )
    await transaction.process()

if __name__ == '__main__':
    init_global_logger(log_level=logging.INFO)
    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    singleton.loop.create_task(_test_coroutine())
    singleton.start_loop()
