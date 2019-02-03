import asyncio
import collections
import concurrent
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
from .order_executor import OPEN_POSITION_STATUS__SUCCEEDED, OrderExecutor
from .util import amount_margin

ArbitrageLeg = collections.namedtuple(
    'ArbitrageLeg',
    ['instrument_id', 'side', 'volume', 'price'])


class WaitingPriceConverge:
    def __init__(self, transaction, timeout_sec):
        self._transaction = transaction
        self._timeout_sec = timeout_sec
        self._slow_leg = transaction.slow_leg
        self._fast_leg = transaction.fast_leg
        if self._slow_leg.side == SHORT and self._fast_leg.side == LONG:
            self._ask_stack_instrument, self._bid_stack_instrument = \
                self._slow_leg.instrument_id,  self._fast_leg.instrument_id
        elif self._slow_leg.side == LONG and self._fast_leg.side == SHORT:
            self._ask_stack_instrument, self._bid_stack_instrument = \
                self._fast_leg.instrument_id, self._slow_leg.instrument_id
        else:
            raise Exception(f'Slow leg: {self._slow_leg.side}, '
                            f'fast leg: {self._fast_leg.side}')
        self._ask_stack = None
        self._bid_stack = None
        self.logger = transaction.logger
        self._future = singleton.loop.create_future()

    async def __aenter__(self):
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

        if instrument_id == self._bid_stack_instrument:
            self._bid_stack = list(zip(bid_prices, bid_vols))
        else:
            assert instrument_id == self._ask_stack_instrument
            self._ask_stack = list(zip(ask_prices, ask_vols))

        should_close, amount_margin = self._should_close_arbitrage()
        if should_close:
            self._future.set_result(amount_margin)

    def _should_close_arbitrage(self):
        if self._ask_stack is None or self._bid_stack is None:
            return False, -1

        available_amount = amount_margin(
            self._ask_stack,
            self._bid_stack,
            lambda ask_price, bid_price:
            ask_price - bid_price <= self._transaction.close_price_gap_threshold)

        self.logger.log_every_n_seconds(
            logging.INFO,
            '[WAITING PRICE CONVERGE] current_gap:%.3f, max_gap: %.3f, '
            'available_amount: %d',
            10,
            self._ask_stack[0][0] - self._bid_stack[0][0],
            self._transaction.close_price_gap_threshold,
            available_amount
        )
        return (available_amount >= MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                available_amount)


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
        self.logger = create_transaction_logger(str(self.id))
        self._start_time_sec = time.time()
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

    def open_position(self, leg, timeout_in_sec):
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

    def close_position(self, leg, timeout_in_sec):
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor(
            instrument_id=leg.instrument_id,
            amount=leg.volume,
            price=-1,
            timeout_sec=timeout_in_sec,
            is_market_order=True,
            logger=self.logger,
            transaction_id=self.id)
        if leg.side == LONG:
            return order_executor.close_long_order()
        else:
            return order_executor.close_short_order()

    async def close_position_guaranteed(self, leg):
        while True:
            self.logger.info('[CLOSE POSITION GUARANTEED] try: %s', leg)
            close_status = await self.close_position(
                leg, CLOSE_POSITION_ORDER_TIMEOUT_SECOND)
            if close_status == OPEN_POSITION_STATUS__SUCCEEDED:
                self.logger.info(
                    '[CLOSE POSITION GUARANTEED] succeeded: %s', leg)
                return
            else:
                self.logger.warning(
                    '[CLOSE POSITION GUARANTEED] failed with %s, will retry: %s',
                    close_status, leg)

    async def process(self):
        self._db_transaction_status_updater('started')
        self.logger.info('=== arbitrage transaction started ===')
        self.logger.info(f'id: {self.id}')
        self.logger.info(f'slow leg: {self.slow_leg}')
        self.logger.info(f'fast leg: {self.fast_leg}')
        result = await self._process()
        self.logger.info('=== arbitrage transaction ended ===')
        singleton.trader.on_going_arbitrage_count -= 1
        return result

    async def _process(self):
        self._db_transaction_status_updater('opening_slow_leg')
        slow_leg_order_status = await self.open_position(
            self.slow_leg, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        )

        if slow_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            self.logger.info(
                f'[SLOW FAILED] failed to open slow leg {self.slow_leg} '
                f'({slow_leg_order_status})')
            self._db_transaction_status_updater('ended_slow_leg_failed')
            return False
        self.logger.info(f'[SLOW FULFILLED] {self.slow_leg} was fulfilled, '
                         f'will open position for fast leg')

        self._db_transaction_status_updater('opening_fast_leg')
        fast_leg_order_status = await self.open_position(
            self.fast_leg, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        )

        if fast_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            self.logger.info(f'[FAST FAILED] failed to open fast leg '
                             f'{self.fast_leg} '
                             f'({fast_leg_order_status}), '
                             'will close slow leg position before aborting the '
                             'rest of this transaction')
            self._db_transaction_status_updater('ended_fast_leg_failed')
            await self.close_position_guaranteed(self.slow_leg)
            self.logger.info(
                f'slow leg position {self.slow_leg} has been closed')
            return False

        self.logger.info(f'[BOTH FULFILLED] fast leg {self.fast_leg} order '
                         f'fulfilled, will wait '
                         f'for converge for {PRICE_CONVERGE_TIMEOUT_IN_SECOND} '
                         f'seconds')
        self._db_transaction_status_updater('waiting_converge')

        async with WaitingPriceConverge(
                transaction=self,
                timeout_sec=PRICE_CONVERGE_TIMEOUT_IN_SECOND) as converge:
            if converge is None:
                # timeout, close the position
                self.logger.info('[CONVERGE TIMEOUT] prices failed to converge '
                                 'in time, closing both legs')
                self._db_transaction_status_updater('ended_converge_timeout')
            else:
                self.logger.info(f'[CONVERGED] prices converged with enough '
                                 f'margin({converge}), closing both legs')
                self._db_transaction_status_updater('ended_normally')

        fast_close_task = self.close_position_guaranteed(self.fast_leg)
        slow_close_task = self.close_position_guaranteed(self.slow_leg)
        await asyncio.gather(fast_close_task, slow_close_task)
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
                              price=Quant(105.0)),
        fast_leg=ArbitrageLeg(instrument_id=week_instrument,
                              side=LONG,
                              volume=1,
                              price=110.0),
        close_price_gap_threshold=1,
    )
    await transaction.process()

if __name__ == '__main__':
    init_global_logger(log_level=logging.INFO)
    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    asyncio.ensure_future(_test_coroutine())
    singleton.start_loop()
