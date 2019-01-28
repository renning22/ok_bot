import collections
import uuid

import eventlet
from absl import app, logging

from . import singleton
from .constants import (FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, LONG,
                        MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE,
                        PRICE_CONVERGE_TIMEOUT_IN_SECOND, SHORT,
                        SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
from .future import Future
from .logger import create_transaction_logger
from .order_executor import OPEN_POSITION_STATUS__SUCCEEDED, OrderExecutor
from .util import amount_margin

ArbitrageLeg = collections.namedtuple('ArbitrageLeg',
                                      ['instrument_id', 'side', 'volume', 'price'])


class WaitingPriceConverge:
    def __init__(self, transaction):
        self._transaction = transaction
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

    def __enter__(self):
        singleton.book_listener.subscribe(
            self._transaction.slow_leg.instrument_id, self)
        singleton.book_listener.subscribe(
            self._transaction.fast_leg.instrument_id, self)
        self._future = Future()
        return self._future

    def __exit__(self, type, value, traceback):
        singleton.book_listener.unsubscribe(
            self._transaction.slow_leg.instrument_id, self)
        singleton.book_listener.unsubscribe(
            self._transaction.fast_leg.instrument_id, self)

    def tick_received(self, instrument_id,
                      ask_prices, ask_vols, bid_prices, bid_vols,
                      timestamp):
        assert instrument_id in [self._ask_stack_instrument,
                                 self._bid_stack_instrument]

        if instrument_id == self._bid_stack_instrument:
            self._bid_stack = list(zip(bid_prices, bid_vols))
        else:
            assert instrument_id == self._ask_stack_instrument
            self._ask_stack = list(zip(ask_prices, ask_vols))

        should_close, amount_margin = self._should_close_arbitrage()
        if should_close:
            self._future.set(amount_margin)

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
            2,
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
                 close_price_gap_threshold):
        self.id = str(uuid.uuid4())
        self.slow_leg = slow_leg
        self.fast_leg = fast_leg
        self.close_price_gap_threshold = close_price_gap_threshold
        self.logger = create_transaction_logger(str(self.id))

        self._db_transaction_status_updater = (
            lambda status: singleton.db.async_update_transaction(
                transaction_id=self.id, status=status))

    def open_position(self, leg, timeout_in_sec):
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor(
            leg.instrument_id,
            leg.volume,
            leg.price,
            timeout_in_sec,
            is_market_order=False,
            logger=self.logger,
            transaction_id=self.id)
        if leg.side == LONG:
            return order_executor.open_long_position()
        else:
            return order_executor.open_short_position()

    def close_position(self, leg):
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor(
            leg.instrument_id,
            leg.volume,
            price=-1,
            timeout_sec=None,
            is_market_order=True,
            logger=self.logger,
            transaction_id=self.id)
        if leg.side == LONG:
            return order_executor.close_long_order()
        else:
            return order_executor.close_short_order()

    def process(self):
        self._db_transaction_status_updater('started')
        self.logger.info('=== arbitrage transaction started ===')
        self.logger.info(f'id: {self.id}')
        self.logger.info(f'slow leg: {self.slow_leg}')
        self.logger.info(f'fast leg: {self.fast_leg}')
        self._process()
        self.logger.info('=== arbitrage transaction ended ===')

    def _process(self):
        self._db_transaction_status_updater('opening_slow_leg')
        slow_leg_order_status = self.open_position(
            self.slow_leg, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        ).get()

        if slow_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            self.logger.info(
                f'[SLOW FAILED] failed to open slow leg {self.slow_leg} '
                f'({slow_leg_order_status})')
            self._db_transaction_status_updater('ended_slow_leg_failed')
            return
        self.logger.info(f'[SLOW FULFILLED] {self.slow_leg} was fulfilled, '
                         f'will open position for fast leg')

        self._db_transaction_status_updater('opening_fast_leg')
        fast_leg_order_status = self.open_position(
            self.fast_leg, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        ).get()

        if fast_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            self.logger.info(f'[FAST FAILED] failed to open fast leg '
                             f'{self.fast_leg} '
                             f'({fast_leg_order_status}), '
                             'will close slow leg position before aborting the '
                             'rest of this transaction')
            self._db_transaction_status_updater('ended_fast_leg_failed')
            self.close_position(self.slow_leg).get()
            self.logger.info(
                f'slow leg position {self.slow_leg} has been closed')
            return

        self.logger.info(f'[BOTH FULFILLED] fast leg {self.fast_leg} order '
                         f'fulfilled, will wait '
                         f'for converge for {PRICE_CONVERGE_TIMEOUT_IN_SECOND} '
                         f'seconds')
        self._db_transaction_status_updater('waiting_converge')

        with WaitingPriceConverge(self) as converge_future:
            converge = converge_future.get(PRICE_CONVERGE_TIMEOUT_IN_SECOND)
            if converge is None:
                # timeout, close the position
                self.logger.info('[CONVERGE TIMEOUT] prices failed to converge '
                                 'in time, closing both legs')
            else:
                self.logger.info(f'[CONVERGED] prices converged with enough '
                                 f'margin({converge}), closing both legs')
            fast_order = self.close_position(self.fast_leg)
            slow_order = self.close_position(self.slow_leg)
            fast_order.get()
            slow_order.get()
            self._db_transaction_status_updater('ended_normally')


def _testing(_):
    def _test_aribitrage():
        singleton.websocket.ready.get()
        logging.info('WebSocket subscription finished')
        week_instrument = singleton.schema.all_instrument_ids[0]
        quarter_instrument = singleton.schema.all_instrument_ids[-1]
        transaction = ArbitrageTransaction(
            slow_leg=ArbitrageLeg(instrument_id=quarter_instrument,
                                  side=SHORT,
                                  volume=1,
                                  price=120.0),
            fast_leg=ArbitrageLeg(instrument_id=week_instrument,
                                  side=LONG,
                                  volume=1,
                                  price=80.0),
            close_price_gap_threshold=1,
        )
        transaction.process()

    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    singleton.websocket.start_read_loop()
    singleton.green_pool.spawn_n(_test_aribitrage)
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
