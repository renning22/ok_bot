import collections

import eventlet
from absl import app, logging

from . import singleton
from .order_executor import OrderExecutor, OPEN_POSITION_STATUS__SUCCEEDED
from .util import amount_margin
from .constants import LONG, SHORT, \
    SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, \
    MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE, PRICE_CONVERGE_TIMEOUT_IN_SECOND

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

    def __enter__(self):
        singleton.book_listener.subscribe(
            self._transaction.slow_leg.instrument_id, self)
        singleton.book_listener.subscribe(
            self._transaction.fast_leg.instrument_id, self)
        self._future = eventlet.event.Event()
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
            try:
                self._future.send(amount_margin)
            except AssertionError as err:
                logging.warning(f'Sending future failed: {err}')

    def _should_close_arbitrage(self):
        if self._ask_stack is None or self._bid_stack is None:
            return False, -1

        available_amount = amount_margin(
            self._ask_stack,
            self._bid_stack,
            lambda ask_price,
                   bid_price:
            ask_price - bid_price <= self._transaction.close_price_gap_threshold)

        logging.log_every_n_seconds(
            logging.INFO,
            '%s:%s current_gap:%.3f, max_gap: %.3f, available_amount: %d',
            20,
            self._ask_stack_instrument,
            self._bid_stack_instrument,
            self._ask_stack[0][0] - self._bid_stack[0][0],
            self._transaction.close_price_gap_threshold,
            available_amount
        )
        return available_amount >= MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE, \
               available_amount


class ArbitrageTransaction:
    def __init__(self,
                 arbitrage_id,
                 slow_leg,
                 fast_leg,
                 close_price_gap_threshold):
        self.id = arbitrage_id
        self.slow_leg = slow_leg
        self.fast_leg = fast_leg
        self.close_price_gap_threshold = close_price_gap_threshold

    def open_position(self, leg, timeout_in_sec):
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor()
        if leg.side == LONG:
            return order_executor.open_long_position(leg.instrument_id,
                                                     leg.volume,
                                                     leg.price,
                                                     timeout_in_sec)
        else:
            # short order
            return order_executor.open_short_position(leg.instrument_id,
                                                      leg.volume,
                                                      leg.price,
                                                      timeout_in_sec)

    def close_position(self, leg):
        if leg.side == LONG:
            singleton.rest_api.close_long_order(leg.instrument_id,
                                                amount=1,
                                                price=-1,
                                                is_market_order=True)
        else:
            singleton.rest_api.close_short_order(leg.instrument_id,
                                                 amount=1,
                                                 price=-1,
                                                 is_market_order=True)

    # def wait_for_order_execution(self, order_exe_future):
    #     order_exe_future.wait()

    def process(self):
        logging.info('starting arbitrage transaction on '
                     f'slow:{self.slow_leg} and fast{self.fast_leg}')


        slow_leg_order_status = self.open_position(
            self.slow_leg, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        ).wait()

        if slow_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            logging.info(f'slow leg {self.slow_leg} is not '
                         f'successful({slow_leg_order_status}), '
                         'will abort the rest of this transaction')
            return
        logging.info(f'{self.slow_leg} is successful, '
                     f'will open position for fast leg')

        fast_leg_order_status = self.open_position(
            self.fast_leg, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND
        ).wait()

        if fast_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:
            logging.info(f'fast leg {self.slow_leg} is not '
                         f'successful({fast_leg_order_status}), '
                         'will close slow leg position before aborting the rest'
                         ' of this transaction')
            self.close_position(self.slow_leg)
            return

        logging.info(f'fast leg {self.fast_leg} order fulfilled, will wait '
                     f'for converge for'
                     f' {PRICE_CONVERGE_TIMEOUT_IN_SECOND} seconds')

        with WaitingPriceConverge(self) as converge_future:
            converge = converge_future.wait(PRICE_CONVERGE_TIMEOUT_IN_SECOND)
            if converge is None:
                # timeout, close the position
                logging.info('Prices failed to converge in time, closing '
                             'both legs')
            else:
                logging.info(f'Prices converged with enough margin({converge}),'
                             f' closing both legs')
            self.close_position(self.fast_leg)
            self.close_position(self.slow_leg)


def _testing(_):
    def _test_aribitrage():
        sleep = 1
        while not singleton.websocket.ready:
            logging.info(f'sleeping for {sleep} seconds, wait for WS'
                         f' subscription to finish')
            eventlet.sleep(sleep)
        logging.info('WebSocket subscription finished')
        week_instrument = singleton.schema.all_instrument_ids[0]
        quarter_instrument = singleton.schema.all_instrument_ids[-1]
        transaction = ArbitrageTransaction(
            'test-transaction',
            slow_leg=ArbitrageLeg(instrument_id=quarter_instrument,
                                  side=LONG,
                                  volume=1,
                                  price=110.6),
            fast_leg=ArbitrageLeg(instrument_id=week_instrument,
                                  side=SHORT,
                                  volume=1,
                                  price=114.0),
            close_price_gap_threshold=1,
        )
        transaction.process()

    singleton.initialize_objects('ETH')
    singleton.websocket.start_read_loop()
    singleton.green_pool.spawn_n(_test_aribitrage)
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
