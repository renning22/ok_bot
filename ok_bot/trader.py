import datetime
import re

import numpy as np
from absl import logging

from . import constants
from . import singleton
from .util import amount_margin
from .arbitrage_execution import ArbitrageLeg, ArbitrageTransaction, SHORT, LONG


def extract_date(instrument_id):
    date_str = re.match(r'[A-Z]+-USD-([0-9]+)', instrument_id).group(1)
    return datetime.datetime.strptime(date_str, '%y%m%d')


def instrument_period(instrument_date):
    now = datetime.datetime.now()
    delta = instrument_date - now
    if delta.days <= 7:
        return 'this_week'
    if delta.days <= 14:
        return 'next_week'
    return 'quarter'


class Trader:
    def __init__(self,
                 min_time_window=np.timedelta64(
                     constants.MIN_TIME_WINDOW_IN_SECOND - 1, 's')):
        self.min_time_window = min_time_window
        self._schema = singleton.schema
        self.max_volume_per_trading = 1  # always use smallest possible amount
        self.order_book = None
        self.new_tick_received = self.new_tick_received__ramp_up_mode
        self.market_depth = {}
        self.waiting_for_execution = False  # used for debugging

    def aribitrage_gap_threshold(self, long_instrument_id, short_instrument_id):
        long_instrument_period = instrument_period(
            extract_date(long_instrument_id))
        short_instrument_period = instrument_period(
            extract_date(short_instrument_id))
        if (long_instrument_period,
           short_instrument_period) in constants.OPEN_THRESHOLDS:
            return constants.OPEN_THRESHOLDS[
                long_instrument_period, short_instrument_period]
        assert short_instrument_period,long_instrument_period in constants.OPEN_THRESHOLDS
        return constants.OPEN_THRESHOLDS[
            short_instrument_period, long_instrument_period]

    def close_aribitrage_gap_threshold(self, long_instrument_id,
                                       short_instrument_id):
        long_instrument_period = instrument_period(
            extract_date(long_instrument_id))
        short_instrument_period = instrument_period(
            extract_date(short_instrument_id))
        if (long_instrument_period,
           short_instrument_period) in constants.CLOSE_THRESHOLDS:
            return constants.CLOSE_THRESHOLDS[
                long_instrument_period, short_instrument_period]
        assert short_instrument_period, long_instrument_period in constants.CLOSE_THRESHOLDS
        return constants.CLOSE_THRESHOLDS[
            short_instrument_period, long_instrument_period]

    def new_tick_received__ramp_up_mode(self, instrument_id, ask_prices,
                                        ask_vols, bid_prices, bid_vols):
        # This can't be in __init__ due to circular dependency with OrderBook
        self.order_book = singleton.order_book

        self.market_depth[instrument_id] = [list(zip(ask_prices, ask_vols)),
                                            list(zip(bid_prices, bid_vols))]

        if self.order_book.time_window >= self.min_time_window:
            self.new_tick_received = self.new_tick_received__regular
            return

        logging.log_every_n_seconds(
            logging.INFO, 'ramping up: %d/%s', 1,
            self.order_book.time_window / np.timedelta64(1, 's'),
            self.min_time_window)

    def new_tick_received__regular(self, instrument_id, ask_prices, ask_vols,
                                   bid_prices, bid_vols):
        self.market_depth[instrument_id] = [list(zip(ask_prices, ask_vols)),
                                            list(zip(bid_prices, bid_vols))]
        for long_instrument, short_instrument, product in self._schema.markets_cartesian_product:
            if instrument_id in [long_instrument, short_instrument]:
                self._process_pair(long_instrument, short_instrument, product)

    def _process_pair(self, long_instrument, short_instrument, product):
        """
        Check if we should long `long_instrument` and short
        `short_instrument`. It's positive when long_instrument has big price
        drop relevant to short_instrument. It could be long_instrument is
        abruptly cheaper or short_instrument is suddenly expensive. When this
        happened, price(short_instrument) - price(long_instrument) will have
        a suddenly increase. Therefore the current value of that gap minus
        the gap average will break the threshold, which is the arbitrage
        triggering condition.
        """
        history_gap = self.order_book.historical_mean_spread(product)
        current_spread = self.order_book.current_spread(product)
        deviation = current_spread - history_gap
        deviation_percent = deviation / abs(history_gap) * 100
        threshold = self.aribitrage_gap_threshold(long_instrument,
                                                  short_instrument)

        min_price_gap = history_gap + threshold * abs(history_gap)
        available_acount = amount_margin(
            ask_stack=self.market_depth[long_instrument][0],
            bid_stack=self.market_depth[short_instrument][1],
            condition=lambda ask_price,
                             bid_price: bid_price - ask_price >= min_price_gap)
        logging.log_every_n_seconds(
            logging.INFO,
            '%s spread: %.3f '
            'history_gap: %.3f\n deviation: %.4f(%.2f)%% '
            'threshold: %.2f available: %d',
            60,
            product, current_spread, history_gap, deviation, deviation_percent,
            threshold * 100, available_acount
        )

        if available_acount >= constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
            # trigger arbitrage
            close_price_gap = \
                history_gap + self.close_aribitrage_gap_threshold(
                    long_instrument, short_instrument) \
                * abs(history_gap)
            long_instrument_speed = self.order_book.price_speed(long_instrument,
                                                                'ask')
            short_instrument_speed = self.order_book.price_speed(
                short_instrument, 'bid')
            logging.info(f'Long instrument speed: {long_instrument_speed:.3f}, '
                         f'short instrument speed: {short_instrument_speed:.3f}')
            if long_instrument_speed > short_instrument_speed:
                self.trigger_arbitrage(
                    slow_instrument_id=short_instrument,
                    fast_instrument_id=long_instrument,
                    slow_side=SHORT,
                    fast_side=LONG,
                    open_price_gap=min_price_gap,
                    close_price_gap=close_price_gap
                )
            else:
                self.trigger_arbitrage(
                    slow_instrument_id=long_instrument,
                    fast_instrument_id=short_instrument,
                    slow_side=LONG,
                    fast_side=SHORT,
                    open_price_gap=min_price_gap,
                    close_price_gap=close_price_gap
                )

    def trigger_arbitrage(self, slow_instrument_id, fast_instrument_id,
                          slow_side, fast_side,
                          open_price_gap, close_price_gap):
        if slow_side == LONG:
            amount = 0
            slow_price = float(self.market_depth[slow_instrument_id][0][0][0])
            for price, vol in self.market_depth[slow_instrument_id][0]:
                amount += vol
                if amount >= constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = float(price)
                    break
            fast_price = slow_price + open_price_gap
        else:
            assert slow_side == SHORT
            amount = 0
            slow_price = float(self.market_depth[slow_instrument_id][1][0][0])
            for price, vol in self.market_depth[slow_instrument_id][1]:
                amount += vol
                if amount >= constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = float(price)
                    break
            fast_price = slow_price - open_price_gap

        timestamp = datetime.datetime.now().strftime('%m%d-%H:%M:%S')
        transaction = ArbitrageTransaction(
            arbitrage_id=f'{timestamp}_{slow_instrument_id}({slow_side})'
                         f'{fast_instrument_id}({fast_side})',
            slow_leg=ArbitrageLeg(
                instrument_id=slow_instrument_id,
                side=slow_side,
                volume=1,
                price=slow_price
            ),
            fast_leg=ArbitrageLeg(
                instrument_id=fast_instrument_id,
                side=fast_side,
                volume=1,
                price=fast_price
            ),
            close_price_gap_threshold=close_price_gap
        )
        singleton.green_pool.spawn_n(transaction.process)


if __name__ == '__main__':
    def _mock_trigger_arbitrage(trans_id, slow_leg, fast_leg, close_threshold):
        logging.info(
            f'trigger arbitrage({trans_id}) on {slow_leg} and {fast_leg},'
            f' close threshold: {close_threshold}')


    def main(_):
        singleton.initialize_objects('ETH')
        singleton.trader.trigger_arbitrage = _mock_trigger_arbitrage
        singleton.start_loop()


    from absl import app

    app.run(main)
