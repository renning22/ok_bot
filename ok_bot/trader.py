import asyncio
import logging
import random

import numpy as np

from . import constants, logger, singleton
from .arbitrage_execution import (LONG, SHORT, ArbitrageLeg,
                                  ArbitrageTransaction)
from .util import amount_margin


def spot_profit(long_begin, long_end, short_begin, short_end):
    usd = constants.TRADING_VOLUME * \
        constants.SINGLE_UNIT_IN_USD[singleton.coin_currency]
    fee = (usd / long_begin + usd / long_end + usd / short_begin
           + usd / short_end) * constants.FEE_RATE
    gain = usd / long_begin - usd / long_end + \
        usd / short_end - usd / short_begin
    return gain - fee


def estimate_profit(prices, gap_threshold):
    # Note low <= high is not necessary
    low, high = prices[LONG], prices[SHORT]

    ret = 1e10
    for _ in range(10):
        # case 1: low side rise to high - gap_threshold
        high_end = np.random.normal(high, high * 0.01)
        low_end = high_end - gap_threshold
        est1 = spot_profit(low, low_end, high, high_end)
        # case 2: high side drop to low + gap_threshold
        low_end = np.random.normal(low, low * 0.01)
        high_end = low_end + gap_threshold
        est2 = spot_profit(low, low_end, high, high_end)
        ret = min(ret, est1, est2)
    return ret


class Trader:
    def __init__(self):
        self.min_time_window = np.timedelta64(
            constants.MIN_TIME_WINDOW_IN_SECOND, 's')
        self.max_volume_per_trading = 1  # always use smallest possible amount
        self.new_tick_received = self.new_tick_received__ramp_up_mode
        self.market_depth = {}
        self.is_in_cooldown = False
        self.on_going_arbitrage_count = 0

    def cool_down(self):
        async def stop_cool_down():
            await asyncio.sleep(constants.INSUFFICIENT_MARGIN_COOL_DOWN_SECOND)
            self.is_in_cooldown = False
        self.is_in_cooldown = True
        asyncio.create_task(stop_cool_down())

    def aribitrage_gap_threshold(self, long_instrument_id, short_instrument_id):
        long_instrument_period = singleton.schema.instrument_period(
            long_instrument_id)
        short_instrument_period = singleton.schema.instrument_period(
            short_instrument_id)
        if (long_instrument_period, short_instrument_period) in \
                constants.OPEN_THRESHOLDS:
            return constants.OPEN_THRESHOLDS[
                long_instrument_period, short_instrument_period]
        assert short_instrument_period, long_instrument_period in \
            constants.OPEN_THRESHOLDS
        return constants.OPEN_THRESHOLDS[
            short_instrument_period, long_instrument_period]

    def close_aribitrage_gap_threshold(self, long_instrument_id,
                                       short_instrument_id):
        long_instrument_period = singleton.schema.instrument_period(
            long_instrument_id)
        short_instrument_period = singleton.schema.instrument_period(
            short_instrument_id)
        if (long_instrument_period, short_instrument_period) in \
                constants.CLOSE_THRESHOLDS:
            return constants.CLOSE_THRESHOLDS[
                long_instrument_period, short_instrument_period]
        assert short_instrument_period, long_instrument_period in \
            constants.CLOSE_THRESHOLDS
        return constants.CLOSE_THRESHOLDS[
            short_instrument_period, long_instrument_period]

    def new_tick_received__ramp_up_mode(self, instrument_id, ask_prices,
                                        ask_vols, bid_prices, bid_vols):
        self.market_depth[instrument_id] = [list(zip(ask_prices, ask_vols)),
                                            list(zip(bid_prices, bid_vols))]

        if singleton.order_book.time_window >= self.min_time_window:
            self.new_tick_received = self.new_tick_received__regular
            return

        logging.log_every_n_seconds(
            logging.INFO, 'ramping up: %d/%s', 1,
            singleton.order_book.time_window / np.timedelta64(1, 's'),
            self.min_time_window)

    def new_tick_received__regular(self, instrument_id, ask_prices, ask_vols,
                                   bid_prices, bid_vols):
        self.market_depth[instrument_id] = [list(zip(ask_prices, ask_vols)),
                                            list(zip(bid_prices, bid_vols))]
        for long_instrument, short_instrument, product in \
                singleton.schema.markets_cartesian_product:
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
        if self.on_going_arbitrage_count > 0:
            logging.log_every_n_seconds(
                logging.CRITICAL,
                'skip process_pair because there are %s on going arbitrages',
                30,
                self.on_going_arbitrage_count
            )
            return

        history_gap = singleton.order_book.historical_mean_spread(product)
        current_spread = singleton.order_book.current_spread(product)
        deviation = current_spread - history_gap
        deviation_percent = deviation / abs(history_gap) * 100
        threshold = self.aribitrage_gap_threshold(long_instrument,
                                                  short_instrument)

        min_price_gap = history_gap + threshold * abs(history_gap)
        available_amount = amount_margin(
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
            threshold * 100, available_amount
        )

        if available_amount >= \
                constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
            # trigger arbitrage
            close_price_gap = (
                history_gap + self.close_aribitrage_gap_threshold(
                    long_instrument, short_instrument) *
                abs(history_gap))
            long_instrument_speed = singleton.order_book.price_speed(
                long_instrument,
                'ask')
            short_instrument_speed = singleton.order_book.price_speed(
                short_instrument, 'bid')
            logging.info(
                f'Long instrument speed: {long_instrument_speed:.3f}, '
                f'short instrument speed: '
                f'{short_instrument_speed:.3f}'
            )
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

    def trigger_arbitrage(self,
                          slow_instrument_id,
                          fast_instrument_id,
                          slow_side,
                          fast_side,
                          open_price_gap,
                          close_price_gap):
        if slow_side == LONG:
            amount = 0
            slow_price = self.market_depth[slow_instrument_id][0][0][0]
            for price, vol in self.market_depth[slow_instrument_id][0]:
                amount += vol
                if amount >= \
                        constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = price
                    break
            fast_price = slow_price + open_price_gap
        else:
            assert slow_side == SHORT
            amount = 0
            slow_price = self.market_depth[slow_instrument_id][1][0][0]
            for price, vol in self.market_depth[slow_instrument_id][1]:
                amount += vol
                if amount >= \
                        constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = price
                    break
            fast_price = slow_price - open_price_gap

        if self.is_in_cooldown:
            logging.log_every_n_seconds(
                logging.WARNING,
                f'[COOL DOWN] Skipping arbitrage between '
                f'{slow_instrument_id}({slow_side}) and '
                f'{fast_instrument_id}({fast_side}) due to '
                f'cool down',
                30
            )
            return

        est_profit = estimate_profit(
            {
                slow_side: slow_price,
                fast_side: fast_price,
            },
            close_price_gap
        )

        if est_profit < constants.MIN_ESTIMATE_PROFIT:
            logging.info(f'Profit estimat is {est_profit}, not enough')
            return

        transaction = ArbitrageTransaction(
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
            close_price_gap_threshold=close_price_gap,
            estimate_net_profit=est_profit
        )
        # Run transaction asynchronously. Main tick_received loop doesn't have
        # to await on it.
        self.on_going_arbitrage_count += 1
        asyncio.create_task(transaction.process())


if __name__ == '__main__':
    def _mock_trigger_arbitrage(
            slow_instrument_id, fast_instrument_id,
            slow_side, fast_side,
            open_price_gap, close_price_gap):
        logging.info(
            f'trigger arbitrage({slow_instrument_id})'
            f' and ({fast_instrument_id})')

    logger.init_global_logger(log_level=logging.INFO)
    constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE = -1  # Ensure trigger
    singleton.initialize_objects('ETH')
    singleton.trader.min_time_window = np.timedelta64(3, 's')
    singleton.trader.trigger_arbitrage = _mock_trigger_arbitrage
    logging.info('Manual test started')
    singleton.start_loop()
