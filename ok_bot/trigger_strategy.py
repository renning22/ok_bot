import logging
from abc import ABC, abstractmethod
from collections import namedtuple

import numpy as np

from . import constants, logger, singleton
from .arbitrage_execution import LONG, SHORT

TriggerInfo = namedtuple('TriggerInfo',
                         [
                             'volume',
                             'slow_instrument_id',
                             'fast_instrument_id',
                             'slow_side',
                             'fast_side',
                             'slow_price',
                             'fast_price',
                             'close_price_gap',
                             'estimate_net_profit',
                         ])


class TriggerStrategy(ABC):
    @abstractmethod
    def trigger(self,
                long_instrument,
                short_instrument,
                product):
        """Returns TriggerInfo or None"""
        raise NotImplemented


class PercentageTriggerStrategy(TriggerStrategy):
    def trigger(self,
                long_instrument,
                short_instrument,
                product):
        history_gap = singleton.order_book.historical_mean_spread(product)
        current_spread = singleton.order_book.current_spread(product)
        deviation = current_spread - history_gap
        deviation_percent = deviation / abs(history_gap) * 100
        threshold = self.aribitrage_gap_threshold(long_instrument,
                                                  short_instrument)

        min_price_gap = history_gap + threshold * abs(history_gap)
        available_amount = amount_margin(
            ask_stack=singleton.order_book.market_depth[long_instrument][0],
            bid_stack=singleton.order_book.market_depth[short_instrument][1],
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
                return self.calculate_prices(
                    slow_instrument_id=short_instrument,
                    fast_instrument_id=long_instrument,
                    slow_side=SHORT,
                    fast_side=LONG,
                    open_price_gap=min_price_gap,
                    close_price_gap=close_price_gap
                )
            else:
                return self.calculate_prices(
                    slow_instrument_id=long_instrument,
                    fast_instrument_id=short_instrument,
                    slow_side=LONG,
                    fast_side=SHORT,
                    open_price_gap=min_price_gap,
                    close_price_gap=close_price_gap
                )
        else:
            return None

    def calculate_prices(self,
                         slow_instrument_id,
                         fast_instrument_id,
                         slow_side,
                         fast_side,
                         open_price_gap,
                         close_price_gap):
        if slow_side == LONG:
            amount = 0
            slow_price = (
                singleton.order_book.market_depth[slow_instrument_id][0][0][0])
            for price, vol in singleton.order_book.market_depth[slow_instrument_id][0]:
                amount += vol
                if amount >= \
                        constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = price
                    break
            fast_price = slow_price + open_price_gap
        else:
            assert slow_side == SHORT
            amount = 0
            slow_price = (
                singleton.order_book.market_depth[slow_instrument_id][1][0][0])
            for price, vol in singleton.order_book.market_depth[slow_instrument_id][1]:
                amount += vol
                if amount >= \
                        constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                    slow_price = price
                    break
            fast_price = slow_price - open_price_gap

        est_profit = self.estimate_profit(
            {
                slow_side: slow_price,
                fast_side: fast_price,
            },
            close_price_gap
        )

        if est_profit < constants.MIN_ESTIMATE_PROFIT:
            logging.info(f'Profit estimat is {est_profit}, not enough')
            return None

        return TriggerInfo(
            volume=constants.TRADING_VOLUME,
            slow_instrument_id=slow_instrument_id,
            fast_instrument_id=fast_instrument_id,
            slow_side=slow_side,
            fast_side=fast_side,
            slow_price=slow_price,
            fast_price=fast_price,
            close_price_gap=close_price_gap,
            estimate_net_profit=est_profit
        )

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

    def close_aribitrage_gap_threshold(self,
                                       long_instrument_id,
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

    def spot_profit(self, long_begin, long_end, short_begin, short_end):
        usd = constants.TRADING_VOLUME * \
            constants.SINGLE_UNIT_IN_USD[singleton.coin_currency]
        fee = (usd / long_begin + usd / long_end + usd / short_begin +
               usd / short_end) * constants.FEE_RATE
        gain = usd / long_begin - usd / long_end + \
            usd / short_end - usd / short_begin
        return gain - fee

    def estimate_profit(self, prices, gap_threshold):
        # Note low <= high is not necessary
        low, high = prices[LONG], prices[SHORT]

        ret = 1e10
        for _ in range(10):
            # case 1: low side rise to high - gap_threshold
            high_end = np.random.normal(high, high * 0.01)
            low_end = high_end - gap_threshold
            est1 = self.spot_profit(low, low_end, high, high_end)
            # case 2: high side drop to low + gap_threshold
            low_end = np.random.normal(low, low * 0.01)
            high_end = low_end + gap_threshold
            est2 = self.spot_profit(low, low_end, high, high_end)
            ret = min(ret, est1, est2)
        return ret


if __name__ == '__main__':
    logger.init_global_logger(log_level=logging.INFO)
