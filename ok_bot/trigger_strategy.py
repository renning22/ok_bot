import logging
from abc import ABC, abstractmethod
from collections import namedtuple

import numpy as np

from . import constants, logger, singleton, util
from .arbitrage_execution import LONG, SHORT

ArbitragePlan = namedtuple('ArbitragePlan',
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


def close_aribitrage_gap_threshold(long_instrument_id,
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


def spot_profit(long_begin, long_end, short_begin, short_end):
    """
    Assume bug at price long_begin and close long at long_end. Hedge by short
    at short_begin and close short at short_end. What will be the profit after
    fee.
    :param long_begin: price to open the long order
    :param long_end: price to close the long order
    :param short_begin: price to open the short order
    :param short_end: price to close the short order
    :return: profit in coin currency after transaction fee
    """
    usd = constants.TRADING_VOLUME * \
        constants.SINGLE_UNIT_IN_USD[singleton.coin_currency]
    fee = (usd / long_begin + usd / long_end + usd / short_begin +
           usd / short_end) * constants.FEE_RATE
    gain = usd / long_begin - usd / long_end + \
        usd / short_end - usd / short_begin
    return gain - fee


def estimate_profit(prices, gap_threshold):
    """
    Estimate the profit assuming open arbitrage at prices and the price gap
    closed to gap_threshold
    :param prices: dict. For instance, {LONG: 100, SHORT: 120}
    :param gap_threshold: The eventual price gap when converged
    :return: the estimated profit in coin currency after fee
    """
    # Note low <= high is not necessary
    low, high = prices[LONG], prices[SHORT]

    ret = 1e10  # Very big number
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


def make_arbitrage_plan(self,
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

    est_profit = estimate_profit(
        {
            slow_side: slow_price,
            fast_side: fast_price,
        },
        close_price_gap
    )

    if est_profit < constants.MIN_ESTIMATE_PROFIT:
        logging.info(f'Profit estimat is {est_profit}, not enough')
        return None

    return ArbitragePlan(
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


class TriggerStrategy(ABC):
    @abstractmethod
    def trigger(self,
                long_instrument,
                short_instrument,
                product):
        """Returns ArbitragePlan or None"""
        raise NotImplemented


class PercentageTriggerStrategy(TriggerStrategy):
    def trigger(self,
                long_instrument,
                short_instrument,
                product):

        history_gap = self.order_book.historical_mean_spread(product)
        close_price_gap = history_gap + close_aribitrage_gap_threshold(
            long_instrument, short_instrument) * abs(history_gap)
        current_spread = self.order_book.current_spread(product)
        profit_est = estimate_profit({
            LONG: self.market_depth[long_instrument][0][0][0],  # Best ask
            SHORT: self.market_depth[short_instrument][1][0][0],  # Best bid
        }, close_price_gap)
        if profit_est < constants.MIN_ESTIMATE_PROFIT:
            logging.log_every_n_seconds(
                logging.INFO,
                '[gap small] %s gap: %.3f history_gap: %.3f profit est: %f',
                60,
                product, current_spread, history_gap, profit_est
            )
            return None

        available_amount = util.amount_margin(
            ask_stack=singleton.order_book.market_depth[long_instrument][0],
            bid_stack=singleton.order_book.market_depth[short_instrument][1],
            condition=lambda ask_price,
            bid_price: bid_price - ask_price >= current_spread)

        logging.log_every_n_seconds(
            logging.INFO,
            '[gap enough] %s spread: %.3f history_gap: %.3f available: %d',
            60,
            product, current_spread, history_gap, available_amount
        )

        if available_amount < \
                constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
            return None
        else:
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
                return self.make_arbitrage_plan(
                    slow_instrument_id=short_instrument,
                    fast_instrument_id=long_instrument,
                    slow_side=SHORT,
                    fast_side=LONG,
                    open_price_gap=current_spread,
                    close_price_gap=close_price_gap
                )
            else:
                return self.make_arbitrage_plan(
                    slow_instrument_id=long_instrument,
                    fast_instrument_id=short_instrument,
                    slow_side=LONG,
                    fast_side=SHORT,
                    open_price_gap=current_spread,
                    close_price_gap=close_price_gap
                )


if __name__ == '__main__':
    logger.init_global_logger(log_level=logging.INFO)
