import logging
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Callable, List

import numpy as np

from . import constants, logger, singleton
from .constants import LONG, SHORT
from .order_book import AvailableOrder

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
                               'z_score',
                           ])


def calculate_amount_margin(ask_stack: List[AvailableOrder],
                            bid_stack: List[AvailableOrder],
                            condition: Callable) -> int:
    available_amount = 0
    bid_copy = [AvailableOrder(price=order.price, volume=order.volume)
                for order in bid_stack]
    for ask_order in ask_stack:
        ask_price, ask_volume = ask_order.price, ask_order.volume
        for i, bid_order in enumerate(bid_copy):
            bid_price, bid_volume = bid_order.price, bid_order.volume
            if not condition(ask_price,
                             bid_price) or ask_volume <= 0 or bid_volume <= 0:
                continue
            amount = min(ask_volume, bid_volume)
            ask_volume -= amount
            bid_copy[i] = AvailableOrder(price=bid_copy[i].price,
                                         volume=bid_copy[i].volume - amount)
            available_amount += amount
    return available_amount


def close_arbitrage_gap_threshold(long_instrument_id,
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
    fee = (usd / long_begin + usd / long_end + usd / short_begin
           + usd / short_end) * constants.FEE_RATE
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


def make_arbitrage_plan(slow_instrument_id,
                        fast_instrument_id,
                        slow_side,
                        fast_side,
                        open_price_gap,
                        close_price_gap,
                        est_profit,
                        z_score) -> ArbitragePlan:
    if slow_side == LONG:
        amount = 0
        slow_price = (
            singleton.order_book.market_depth(
                slow_instrument_id).best_ask_price)
        for available_order in singleton.order_book.market_depth(
                slow_instrument_id).ask():
            price = available_order.price
            vol = available_order.volume
            amount += vol
            if amount >= constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                slow_price = price
                break
        fast_price = slow_price + open_price_gap
    else:
        assert slow_side == SHORT
        amount = 0
        slow_price = (
            singleton.order_book.market_depth(
                slow_instrument_id).best_bid_price)
        for available_order in singleton.order_book.market_depth(
                slow_instrument_id).bid():
            price = available_order.price
            vol = available_order.volume
            amount += vol
            if amount >= constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
                slow_price = price
                break
        fast_price = slow_price - open_price_gap

    return ArbitragePlan(
        volume=constants.TRADING_VOLUME,
        slow_instrument_id=slow_instrument_id,
        fast_instrument_id=fast_instrument_id,
        slow_side=slow_side,
        fast_side=fast_side,
        slow_price=slow_price,
        fast_price=fast_price,
        close_price_gap=close_price_gap,
        estimate_net_profit=est_profit,
        z_score=z_score
    )


class TriggerStrategy(ABC):
    @abstractmethod
    def is_there_a_plan(self,
                        long_instrument,
                        short_instrument,
                        product) -> ArbitragePlan:
        """Returns ArbitragePlan or None"""
        raise NotImplemented


class PercentageTriggerStrategy(TriggerStrategy):
    def is_there_a_plan(self,
                        long_instrument,
                        short_instrument,
                        product) -> ArbitragePlan:
        z_score = singleton.order_book.zscore(product)
        history_gap = singleton.order_book.historical_mean_spread(product)
        current_spread = singleton.order_book.current_spread(product)
        deviation = current_spread - history_gap
        close_price_gap = (
                history_gap + deviation * constants.SIMPLE_STRATEGY_RESILIANCE)
        profit_est = estimate_profit({
            # Best ask
            LONG: singleton.order_book.market_depth(
                long_instrument).best_ask_price(),
            # Best bid
            SHORT: singleton.order_book.market_depth(
                short_instrument).best_bid_price(),
        }, close_price_gap)
        if z_score < constants.SIMPLE_STRATEGY_ZSCORE_THRESHOLD or\
                profit_est < constants.MIN_ESTIMATE_PROFIT:
            logging.log_every_n_seconds(
                logging.CRITICAL,
                '[heartbeat] %s z-score: %.2f profit est: %f',
                60 * 30,  # Every 30 min
                product, z_score, profit_est
            )
            return None

        available_amount = calculate_amount_margin(
            ask_stack=singleton.order_book.market_depth(long_instrument).ask(),
            bid_stack=singleton.order_book.market_depth(
                short_instrument).bid(),
            condition=lambda ask_price,
            bid_price: bid_price - ask_price >= current_spread)

        if available_amount < \
                constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE:
            logging.info('[amount margin too small] skip')
            return None

        logging.critical('[Detected] z-score: %.2f, profit est: %f, amount: %d',
                         z_score, profit_est, available_amount)

        # Built arbitrage plan
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
            return make_arbitrage_plan(
                slow_instrument_id=short_instrument,
                fast_instrument_id=long_instrument,
                slow_side=SHORT,
                fast_side=LONG,
                open_price_gap=current_spread,
                close_price_gap=close_price_gap,
                est_profit=profit_est,
                z_score=z_score,
            )
        else:
            return make_arbitrage_plan(
                slow_instrument_id=long_instrument,
                fast_instrument_id=short_instrument,
                slow_side=LONG,
                fast_side=SHORT,
                open_price_gap=current_spread,
                close_price_gap=close_price_gap,
                est_profit=profit_est,
                z_score=z_score,
            )


class SimpleTriggerStrategy(TriggerStrategy):
    """Simple short-term mean-reversion strategy.

    The deviation is the larger the better (the more profitable).

      deviation = (bid - ask) - (avg(bid) - avg(ask))
                = (short_price - long_price) - ...

    Imagine deviation becomes deviation - 1.
    It could be either (or other cases equivalent) from:
      1) short_price -> short_price - 1
      2) long_price -> long_price + 1

    For either case we would profit 1 dollar (assume we opened
    1-coin-value-equivalent contracts (after leverage) at on both side at
    price short_price/long_price.

    Assume the resilience drives deviation back to the half of the delta.

      deviation --> deviation / 2

    And analogously, the total price differences would be:
      total_price_diff = deviation / 2 - deviation

    Can it be an estimate of profit?

    For fee we can approximate it by 4 times of current average price:
      total_fee = 4 * 0.030% * (short_price + long_price) / 2
    """

    def is_there_a_plan(self,
                        long_instrument,
                        short_instrument,
                        product) -> ArbitragePlan:
        # zscore is a normalized measure of how large the last sample is
        # deviated from center amongst population.
        zscore = singleton.order_book.zscore(product)

        history_gap = singleton.order_book.historical_mean_spread(product)
        current_spread = singleton.order_book.current_spread(product)

        deviation = current_spread - history_gap

        current_price_average = singleton.order_book.current_price_average(
            product)

        estimate_total_price_diff_after_resiliance = (
            deviation * constants.SIMPLE_STRATEGY_RESILIANCE)

        # USD per transaction per USD.
        estimate_profit_per_tran_per_usd = (
            estimate_total_price_diff_after_resiliance /
            current_price_average)

        usd_per_contract = constants.SINGLE_UNIT_IN_USD[singleton.coin_currency]

        # USD per transaction per contract.
        estimate_profit_per_transaction = (
            estimate_profit_per_tran_per_usd * usd_per_contract)

        # USD per transaction per contract.
        estimate_fee_per_transaction = (
            4 * constants.FEE_RATE * usd_per_contract)

        # USD per transaction per contract.
        estimate_net_profit = (estimate_profit_per_transaction -
                               estimate_fee_per_transaction)

        # If esiamte_net_profit > 0, current spread is the minimum profitable
        # gap.
        min_profitable_gap = current_spread

        # Close price gas is the target resilience point we estimate.
        close_price_gap = (
            min_profitable_gap - estimate_total_price_diff_after_resiliance)

        logging.log_every_n_seconds(
            logging.CRITICAL,
            '\nlong:%s , short:%s'
            '\ncurrent_price_average: %.3f'
            '\nestimate_total_price_diff_after_resiliance: %.3f'
            '\nestimate_profit_per_transaction: %.3f'
            '\nestimate_fee_per_transaction: %.3f'
            '\nestimate_net_profit: %.3f'
            '\nzscore: %.3f'
            '\nclose_price_gap: %.3f',
            60 * 60,
            long_instrument,
            short_instrument,
            current_price_average,
            estimate_total_price_diff_after_resiliance,
            estimate_profit_per_transaction,
            estimate_fee_per_transaction,
            estimate_net_profit,
            zscore,
            close_price_gap
        )

        if (estimate_net_profit > constants.SIMPLE_STRATEGY_NET_PROFIT_THRESHOLD and
                zscore >= constants.SIMPLE_STRATEGY_ZSCORE_THRESHOLD):
            long_instrument_speed = singleton.order_book.price_speed(
                long_instrument, 'ask')
            short_instrument_speed = singleton.order_book.price_speed(
                short_instrument, 'bid')
            logging.info(
                'long instrument speed: %.3f, short instrument speed: %.3f',
                long_instrument_speed,
                short_instrument_speed
            )
            if long_instrument_speed > short_instrument_speed:
                slow_instrument_id = short_instrument
                fast_instrument_id = long_instrument
                slow_side = SHORT
                fast_side = LONG
                slow_price = singleton.order_book.bid_price(slow_instrument_id)
                fast_price = singleton.order_book.ask_price(fast_instrument_id)
            else:
                slow_instrument_id = long_instrument
                fast_instrument_id = short_instrument
                slow_side = LONG
                fast_side = SHORT
                slow_price = singleton.order_book.ask_price(slow_instrument_id)
                fast_price = singleton.order_book.bid_price(fast_instrument_id)
            logging.critical(
                '\nTRIGGERED'
                '\nlong:%s , short:%s'
                '\ncurrent_price_average: %.3f'
                '\nestimate_total_price_diff_after_resiliance: %.3f'
                '\nestimate_profit_per_transaction: %.3f'
                '\nestimate_fee_per_transaction: %.3f'
                '\nestimate_net_profit: %.3f'
                '\nzscore: %.3f'
                '\nclose_price_gap: %.3f',
                long_instrument,
                short_instrument,
                current_price_average,
                estimate_total_price_diff_after_resiliance,
                estimate_profit_per_transaction,
                estimate_fee_per_transaction,
                estimate_net_profit,
                zscore,
                close_price_gap
            )
            return ArbitragePlan(
                volume=constants.TRADING_VOLUME,
                slow_instrument_id=slow_instrument_id,
                fast_instrument_id=fast_instrument_id,
                slow_side=slow_side,
                fast_side=fast_side,
                slow_price=slow_price,
                fast_price=fast_price,
                close_price_gap=close_price_gap,
                estimate_net_profit=estimate_net_profit
            )
        else:
            return None


if __name__ == '__main__':
    logger.init_global_logger(log_level=logging.INFO)
