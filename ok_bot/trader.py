import asyncio
import logging

import numpy as np

from . import constants, logger, singleton
from .arbitrage_execution import (LONG, SHORT, ArbitrageLeg,
                                  ArbitrageTransaction)
from .util import amount_margin


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
        self.is_in_cooldown = False
        self.arbitrage_wip = False

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
        for long_instrument, short_instrument, product in \
                self._schema.markets_cartesian_product:
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
        if self.arbitrage_wip:
            logging.warning('skip process_pair because there\'s on '
                            'going arbitrage')
            return
        history_gap = self.order_book.historical_mean_spread(product)
        current_spread = self.order_book.current_spread(product)

        # The deviation is the larger the better (the more profitable).
        #
        #   deviation = (bid - ask) - (avg(bid) - avg(ask))
        #             = (short_price - long_price) - ...
        #
        # Imagine deviation becomes deviation - 1.
        # It could be either (or other cases equivalent) from:
        #   1) short_price -> short_price - 1
        #   2) long_price -> long_price + 1
        #
        # For either case we would profit 1 dollar (assume we opened
        # 1-coin-value-equivalent contracts (after leverage) at on both side at
        # price short_price/long_price.
        #
        # Assume the resilience drives deviation back to the half of the delta.
        #
        #   deviation --> deviation / 2
        #
        # And analogously, the total price differences would be:
        #   total_price_diff = deviation / 2 - deviation
        #
        # Can it be an estimate of profit?
        #
        # For fee we can approximate it by 4 times of current average price:
        #   total_fee = 4 * 0.030% * (short_price + long_price) / 2
        deviation = current_spread - history_gap

        current_price_average = self.order_book.current_price_average(product)

        estimate_total_price_diff_after_resiliance = deviation / 2

        estimate_profit_per_tran_per_dollar = (
            estimate_total_price_diff_after_resiliance /
            current_price_average)

        estimate_profit_per_tran_per_contract = (
            estimate_profit_per_tran_per_dollar
            * constants.SINGLE_UNIT_IN_USD[singleton.coin_currency])

        estimate_fee_per_tran_per_contract = (
            4 * 0.00030 * constants.SINGLE_UNIT_IN_USD[singleton.coin_currency])

        estimate_net_profit = estimate_profit_per_tran_per_contract - \
            estimate_fee_per_tran_per_contract

        # If esiamte_net_profit > 0, current spread is the minimum profitable
        # gap.
        min_price_gap = current_spread

        # Close price gas is the target resilience point we estimate.
        close_price_gap = (
            min_price_gap - estimate_total_price_diff_after_resiliance)

        logging.log_every_n_seconds(
            logging.CRITICAL,
            '\nlong:%s , short:%s'
            '\ncurrent_price_average: %.3f'
            '\nestimate_total_price_diff_after_resiliance: %.3f'
            '\nestimate_profit_per_tran_per_dollar: %.3f'
            '\nestimate_profit_per_tran_per_contract: %.3f'
            '\nestimate_fee_per_tran_per_contract: %.3f'
            '\nestimate_net_profit: %.3f'
            '\nmin_price_gap: %.3f'
            '\nclose_price_gap: %.3f',
            60,
            long_instrument,
            short_instrument,
            current_price_average,
            estimate_total_price_diff_after_resiliance,
            estimate_profit_per_tran_per_dollar,
            estimate_profit_per_tran_per_contract,
            estimate_fee_per_tran_per_contract,
            estimate_net_profit,
            min_price_gap,
            close_price_gap
        )

        # Ignore available amount for now.
        if estimate_net_profit > 0:
            long_instrument_speed = self.order_book.price_speed(
                long_instrument, 'ask')
            short_instrument_speed = self.order_book.price_speed(
                short_instrument, 'bid')
            logging.info(f'Long instrument speed: {long_instrument_speed:.3f}, '
                         f'short instrument speed: '
                         f'{short_instrument_speed:.3f}')
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
            logging.warning(f'[COOL DOWN] Skipping arbitrage between '
                            f'{slow_instrument_id}({slow_side}) and '
                            f'{fast_instrument_id}({fast_side}) due to '
                            f'cool down')
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
            estimate_net_profit=estimate_net_profit
        )
        # Run transaction asynchronously. Main tick_received loop doesn't have
        # to await on it.
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
