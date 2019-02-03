import asyncio
import logging
import random

import numpy as np

from . import constants, logger, singleton, trigger_strategy
from .arbitrage_execution import (LONG, SHORT, ArbitrageLeg,
                                  ArbitrageTransaction)
from .util import amount_margin


class Trader:
    def __init__(self):
        self.min_time_window = np.timedelta64(
            constants.MIN_TIME_WINDOW_IN_SECOND, 's')
        self.max_volume_per_trading = 1  # always use smallest possible amount
        self.new_tick_received = self.new_tick_received__ramp_up_mode
        self.is_in_cooldown = False
        self.on_going_arbitrage_count = 0

        # TODO: make this configurable
        self.trigger_strategy = trigger_strategy.PercentageTriggerStrategy()
        assert isinstance(self.trigger_strategy,
                          trigger_strategy.TriggerStrategy)

    def cool_down(self):
        async def stop_cool_down():
            await asyncio.sleep(constants.INSUFFICIENT_MARGIN_COOL_DOWN_SECOND)
            self.is_in_cooldown = False
        self.is_in_cooldown = True
        asyncio.create_task(stop_cool_down())

    def new_tick_received__ramp_up_mode(self, instrument_id, ask_prices,
                                        ask_vols, bid_prices, bid_vols):
        if singleton.order_book.time_window >= self.min_time_window:
            self.new_tick_received = self.new_tick_received__regular
            return

        logging.log_every_n_seconds(
            logging.INFO, 'ramping up: %d/%s', 1,
            singleton.order_book.time_window / np.timedelta64(1, 's'),
            self.min_time_window)

    def new_tick_received__regular(self, instrument_id, ask_prices, ask_vols,
                                   bid_prices, bid_vols):
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
        elif self.is_in_cooldown:
            logging.log_every_n_seconds(
                logging.WARNING,
                f'[COOL DOWN] Skipping arbitrage between '
                f'{slow_instrument_id}({slow_side}) and '
                f'{fast_instrument_id}({fast_side}) due to '
                f'cool down',
                30
            )
            return

        trigger_info = self.trigger_strategy.trigger()
        if trigger_info:
            self.trigger_arbitrage(trigger_info)

    def trigger_arbitrage(self, trigger_info):
        transaction = ArbitrageTransaction(
            slow_leg=ArbitrageLeg(
                instrument_id=trigger_info.slow_instrument_id,
                side=trigger_info.slow_side,
                volume=trigger_info.volume,
                price=trigger_info.slow_price
            ),
            fast_leg=ArbitrageLeg(
                instrument_id=trigger_info.fast_instrument_id,
                side=trigger_info.fast_side,
                volume=trigger_info.volume,
                price=trigger_info.fast_price
            ),
            close_price_gap_threshold=trigger_info.close_price_gap,
            estimate_net_profit=trigger_info.estimate_net_profit
        )
        # Run transaction asynchronously. Main tick_received loop doesn't have
        # to await on it.
        self.on_going_arbitrage_count += 1
        asyncio.create_task(transaction.process())


if __name__ == '__main__':
    logger.init_global_logger(log_level=logging.INFO)
    constants.MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE = -1  # Ensure trigger
    singleton.initialize_objects('ETH')
    singleton.trader.min_time_window = np.timedelta64(3, 's')
    logging.info('Manual test started')
    singleton.start_loop()
