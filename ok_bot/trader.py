import asyncio
import logging

import numpy as np

from . import constants, logger, singleton, trigger_strategy
from .arbitrage_execution import (ArbitrageLeg,
                                  ArbitrageTransaction)


class Trader:
    def __init__(self,
                 simple_strategy=False,
                 max_parallel_transaction_num=int(1e9)):
        self.min_time_window = np.timedelta64(
            constants.MIN_TIME_WINDOW_IN_SECOND, 's')
        self.max_volume_per_trading = 1  # always use smallest possible amount
        self.new_tick_received = self.new_tick_received__ramp_up_mode
        self.is_in_cooldown = False
        self.on_going_arbitrage_count = 0
        self.max_parallel_transaction_num = max_parallel_transaction_num
        self.ready = singleton.loop.create_future()

        if simple_strategy:
            self.trigger_strategy = trigger_strategy.SimpleTriggerStrategy()
        else:
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
            if not self.ready.done():
                self.ready.set_result(True)
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
        if self.on_going_arbitrage_count >= self.max_parallel_transaction_num:
            logging.log_every_n_seconds(
                logging.CRITICAL,
                '[WIP SKIP] skip process_pair because there '
                'are %d on going arbitrages(max=%d)',
                30,
                self.on_going_arbitrage_count,
                self.max_parallel_transaction_num
            )
            return
        elif self.is_in_cooldown:
            logging.log_every_n_seconds(
                logging.WARNING,
                f'[COOL DOWN] Skipping arbitrage between '
                f'{slow_instrument}({slow_side}) and '
                f'{fast_instrument}({fast_side}) due to '
                f'cool down',
                30
            )
            return

        arbitrage_plan = self.trigger_strategy.is_there_a_plan(
            long_instrument=long_instrument,
            short_instrument=short_instrument,
            product=product)
        if arbitrage_plan:
            self.kick_off_arbitrage(arbitrage_plan)

    def kick_off_arbitrage(self, arbitrage_plan):
        transaction = ArbitrageTransaction(
            slow_leg=ArbitrageLeg(
                instrument_id=arbitrage_plan.slow_instrument_id,
                side=arbitrage_plan.slow_side,
                volume=arbitrage_plan.volume,
                price=arbitrage_plan.slow_price
            ),
            fast_leg=ArbitrageLeg(
                instrument_id=arbitrage_plan.fast_instrument_id,
                side=arbitrage_plan.fast_side,
                volume=arbitrage_plan.volume,
                price=arbitrage_plan.fast_price
            ),
            close_price_gap_threshold=arbitrage_plan.close_price_gap,
            estimate_net_profit=arbitrage_plan.estimate_net_profit
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
