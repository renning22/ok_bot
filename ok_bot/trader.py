import eventlet
import numpy as np
from absl import logging

from . import constants
from .order_executor import OrderExecutor

rest_api = eventlet.import_patched('ok_bot.rest_api')

MIN_TIME_WINDOW_IN_SECOND = 1  # 60 * 3 # 3 minutes


class Trader:
    def __init__(self,
                 order_executor,
                 arbitrage_threshold,
                 min_time_window=np.timedelta64(
                     MIN_TIME_WINDOW_IN_SECOND, 's'),
                 max_volume_per_trading=2,
                 close_position_take_profit_threshold=40):
        self.arbitrage_threshold = arbitrage_threshold
        self.min_time_window = min_time_window
        self.order_executor = order_executor
        self.order_book = None
        self.max_volume_per_trading = max_volume_per_trading
        self.close_position_take_profit_threshold = close_position_take_profit_threshold

    def new_tick_received(self, order_book):
        if order_book.row_num <= 1 or order_book.time_window < self.min_time_window:
            logging.log_every_n(
                logging.INFO, 'too few ticks, skip trading', 10)
            return

        self.order_book = order_book
        for period_pair in constants.PERIOD_PAIRS:
            ask_period, bid_period = period_pair
            if not order_book.contains_gap_hisotry(ask_period, bid_period):
                continue
            history = order_book.historical_mean_spread(ask_period, bid_period)
            current_spread = order_book.current_spread(ask_period, bid_period)
            deviation = current_spread - history

            logging.debug(
                f'%s, %s, spread: {current_spread}, '
                f'history: {history}, devidation: {deviation}',
                order_book.ask_price(ask_period),
                order_book.bid_price(bid_period))

            # First check if we should open new position
            if deviation < self.arbitrage_threshold:
                self.arbitrage_trading(ask_period, bid_period)
            # Then check if we should close existing position
            if self.should_close_arbitrage(ask_period, bid_period):
                self.close_arbitrage(ask_period, bid_period)

    def close_arbitrage(self, long_period, short_period):
        logging.info('closing arbitrage position with (%s, %s)' %
                     (long_period, short_period))
        vol = min(self.order_book.long_position_volume(long_period),
                  self.order_book.short_position_volume(short_period),
                  self.order_book.ask_volume(long_period),
                  self.order_book.bid_volume(short_period),
                  self.max_volume_per_trading)
        if vol <= 0:
            logging.warning(f'trying to close with [{long_period}] and [{short_period}], '
                            f'but the volume available is {vol}')
            return
        self.order_executor.close_long(
            long_period, self.order_book.bid_price(long_period), vol)
        self.order_executor.close_short(
            short_period, self.order_book.ask_volume(short_period), vol)

    # right now the logic is to close positions when there's enough profit
    def should_close_arbitrage(self, long_period, short_period):
        long_volume = self.order_book.long_position_volume(long_period)
        short_volume = self.order_book.short_position_volume(short_period)
        if long_volume == 0 or short_volume == 0:
            return False  # separate program like order canceler should take care of one side positions
        long_price = self.order_book.long_position_price(long_period)
        short_price = self.order_book.short_position_price(short_period)
        bid_price = self.order_book.bid_price(long_period)
        ask_price = self.order_book.ask_price(short_period)
        estimated_profit = (bid_price - long_price) + (short_price - ask_price)
        return estimated_profit >= self.close_position_take_profit_threshold

    def arbitrage_trading(self, long_period, short_period):
        # TODO: add logic about lock position
        logging.info('opening arbitrage position with (%s, %s)' %
                     (long_period, short_period))
        vol = min(self.order_book.ask_volume(long_period),
                  self.order_book.bid_volume(short_period),
                  self.max_volume_per_trading)
        # will block for execution
        self.order_executor.open_arbitrage_position(
            long_period, self.order_book.ask_price(long_period),
            short_period, self.order_book.bid_price(short_period),
            vol)


if __name__ == '__main__':
    from order_book import MockOrderBook

    def main(_):
        executor = OrderExecutor(rest_api.OKRest('btc'))
        trader = Trader(executor, 0, np.timedelta64(1, 's'))
        trader.new_tick_received(MockOrderBook())
    from absl import app
    app.run(main)
