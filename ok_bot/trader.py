from random import shuffle

import numpy as np
from absl import logging

from . import constants
from .order_executor import OrderExecutor


class Trader:
    def __init__(self,
                 schema,
                 order_executor,
                 arbitrage_threshold,
                 min_time_window=np.timedelta64(
                     constants.MIN_TIME_WINDOW_IN_SECOND, 's'),
                 max_volume_per_trading=2,
                 close_position_take_profit_threshold=40):
        self.arbitrage_threshold = arbitrage_threshold
        self.min_time_window = min_time_window
        self._schema = schema
        self.order_executor = order_executor
        self.order_book = None
        self.max_volume_per_trading = max_volume_per_trading
        self.close_position_take_profit_threshold = close_position_take_profit_threshold

        self.new_tick_received = self.new_tick_received__ramp_up_mode

    def new_tick_received__ramp_up_mode(self, order_book):
        if order_book.time_window >= self.min_time_window:
            self.new_tick_received = self.new_tick_received__regular
            return

        logging.log_every_n_seconds(
            logging.INFO, 'ramping up: %s/%s', 1, order_book.time_window,
            self.min_time_window)

    def new_tick_received__regular(self, order_book):
        self.order_book = order_book
        for ask_market, bid_market, product in self._schema.markets_cartesian_product:
            self._process_pair(ask_market, bid_market, product)

    def _process_pair(self, ask_market, bid_market, product):
        history = self.order_book.historical_mean_spread(product)
        current_spread = self.order_book.current_spread(product)
        deviation = current_spread - history

        logging.log_every_n_seconds(
            logging.INFO,
            f'%s, %s, %s: %s, %s, spread: {current_spread}, '
            f'history: {history}, devidation: {deviation}',
            2,
            self.order_book.time_window,
            ask_market,
            bid_market,
            self.order_book.ask_price(ask_market),
            self.order_book.bid_price(bid_market))

        # First check if we should open new position
        if deviation < self.arbitrage_threshold:
            self._arbitrage_trading(ask_market, bid_market)
        # Then check if we should close existing position
        # if self._should_close_arbitrage(ask_market, bid_market):
        #     self._close_arbitrage(ask_market, bid_market)

    def _close_arbitrage(self, long_period, short_period):
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
    def _should_close_arbitrage(self, long_period, short_period):
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

    def _arbitrage_trading(self, long_period, short_period):
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
    from .order_book import MockOrderBook
    from .rest_api_v3 import RestApiV3

    def main(_):
        executor = OrderExecutor(RestApiV3())
        trader = Trader(executor, 0, np.timedelta64(1, 's'))
        trader.new_tick_received(MockOrderBook())
    from absl import app
    app.run(main)
