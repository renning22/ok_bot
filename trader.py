from order_book import OrderBook
from order_executor import OrderExecutor
from absl import logging
from rest_api import OKRest
import numpy as np
import asyncio

MIN_TIME_WINDOW_IN_SECOND = 1 # 60 * 3 # 3 minutes


class Trader(object):
    def __init__(self,
                 order_executor,
                 arbitrage_threshold,
                 min_time_window = np.timedelta64(MIN_TIME_WINDOW_IN_SECOND, 's'),
                 max_volume_per_trading = 2):
        self.arbitrage_threshold = arbitrage_threshold
        self.min_time_window = min_time_window
        self.order_executor = order_executor
        self.order_book = None
        self.max_volume_per_trading = max_volume_per_trading

    def new_tick_received(self, order_book):
        if order_book.row_num <= 1 or order_book.time_window < self.min_time_window:
            logging.info("too few ticks, skip trading")
            return
        # First check if we should open new position
        self.order_book = order_book
        for pair_column in order_book.ask_minus_bid_columns:
            if pair_column not in order_book.table.columns:
                continue
            ask_period, bid_period = OrderBook.extract_ask_bid_period(pair_column)
            history = order_book.historical_mean_spread(pair_column)
            current_spread = order_book.current_spread(pair_column)
            deviation = current_spread - history
            logging.info(f"{ask_period}[%.2f] "
                         f"{bid_period}[%.2f] spread: {current_spread}, "
                         f"history: {history}, devidation: {deviation}"
                         % (order_book.ask_price(ask_period), order_book.bid_price(bid_period)))
            if deviation < self.arbitrage_threshold:
                self.arbitrage_trading(ask_period, bid_period)
        # Then check if we should close existing position

    def arbitrage_trading(self, long_period, short_period):
        # TODO: add logic about lock position
        vol = min(self.order_book.ask_volume(long_period),
                  self.order_book.bid_volume(short_period),
                  self.max_volume_per_trading)
        asyncio.ensure_future(
            self.arbitrage_with_executors(long_period, self.order_book.ask_price(long_period),
                                          short_period, self.order_book.bid_price(short_period),
                                          vol)
        )

    async def arbitrage_with_executors(self, long_period, long_price, short_period, short_price, volume):
        self.order_executor.long(long_period, long_price, volume)
        self.order_executor.short(short_period, short_price, volume)


if __name__ == "__main__":
    def main(_):
        executor = OrderExecutor(OKRest('btc'))
        trader = Trader(executor, 0, np.timedelta64(1, 's'))
        asyncio.ensure_future(trader.arbitrage_with_executors('this_week', '1000',
                                                              'quarter', '9000', 100))
        asyncio.get_event_loop().run_forever()
    from absl import app
    app.run(main)