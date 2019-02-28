import datetime
import logging
import pprint
import time
from collections import defaultdict, namedtuple

import dateutil.parser as dp
import numpy as np
import pandas as pd
from scipy import stats

from . import singleton
from .constants import (MOVING_AVERAGE_TIME_WINDOW_IN_SECOND,
                        PRICE_PREDICTION_WINDOW_SECOND)
from .quant import Quant
from .schema import Schema

_TIME_WINDOW = np.timedelta64(
    MOVING_AVERAGE_TIME_WINDOW_IN_SECOND, 's')


class AvailableOrder:
    def __init__(self, price, volume):
        self.price = price
        self.volume = volume

    def __repr__(self):
        return f'{self.price:8.6} {self.volume:6}'

    def __lt__(self, other):
        return (self.price, self.volume) < (other.price, other.volume)


class MarketDepth:
    # ask_prices and bid_prices are sorted in book_listener
    def __init__(self, instrument_id, ask_prices, ask_vols, bid_prices, bid_vols, timestamp):
        self.instrument_id = instrument_id
        self.timestamp_local = time.time()
        self.timestamp_server = dp.parse(timestamp).timestamp()
        self.bid_stack_ = []
        self.ask_stack_ = []
        self.update(ask_prices, ask_vols, bid_prices, bid_vols)

    def best_ask_price(self):
        return self.ask_stack_[0].price

    def best_bid_price(self):
        return self.bid_stack_[0].price

    def ask(self):
        return self.ask_stack_

    def bid(self):
        return self.bid_stack_

    def __str__(self):
        now_local = time.time()
        now_server = now_local + singleton.schema.time_diff_sec
        ask_slope = singleton.order_book.price_linear_fit(
            self.instrument_id, 'ask', PRICE_PREDICTION_WINDOW_SECOND)
        bid_slope = singleton.order_book.price_linear_fit(
            self.instrument_id, 'bid', PRICE_PREDICTION_WINDOW_SECOND)
        ret = '------ market_depth ------\n'
        ret += 'ask_slope: {:.6f}, bid_slope: {:.6f}\n'.format(
            ask_slope, bid_slope)
        ret += 'local_delay: {:.2f} sec\n'.format(
            now_local - self.timestamp_local)
        ret += 'server_delay: {:.2f} sec\n'.format(
            now_server - self.timestamp_server)
        ret += 'local_server_diff: {:.2f} sec\n'.format(
            self.timestamp_local - self.timestamp_server)
        ret += pprint.pformat(list(reversed(self.ask_stack_)))
        ret += '\n'
        ret += pprint.pformat(self.bid_stack_)
        ret += '\n--------------------------'
        return ret

    def update(self, ask_prices, ask_vols, bid_prices, bid_vols):
        self.bid_stack_ = [AvailableOrder(price=price, volume=volume)
                           for price, volume in list(zip(bid_prices, bid_vols))]
        self.ask_stack_ = [AvailableOrder(price=price, volume=volume)
                           for price, volume in list(zip(ask_prices, ask_vols))]


class OrderBook:
    def __init__(self):
        self._schema = singleton.schema
        self._trader = singleton.trader

        # order book data
        self.table = defaultdict(pd.Series)
        self.last_record = {}
        self._market_depth = {}

        self.update_book = self._update_book__ramp_up_mode
        for instrument_id in singleton.schema.all_instrument_ids:
            singleton.book_listener.subscribe(instrument_id, self)
        self.ready = singleton.loop.create_future()

    def window(self, column, window_sec=None):
        if window_sec is None:
            return self.table[column]
        else:
            assert window_sec > 0
            return self.table[column].loc[
                self.table[column].index >= self.table[column].index[-1] - np.timedelta64(window_sec, 's')]

    def zscore(self, cross_product):
        zscores = stats.zscore(self.table[cross_product].astype('float64'))
        return Quant(zscores[-1])

    def historical_mean_spread(self, cross_product):
        return Quant(self.table[cross_product].values[:-1].mean())

    def current_spread(self, cross_product):
        return Quant(self.table[cross_product].values[-1])

    def current_price_average(self, cross_product):
        for long_instrument, short_instrument, product in self._schema.markets_cartesian_product:
            if product == cross_product:
                return (self.ask_price(long_instrument) + self.bid_price(short_instrument)) / 2
        raise RuntimeError(f'no such {cross_product}')

    def price_speed(self, instrument_id, ask_or_bid, window_sec=None):
        assert ask_or_bid in ['ask', 'bid']
        column = Schema.make_column_name(
            instrument_id, ask_or_bid, 'price')
        w = self.window(column, window_sec)
        if len(w) <= 1:
            return Quant(0)
        history = w.values[:-1].mean()
        current = w.values[-1]
        return Quant((current - history) / history)

    def price_linear_fit(self, instrument_id, ask_or_bid, window_sec=None):
        assert ask_or_bid in ['ask', 'bid']
        column = Schema.make_column_name(
            instrument_id, ask_or_bid, 'price')
        w = self.window(column, window_sec)
        if len(w) <= 1:
            return Quant(0)

        left = w.index[0].timestamp()
        x = [i.timestamp() - left for i in w.index]
        y = w.values.astype('float64')
        p = np.polynomial.polynomial.Polynomial.fit(x=x, y=y, deg=1)
        return p.coef[1]

    def ask_price(self, instrument_id):
        return Quant(self.last_record[Schema.make_column_name(instrument_id, 'ask', 'price')])

    def bid_price(self, instrument_id):
        return Quant(self.last_record[Schema.make_column_name(instrument_id, 'bid', 'price')])

    def ask_volume(self, instrument_id):
        return Quant(self.last_record[Schema.make_column_name(instrument_id, 'ask', 'vol')])

    def bid_volume(self, instrument_id):
        return Quant(self.last_record[Schema.make_column_name(instrument_id, 'bid', 'vol')])

    @property
    def time_window(self):
        return min([i.index[-1] - i.index[0] for _, i in self.table.items()])

    def recent_tick_source(self):
        return self.last_record['source']

    def tick_received(self,
                      instrument_id,
                      ask_prices,
                      ask_vols,
                      bid_prices,
                      bid_vols,
                      timestamp):
        self._market_depth[instrument_id] = MarketDepth(
            instrument_id, ask_prices, ask_vols, bid_prices, bid_vols, timestamp)

        self.last_record['source'] = instrument_id
        self.last_record['timestamp'] = np.datetime64(timestamp)

        self.update_book(instrument_id,
                         ask_prices,
                         ask_vols,
                         bid_prices,
                         bid_vols)

    def market_depth(self, instrument_id):
        return self._market_depth[instrument_id]

    def _update_book__ramp_up_mode(self,
                                   instrument_id,
                                   ask_prices,
                                   ask_vols,
                                   bid_prices,
                                   bid_vols):
        self._update_raw_data(instrument_id,
                              ask_prices,
                              ask_vols,
                              bid_prices,
                              bid_vols)

        a = set(self._schema.all_necessary_source_columns)
        b = set(self.last_record.keys())
        if a.intersection(b) == a:
            logging.info('have all the necessary prices in every market, '
                         'ramping up finished')
            self.update_book = self._update_book__regular

    def _update_book__regular(self,
                              instrument_id,
                              ask_prices,
                              ask_vols,
                              bid_prices,
                              bid_vols):
        self._update_raw_data(instrument_id,
                              ask_prices,
                              ask_vols,
                              bid_prices,
                              bid_vols)
        self._update_derived_data()

        # Until there are more than 1 data points. Otherwise
        # "values[:-1].mean()" will have problem.
        if not self.ready.done() and min([len(i) for i in self.table]) > 1:
            self.ready.set_result(True)

        # Callback
        self._trader.new_tick_received(
            instrument_id, ask_prices, ask_vols, bid_prices, bid_vols)

    def _update_raw_data(self,
                         instrument_id,
                         ask_prices,
                         ask_vols,
                         bid_prices,
                         bid_vols):
        self.last_record[Schema.make_column_name(
            instrument_id, 'ask', 'price')] = ask_prices[0]
        self._update_table(Schema.make_column_name(
            instrument_id, 'ask', 'price'), ask_prices[0])
        self.last_record[Schema.make_column_name(
            instrument_id, 'ask', 'vol')] = ask_vols[0]
        self._update_table(Schema.make_column_name(
            instrument_id, 'ask', 'vol'), ask_vols[0])
        self.last_record[Schema.make_column_name(
            instrument_id, 'bid', 'price')] = bid_prices[0]
        self._update_table(Schema.make_column_name(
            instrument_id, 'bid', 'price'), bid_prices[0])
        self.last_record[Schema.make_column_name(
            instrument_id, 'bid', 'vol')] = bid_vols[0]
        self._update_table(Schema.make_column_name(
            instrument_id, 'bid', 'vol'), bid_vols[0])

    def _update_derived_data(self):
        for long_instrument, short_instrument, product in self._schema.markets_cartesian_product:
            ask_price_name = Schema.make_column_name(
                long_instrument, 'ask', 'price')
            bid_price_name = Schema.make_column_name(
                short_instrument, 'bid', 'price')
            new_point = self.last_record[bid_price_name] - \
                self.last_record[ask_price_name]
            self._update_table(product, new_point)

    def _update_table(self, column, value):
        new_point = pd.Series(value, index=[self.last_record['timestamp']])
        this_table = self.table[column]
        this_table = this_table.append(new_point)
        self.table[column] = this_table.loc[
            this_table.index >= this_table.index[-1] - _TIME_WINDOW]


def _testing_non_blocking():
    import asyncio
    from . import singleton, logger
    logger.init_global_logger(log_level=logging.INFO, log_to_stderr=True)

    async def ping():
        await singleton.order_book.ready
        logging.info('ready')
        while True:
            for long_instrument, short_instrument, product in singleton.schema.markets_cartesian_product:
                t = singleton.order_book.table[Schema.make_column_name(
                    long_instrument, 'ask', 'price')]
                p = singleton.order_book.current_spread(product)
                s = singleton.order_book.price_linear_fit(
                    long_instrument, 'ask', 2)
                logging.info('%s : %s', long_instrument, t)
                logging.info('%s : %s', long_instrument, p)
                logging.info('%s : %s', long_instrument, s)
                logging.info('%s : %s', long_instrument,
                             singleton.order_book.time_window)
                break

            await asyncio.sleep(1)

    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    singleton.loop.create_task(ping())
    singleton.start_loop()


if __name__ == '__main__':
    _testing_non_blocking()
