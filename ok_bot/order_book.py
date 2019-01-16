import datetime
import pprint

import numpy as np
import pandas as pd
from absl import logging

from . import constants
from . import singleton
from .schema import Schema

_TIME_WINDOW = np.timedelta64(
    constants.MOVING_AVERAGE_TIME_WINDOW_IN_SECOND, 's')


class OrderBook:
    def __init__(self):
        self._schema = singleton.schema
        self._trader = singleton.trader

        # order book data
        self.table = pd.DataFrame()
        self.last_record = {}

        self.update_book = self._update_book__ramp_up_mode
        for instrument_id in singleton.schema.all_instrument_ids:
            singleton.book_listener.subscribe(instrument_id, self)

    def has_ramped_up(self):
        return self.update_book == self._update_book__regular

    def historical_mean_spread(self, cross_product):
        return self.table[cross_product].astype('float64').values[:-1].mean()

    def current_spread(self, cross_product):
        return self.table[cross_product].astype('float64').values[-1]

    def price_speed(self, instrument_id, ask_or_bid):
        assert ask_or_bid in ['ask', 'bid']
        column = Schema.make_column_name(
            instrument_id, ask_or_bid, 'price')
        history = self.table[column].astype('float64').values[:-1].mean()
        current = self.table[column].astype('float64').values[-1]
        return abs(current - history) / history


    def ask_price(self, instrument_id):
        return self.last_record[Schema.make_column_name(instrument_id, 'ask', 'price')]

    def bid_price(self, instrument_id):
        return self.last_record[Schema.make_column_name(instrument_id, 'bid', 'price')]

    def ask_volume(self, instrument_id):
        return self.last_record[Schema.make_column_name(instrument_id, 'ask', 'vol')]

    def bid_volume(self, instrument_id):
        return self.last_record[Schema.make_column_name(instrument_id, 'bid', 'vol')]

    @property
    def row_num(self):
        return len(self.table)

    @property
    def time_window(self):
        if self.row_num <= 1:
            return np.timedelta64(0, 's')
        return self.table.index[-1] - self.table.index[0]

    def recent_tick_source(self):
        return self.last_record['source']

    def tick_received(self,
                      instrument_id,
                      ask_prices,
                      ask_vols,
                      bid_prices,
                      bid_vols,
                      timestamp):
        self.update_book(instrument_id, ask_prices, ask_vols, bid_prices, bid_vols)

    def _sink_piece_of_fresh_data_to_last_record(self,
                                                 instrument_id,
                                                 ask_prices,
                                                 ask_vols,
                                                 bid_prices,
                                                 bid_vols):
        self.last_record[Schema.make_column_name(
            instrument_id, 'ask', 'price')] = ask_prices[0]
        self.last_record[Schema.make_column_name(
            instrument_id, 'ask', 'vol')] = ask_vols[0]
        self.last_record[Schema.make_column_name(
            instrument_id, 'bid', 'price')] = bid_prices[0]
        self.last_record[Schema.make_column_name(
            instrument_id, 'bid', 'vol')] = bid_vols[0]

    def _update_book__ramp_up_mode(self,
                                   instrument_id,
                                   ask_prices,
                                   ask_vols,
                                   bid_prices,
                                   bid_vols):
        self._sink_piece_of_fresh_data_to_last_record(instrument_id,
                                                      ask_prices,
                                                      ask_vols,
                                                      bid_prices,
                                                      bid_vols)

        if set(self._schema.all_necessary_source_columns) == set(self.last_record.keys()):
            logging.info('have all the necessary prices in every market, ramping up finished:\n%s',
                         pprint.pformat(self.last_record))
            # Finished ramp-up
            self.update_book = self._update_book__regular

    def _update_book__regular(self,
                              instrument_id,
                              ask_prices,
                              ask_vols,
                              bid_prices,
                              bid_vols):
        self.last_record['source'] = instrument_id
        self.last_record['timestamp'] = np.datetime64(
            datetime.datetime.utcnow())

        self._sink_piece_of_fresh_data_to_last_record(instrument_id,
                                                      ask_prices,
                                                      ask_vols,
                                                      bid_prices,
                                                      bid_vols)
        self.table = self.table.append(
            self._convert_last_record_to_table_row(), sort=True)
        # remove old rows
        self.table = self.table.loc[self.table.index
                                    >= self.table.index[-1] - _TIME_WINDOW]

        # Callback
        self._trader.new_tick_received(instrument_id, ask_prices, ask_vols, bid_prices, bid_vols)

    def _convert_last_record_to_table_row(self):
        # TODO(luanjunyi): consider removing the handicap data from table. Use table only
        # for price spread history

        # Move old source data.
        data = self.last_record.copy()

        # Calculate new derived data.
        for long_instrument, short_instrument, product in self._schema.markets_cartesian_product:
            ask_price_name = Schema.make_column_name(
                long_instrument, 'ask', 'price')
            bid_price_name = Schema.make_column_name(
                short_instrument, 'bid', 'price')
            data[product] = self.last_record[bid_price_name] - self.last_record[ask_price_name]
        return pd.DataFrame(data, index=[self.last_record['timestamp']])


class MockOrderBook:
    def contains_gap_hisotry(self, *args):
        return True

    def historical_mean_spread(self, *args):
        return 0

    def current_spread(self, *args):
        return -500

    def ask_price(self, *args):
        return 1000

    def bid_price(self, *args):
        return 2000

    def ask_volume(self, *args):
        return 100

    def bid_volume(self, *args):
        return 200

    @property
    def row_num(self):
        return 100000

    @property
    def time_window(self):
        return np.timedelta64(60 * 60, 's')

    def update_book(self, market, data):
        logging.info(
            'MockOrderBook.update_book:\n %s\n %s', market, data)
