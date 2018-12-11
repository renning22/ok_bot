import datetime
import pprint
from collections import defaultdict

import numpy as np
import pandas as pd
from absl import logging

from . import constants


def _generate_table_columns():
    columns = []
    for period in constants.PERIOD_TYPES:
        for side in ['bid', 'ask']:
            for depth in ['', '2', '3']:
                columns.append(f'{period}_{side}{depth}_price')
                columns.append(f'{period}_{side}{depth}_vol')
    return columns


_COLUMNS = set(['timestamp', 'source'] + _generate_table_columns())
_TIME_WINDOW = np.timedelta64(
    constants.MOVING_AVERAGE_TIME_WINDOW_IN_SECOND, 's')


class OrderBook:
    def __init__(self):
        # order book data
        self.table = pd.DataFrame()
        self.last_record = {}
        self.ask_minus_bid_columns = OrderBook._generate_pair_columns()
        # position data
        self.positions = defaultdict(dict)

    @staticmethod
    def _extract_ask_bid_period(column_name):
        """Returns periods from ask_minus_bid_columns.

        Example:
         next_week_ask_price-this_week_bid_price => (next_week, this_week)
        """
        assert '-' in column_name
        ask, bid = column_name.split('-')
        return OrderBook.extract_period(ask), OrderBook.extract_period(bid)

    @staticmethod
    def extract_period(column_name):
        """Returns period from column name.

        Example:
         this_week_ask_price => this_week
        """
        # ask_minus_bid_columns name should never be passed
        assert '-' not in column_name
        if column_name.startswith('this_week_'):
            return 'this_week'
        if column_name.startswith('next_week_'):
            return 'next_week'
        if column_name.startswith('quarter_'):
            return 'quarter'
        raise Exception(
            f'trying to extract period from [{column_name}], should never happen')

    def contains_gap_hisotry(self, ask_period, bid_period):
        return OrderBook._pair_column(ask_period, bid_period) in self.table.columns

    @staticmethod
    def _pair_column(ask_period, bid_period):
        return f'{ask_period}_ask_price-{bid_period}_bid_price'

    def historical_mean_spread(self, ask_period, bid_period):
        column = OrderBook._pair_column(ask_period, bid_period)
        return self.table[column].astype('float64').values[:-1].mean()

    def current_spread(self, ask_period, bid_period):
        column = OrderBook._pair_column(ask_period, bid_period)
        return self.table[column].astype('float64').values[-1]

    def ask_price(self, period):
        return self.last_record[f'{period}_ask_price']

    def bid_price(self, period):
        return self.last_record[f'{period}_bid_price']

    def ask_volume(self, period):
        return self.last_record[f'{period}_ask_vol']

    def bid_volume(self, period):
        return self.last_record[f'{period}_bid_vol']

    def long_position_volume(self, period):
        if period not in self.positions or 'long' not in self.positions[period]:
            return 0
        return self.positions[period]['long']['volume']

    def long_position_price(self, period):
        if period not in self.positions or 'long' not in self.positions[period]:
            return 0
        return self.positions[period]['long']['price']

    def short_position_volume(self, period):
        if period not in self.positions or 'short' not in self.positions[period]:
            return 0
        return self.positions[period]['short']['volume']

    def short_position_price(self, period):
        if period not in self.positions or 'short' not in self.positions[period]:
            return 0
        return self.positions[period]['short']['price']

    @property
    def row_num(self):
        return len(self.table)

    @property
    def time_window(self):
        if self.row_num <= 1:
            return np.timedelta64(0, 's')
        return self.table.index[-1] - self.table.index[0]

    def _build_table_row(self, record):
        data = {}
        for c in _COLUMNS:
            if c != 'timestamp':
                data[c] = [record[c]]
        for c in self.ask_minus_bid_columns:
            c1, c2 = tuple(c.split('-'))
            data[f'{c1}-{c2}'] = [record[c1] - record[c2]]
        return pd.DataFrame(data, index=[record['timestamp']])

    def recent_tick_period(self):
        return self.last_record['source'] if self.last_record is not None else None

    def update_book(self, period, data):
        self.last_record['source'] = period
        self.last_record['timestamp'] = np.datetime64(
            datetime.datetime.utcnow())

        for key, value in data.items():
            assert(key in _COLUMNS)
            self.last_record[key] = value
        if len(self.last_record) == len(_COLUMNS):
            self.table = self.table.append(
                self._build_table_row(self.last_record))
            # remove old rows
            self.table = self.table.loc[self.table.index >=
                                        self.table.index[-1] - _TIME_WINDOW]

    def update_position(self, period, data):
        self.positions[period].clear()
        for p in data:
            self.positions[period][p['side']] = {
                'volume': p['amount'],
                'price': p['open_price'],
            }

    def print_debug_string(self):
        pprint.pprint(_COLUMNS)
        pprint.pprint(self.ask_minus_bid_columns)

    @staticmethod
    def _generate_pair_columns():
        asks = [f'{period}_ask_price' for period in constants.PERIOD_TYPES]
        bids = [f'{period}_bid_price' for period in constants.PERIOD_TYPES]
        columns = []
        for ask in asks:
            for bid in bids:
                ask_period = OrderBook.extract_period(ask)
                bid_period = OrderBook.extract_period(bid)
                assert ask_period in constants.PERIOD_TYPES
                assert bid_period in constants.PERIOD_TYPES
                if ask_period != bid_period:  # if not same contract_type
                    columns.append(f'{ask}-{bid}')
        return columns


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

    def long_position_volume(self, *args):
        return 0

    def short_position_volume(self, *args):
        return 0

    @property
    def row_num(self):
        return 100000

    @property
    def time_window(self):
        return np.timedelta64(60 * 60, 's')

    def update_position(self, period, data):
        logging.info(
            'MockOrderBook.update_position:\n %s\n %s', period, data)


if __name__ == '__main__':
    order_book = OrderBook()
    order_book.print_debug_string()
