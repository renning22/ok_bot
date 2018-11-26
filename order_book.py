import pandas as pd
import numpy as np
import datetime
import pprint
import constants


class OrderBook:
    def __init__(self):
        # order book data
        self.table = pd.DataFrame()
        self.last_record = {}
        self.TIME_WINDOW = np.timedelta64(constants.MOVING_AVERAGE_TIME_WINDOW_IN_SECOND, 's')
        self.columns = set(['timestamp', 'source'] + OrderBook._generate_table_columns())
        self.ask_minus_bid_columns = OrderBook._generate_pair_columns()
        # position data
        self.positions = {}

    # return periods from ask_minus_bid_columns
    # for example:
    # next_week_ask_price-this_week_bid_price => (next_week, this_week)
    @staticmethod
    def _extract_ask_bid_period(column_name):
        assert '-' in column_name
        ask, bid = column_name.split('-')
        return OrderBook.extract_period(ask), OrderBook.extract_period(bid)

    # return period from column name
    # for example:
    # this_week_ask_price => this_week
    @staticmethod
    def extract_period(column_name):
        assert '-' not in column_name # ask_minus_bid_columns name should never be passed
        if column_name.startswith('this_week_'):
            return 'this_week'
        if column_name.startswith('next_week_'):
            return 'next_week'
        if column_name.startswith('quarter_'):
            return 'quarter'
        raise Exception(f'trying to extract period from [{column_name}], should never happen')

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
        self.positions[period]['long']['volume']

    def long_position_price(self, period):
        if period not in self.positions or 'long' not in self.positions[period]:
            return 0
        self.positions[period]['long']['price']

    def short_position_volume(self, period):
        if period not in self.positions or 'short' not in self.positions[period]:
            return 0
        self.positions[period]['short']['volume']

    def short_position_price(self, period):
        if period not in self.positions or 'short' not in self.positions[period]:
            return 0
        self.positions[period]['short']['price']


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
        for c in self.columns:
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
        self.last_record['timestamp'] = np.datetime64(datetime.datetime.utcnow())

        for key, value in data.items():
            assert(key in self.columns)
            self.last_record[key] = value
        if len(self.last_record) == len(self.columns):
            self.table = self.table.append(self._build_table_row(self.last_record))
            # remove old rows
            self.table = self.table.loc[self.table.index >= self.table.index[-1] - self.TIME_WINDOW]

    def update_position(self, period, data):
        self.positions.setdefault(period, {})
        if period in self.positions:
            self.positions[period].clear()

        for p in data:
            self.positions[period][p['side']] = {
                'volume': p['amount'],
                'price': p['open_price'],
            }

    def print_debug_string(self):
        pprint.pprint(self.columns)
        pprint.pprint(self.ask_minus_bid_columns)

    @staticmethod
    def _generate_table_columns():
        columns = []
        for period in constants.PERIOD_TYPES:
            for side in ['bid', 'ask']:
                for depth in ['', '2', '3']:
                    columns.append(f'{period}_{side}{depth}_price')
                    columns.append(f'{period}_{side}{depth}_vol')
        return columns

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


if __name__ == '__main__':
    order_book = OrderBook()
    order_book.print_debug_string()