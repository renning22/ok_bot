import pandas as pd
import numpy as np
import datetime
import pprint
import constants


class OrderBook(object):
    def __init__(self):
        # order book data
        self.table = pd.DataFrame()
        self.columns = ['timestamp', 'source']
        self.last_record = {}
        self.TIME_WINDOW = np.timedelta64(constants.MOVING_AVERAGE_TIME_WINDOW_IN_SECOND, 's')
        for period in constants.PERIOD_TYPES:
            for side in ['bid', 'ask']:
                for depth in ['', '2', '3']:
                    self.columns.append(f'{period}_{side}{depth}_price')
                    self.columns.append(f'{period}_{side}{depth}_vol')
        self.columns_best_asks = [f'{period}_ask_price' for period in constants.PERIOD_TYPES]
        self.columns_best_bids = [f'{period}_bid_price' for period in constants.PERIOD_TYPES]
        self.ask_minus_bid_columns = []
        for ask in self.columns_best_asks:
            for bid in self.columns_best_bids:
                ask_period = OrderBook.extract_period(ask)
                bid_period = OrderBook.extract_period(bid)
                assert ask_period in constants.PERIOD_TYPES
                assert bid_period in constants.PERIOD_TYPES
                if ask_period != bid_period:  # if not same contract_type
                    self.ask_minus_bid_columns.append(f'{ask}-{bid}')
        self.columns = set(self.columns)
        # position data
        self.positions = {}

    # return periods from ask_minus_bid_columns
    # for example:
    # next_week_ask_price-this_week_bid_price => (next_week, this_week)
    @staticmethod
    def extract_ask_bid_period(column_name):
        assert '-' in column_name
        ask, bid = column_name.split('-')
        return OrderBook.extract_period(ask), OrderBook.extract_period(bid)

    # return period from column name
    # for example:
    # this_week_ask_price => this_week
    @staticmethod
    def extract_period(column_name):
        assert '-' not in column_name # ask_minus_bid_columns name should never be passed
        if column_name.startswith("this_week_"):
            return "this_week"
        if column_name.startswith("next_week_"):
            return "next_week"
        if column_name.startswith("quarter_"):
            return "quarter"
        raise Exception("trying to extract period from [%s], should never happen" % column_name)

    def historical_mean_spread(self, column):
        return self.table[column].astype('float64').values[:-1].mean()

    def current_spread(self, column):
        return self.table[column].astype('float64').values[-1]

    def ask_price(self, period):
        return self.last_record[f"{period}_ask_price"]

    def bid_price(self, period):
        return self.last_record[f"{period}_bid_price"]

    def ask_volume(self, period):
        return self.last_record[f"{period}_ask_vol"]

    def bid_volume(self, period):
        return self.last_record[f"{period}_bid_vol"]

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
            return 0
        return self.table.index[-1] - self.table.index[0]

    def build_table_row(self, record):
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
            self.table = self.table.append(self.build_table_row(self.last_record))
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
        pprint.pprint(self.columns_best_asks)
        pprint.pprint(self.columns_best_bids)
        pprint.pprint(self.ask_minus_bid_columns)


if __name__ == '__main__':
    order_book = OrderBook()
    order_book.print_debug_string()