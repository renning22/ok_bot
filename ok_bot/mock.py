import pprint

from absl import logging


class MockTrader:
    def new_tick_received(self,
                          instrument_id,
                          ask_prices,
                          ask_vols,
                          bid_prices,
                          bid_vols):
        logging.log_every_n_seconds(
            logging.INFO,
            'mock trader got new tick: %s, best ask: %.3f@%d,'
            'best bid: %.3f@%d',
            10,
            instrument_id,
            ask_prices[0],
            ask_vols[0],
            bid_prices[0],
            bid_vols[0])


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


class MockBookListener:
    def received_futures_depth5(self, *argv):
        logging.info('MockBookListener:\n%s', pprint.pformat(argv))
