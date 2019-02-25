import logging
import time

from . import server_time, singleton


class Schema:
    def __init__(self, currency):
        self.currency = currency
        self._all_instrument_ids = (
            singleton.rest_api.get_all_instrument_ids_blocking(currency))
        self._instrument_periods = dict(
            zip(self._all_instrument_ids,
                ['this_week', 'next_week', 'quarter']))
        self._markets_cartesian_product = self._init_markets_cartesian_product()
        self._all_necessary_source_columns =\
            self._init_all_necessary_source_columns()

        local_timestamp_sec = time.time()
        server_timestamp_sec = server_time.get_server_timestamp()
        self.time_diff_sec = server_timestamp_sec - local_timestamp_sec

    @staticmethod
    def make_column_name(instrument_id, ask_or_bid, price_or_vol):
        return f'{instrument_id}_{ask_or_bid}_{price_or_vol}'

    @staticmethod
    def make_market_cross_product(ask_market, bid_market):
        return '{}*{}'.format(ask_market, bid_market)

    @property
    def all_instrument_ids(self):
        return self._all_instrument_ids

    @property
    def markets_cartesian_product(self):
        return self._markets_cartesian_product

    @property
    def all_necessary_source_columns(self):
        return self._all_necessary_source_columns

    def instrument_period(self, instrument_id):
        # Crash if instrument_id not in self._instrument_periods
        return self._instrument_periods[instrument_id]

    def _init_markets_cartesian_product(self):
        """In format ASK_MARKET*BID_MARKET.

        E.g. BTC-USD-190329*BTC-USD-190104

        Returns:
            [(MARKET_A, MARKET_B, MARKET_B*MARKET_A), ...]

        It means if we wanna long MARKET_A and short MARKET_B,
        we should look at if MARKET_B*MARKET_A(price_B - price_A)
        has increased beyond the gap threshold.
        """
        columns = []
        for ask_market in self._all_instrument_ids:
            for bid_market in self._all_instrument_ids:
                if ask_market != bid_market:
                    columns.append(
                        (
                            ask_market,
                            bid_market,
                            Schema.make_market_cross_product(
                                ask_market=ask_market,
                                bid_market=bid_market)
                        )
                    )
        return columns

    def _init_all_necessary_source_columns(self):
        """MARKET_ask_price, MARKET_ask_vol, etc.."""
        columns = []
        for instrument_id in self._all_instrument_ids:
            for side in ['bid', 'ask']:
                columns.append(f'{instrument_id}_{side}_price')
                columns.append(f'{instrument_id}_{side}_vol')
        return columns


def _testing():
    from .logger import init_global_logger
    from .rest_api_v3 import RestApiV3
    import pprint
    init_global_logger(log_level=logging.INFO, log_to_stderr=True)
    singleton.rest_api = RestApiV3()
    schema = Schema('BTC')
    logging.info('\n%s', pprint.pformat(schema.all_instrument_ids))
    logging.info('\n%s', pprint.pformat(schema.markets_cartesian_product))
    logging.info('\n%s', pprint.pformat(schema.all_necessary_source_columns))
    logging.info('delta: %s', schema.time_diff_sec)


if __name__ == '__main__':
    _testing()
