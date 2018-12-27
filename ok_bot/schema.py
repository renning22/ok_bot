import functools
import pprint

import eventlet
from absl import app, logging

requests = eventlet.import_patched('requests')

OK_TICKER_ADDRESS = 'http://www.okex.com/api/futures/v3/instruments/ticker'


def _request_all_instrument_ids(currency):
    response = requests.get(OK_TICKER_ADDRESS)
    if response.status_code == 200:
        all_ids = [asset['instrument_id'] for asset in response.json()
                   if currency in asset['instrument_id']]
    else:
        logging.fatal('http error on request all instrument ids')

    if not all_ids:
        raise Exception(
            f'Wrong currency? no trading market found for "{currency}"')

    return all_ids


class Schema:
    def __init__(self, currency):
        self.currency = currency
        self._all_instrument_ids = _request_all_instrument_ids(currency)

    @staticmethod
    def _extract_instrument_id(column_name):
        """Returns period from column name.

        Example:
         BTC-USD-190329_ask_price => BTC-USD-190329
        """
        # The lenght of 'BTC-USD-190329' is 14.
        return column_name[:14]

    @staticmethod
    def make_column_name(instrument_id, ask_or_bid, price_or_vol):
        return f'{instrument_id}_{ask_or_bid}_{price_or_vol}'

    @staticmethod
    def make_market_cross_product(ask_market, bid_market):
        return '{}*{}'.format(ask_market, bid_market)

    def get_all_instrument_ids(self):
        return self._all_instrument_ids

    @functools.lru_cache()
    def get_markets_cartesian_product(self):
        """In format ASK_MARKET*BID_MARKET.

        E.g. BTC-USD-190329*BTC-USD-190104

        Returns:
            [(MARKET_A, MARKET_B, MARKET_A*MARKET_B), ...]
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
                                ask_market, bid_market)
                        )
                    )
        return columns

    @functools.lru_cache()
    def get_all_necessary_source_columns(self):
        """MARKET_ask_price, MARKET_ask_vol, etc.."""
        columns = []
        for period in self._all_instrument_ids:
            for side in ['bid', 'ask']:
                columns.append(f'{period}_{side}_price')
                columns.append(f'{period}_{side}_vol')
        return columns


def _testing(_):
    schema = Schema('BTC')
    logging.info('\n%s', pprint.pformat(schema.get_all_instrument_ids()))
    logging.info('\n%s', pprint.pformat(
        schema.get_markets_cartesian_product()))
    logging.info('\n%s', pprint.pformat(
        schema.get_all_necessary_source_columns()))


if __name__ == '__main__':
    app.run(_testing)
