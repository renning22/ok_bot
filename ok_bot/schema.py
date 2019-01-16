import functools
import pprint

import eventlet
from absl import app, logging

requests = eventlet.import_patched('requests')

OK_TICKER_ADDRESS = 'http://www.okex.com/api/futures/v3/instruments/ticker'


def _instrument_contains_currency(instrument_id, currency):
    """E.g. BTC-USD-190329"""
    t = instrument_id.split('-')
    return len(t) == 3 and t[0] == currency


def _request_all_instrument_ids(currency):
    response = requests.get(OK_TICKER_ADDRESS)
    if response.status_code == 200:
        all_ids = [asset['instrument_id'] for asset in response.json()
                   if _instrument_contains_currency(asset['instrument_id'],
                                                    currency)]
    else:
        logging.fatal('http error on request all instrument ids')

    if not all_ids:
        raise Exception(
            f'Wrong currency? no trading market found for "{currency}"')

    return sorted(all_ids)


class Schema:
    def __init__(self, currency):
        self.currency = currency
        self._all_instrument_ids = _request_all_instrument_ids(currency)
        self._markets_cartesian_product = self._init_markets_cartesian_product()
        self._all_necessary_source_columns = self._init_all_necessary_source_columns()

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
                                bid_market, ask_market)
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


def _testing(_):
    schema = Schema('BTC')
    logging.info('\n%s', pprint.pformat(schema.all_instrument_ids))
    logging.info('\n%s', pprint.pformat(schema.markets_cartesian_product))
    logging.info('\n%s', pprint.pformat(schema.all_necessary_source_columns))


if __name__ == '__main__':
    app.run(_testing)
