import json
import re

import eventlet
from absl import logging

from ok_bot.patched_io_modules import requests

from .api_v3.okex_sdk.futures_api import FutureAPI
from .api_v3_key_reader import API_KEY, KEY_SECRET, PASS_PHRASE


class RestApiV3:
    def __init__(self):
        self.future_sdk = FutureAPI(API_KEY, KEY_SECRET, PASS_PHRASE)

    def ticker(self, instrument_id):
        # use print to show it's non-blocking
        from datetime import datetime
        print(f'starting {instrument_id} at {datetime.now()}')
        data = self.future_sdk.get_specific_ticker(instrument_id)
        print(f'{instrument_id} finished at {datetime.now()}')
        return json.dumps(data)

    def all_instrument_ids(self, currency):
        instruments = self.future_sdk.get_ticker()
        ret = []
        for instrument in instruments:
            # Crash if regex failed to match, as it is fatal
            if re.match(r'([A-Z]{3})-USD-[0-9]{6}',
                        instrument['instrument_id']).group(1) == currency:
                ret.append(instrument['instrument_id'])
        return sorted(ret)

    def create_order(self, client_oid, instrument_id, order_type, amount, price, is_market_order=False, leverage=10):
        """
        :param client_oid: the order ID customized by client side
        :param instrument_id: for example: "TC-USD-180213"
        :param order_type: 1:open long 2:open short 3:close long 4:close short
        :param amount:
        :param price:
        :param is_market_order: place market order if True, price will be ignored for market orders
        :param leverage: 10 or 20
        :return: Order ID and None if success, None and OKEX error code
                 otherwise(https://www.okex.com/docs/en/#error-Error_Code)

        Note:
        * Market order is supported by API V3 in match_price parameter
        * price, amount etc can be int or str, they are all converted to string before being used to compose the
          request URL
        * Limit: 40 times / 2s
        """
        # amount must be integer otherwise OKEX will
        # complain about 'illegal parameter'
        amount = int(amount)
        try:
            resp = self.future_sdk.take_order(
                client_oid,
                instrument_id,
                order_type,
                price,
                amount,
                match_price=1 if is_market_order else 0,
                leverage=leverage)
            if resp['result'] is True and resp['order_id'] != '-1':
                return int(resp['order_id']), None
            else:
                return None, -1
        except Exception as ex:
            logging.error(f'Failed to place order: {ex}')
            return None, ex.code

    def open_long_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            1,
            amount,
            price,
            is_market_order
        )
        return ret

    def open_short_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            2,
            amount,
            price,
            is_market_order
        )
        return ret

    def close_long_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            3,
            amount,
            price,
            is_market_order
        )
        return ret

    def close_short_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            4,
            amount,
            price,
            is_market_order
        )
        return ret

    def revoke_order(self, instrument_id, order_id):
        return self.future_sdk.revoke_order(instrument_id, order_id)

    def _test(self):
        tickers = [
            'BTC-USD-190104',
            'BTC-USD-190111',
            'BTC-USD-190329',
        ]
        pool = eventlet.GreenPool()
        for t in pool.imap(self.ticker, tickers):
            print(t)

        def time_web_speed(url):
            from datetime import datetime
            print(f'starting {url} at {datetime.now()}')
            requests.get(url)
            print(f'{url} finished at {datetime.now()}')

        for url in [
            'https://www.douban.com',
            'https://www.airbnb.com',
            'https://www.baidu.com',
            'https://www.google.com',
            'https://www.amazon.com',
        ]:
            pool.spawn_n(time_web_speed, url)
        pool.waitall()


if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser(description='Manually make/cancel orders')
    arg_parser.add_argument('--action', choices=['open_long_order', 'open_short_order',
                                                 'close_long_order', 'close_short_order',
                                                 'cancel_order'])
    arg_parser.add_argument('--price', type=float)
    arg_parser.add_argument('--volume', type=int)
    arg_parser.add_argument('--custom-id', type=str)
    arg_parser.add_argument('--order-id', type=str)
    arg_parser.add_argument('--instrument-id', type=str,
                            help='Instrument ID is symbols like BTC-USD-190104')
    arg_parser.add_argument('--market-order', default=False, action='store_true', dest='market_order',
                            help='if True, will place market order')
    args = arg_parser.parse_args()
    api = RestApiV3()

    func = getattr(api, args.action)
    if args.action == 'cancel_order':
        print(f'action:{args.action}, {args.instrument_id} {args.order_id}')
        print(func(args.instrument_id, args.order_id))
    else:
        print(f'action:{args.action}, {args.instrument_id} {args.price}@{args.volume}, {args.custom_id}, '
              'market_order:{args.market_order}')
        print(func(args.instrument_id, args.volume,
                   args.price, args.custom_id, args.market_order))
