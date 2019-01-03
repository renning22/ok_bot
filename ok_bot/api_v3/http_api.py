from .okex_sdk.futures_api import FutureAPI
import json
import eventlet
from absl import logging


requests = eventlet.import_patched('requests')


class OKHttpV3:
    def __init__(self, api_key, api_secret_key, passphrase):
        self.future_sdk = FutureAPI(api_key, api_secret_key, passphrase)

    def ticker(self, instrument_id):
        # use print to show it's non-blocking
        from datetime import datetime
        print(f'starting {instrument_id} at {datetime.now()}')
        data = self.future_sdk.get_specific_ticker(instrument_id)
        print(f'{instrument_id} finished at {datetime.now()}')
        return json.dumps(data)

    def create_order(self, client_oid, instrument_id, order_type, amount, price, leverage=10):
        """
        :param client_oid: the order ID customized by client side
        :param instrument_id: for example: "TC-USD-180213"
        :param order_type: 1:open long 2:open short 3:close long 4:close short
        :param amount:
        :param price:
        :param leverage: 10 or 20
        :return: Order ID if success, None otherwise

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
                match_price=0,
                leverage=leverage)
            return int(resp['order_id']) \
                if resp['result'] is True and resp['order_id'] != '-1' \
                else None
        except Exception as ex:
            logging.error(f'Failed to place order: {ex}')
            return None

    def open_long_order(self, instrument_id, amount, price, custom_order_id):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            1,
            amount,
            price
        )
        return ret

    def open_short_order(self, instrument_id, amount, price, custom_order_id):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            2,
            amount,
            price
        )
        return ret

    def close_long_order(self, instrument_id, amount, price, custom_order_id):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            3,
            amount,
            price
        )
        return ret

    def close_short_order(self, instrument_id, amount, price, custom_order_id):
        ret = self.create_order(
            custom_order_id,
            instrument_id,
            4,
            amount,
            price
        )
        return ret

    def cancel_order(self, instrument_id, order_id):
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
    from ..api_v3_key_reader import API_KEY, KEY_SECRET, PASS_PHRASE
    from argparse import ArgumentParser
    arg_parser = ArgumentParser(description='Manually make/cancel orders')
    arg_parser.add_argument('--action', choices=['open_long_order', 'open_short_order',
                                                 'close_long_order', 'close_short_order',
                                                 'cancel_order'])
    arg_parser.add_argument('--price', type=float)
    arg_parser.add_argument('--volume', type=int)
    arg_parser.add_argument('--custom-id', type=str)
    arg_parser.add_argument('--order-id', type=str)
    arg_parser.add_argument('--instrument-id', type=str, help='Instrument ID is symbols like BTC-USD-190104')
    args = arg_parser.parse_args()
    api = OKHttpV3(API_KEY, KEY_SECRET, PASS_PHRASE)

    func = getattr(api, args.action)
    if args.action == 'cancel_order':
        print(f'action:{args.action}, {args.instrument_id} {args.order_id}')
        print(func(args.instrument_id, args.order_id))
    else:
        print(f'action:{args.action}, {args.instrument_id} {args.price}@{args.volume}, {args.custom_id}')
        print(func(args.instrument_id, args.volume, args.price, args.custom_id))
