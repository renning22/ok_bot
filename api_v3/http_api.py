from .okex_sdk.futures_api import FutureAPI
import json
import pprint
import eventlet


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

    def _test(self):
        tickers = [
            'BTC-USD-181130',
            'BTC-USD-181207',
            'BTC-USD-181228',
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
    from ..api_key_v3 import API_KEY, KEY_SECRET, PASS_PHRASE
    api = OKHttpV3(API_KEY, KEY_SECRET, PASS_PHRASE)
    api._test()
