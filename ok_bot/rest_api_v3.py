import asyncio
import json
import re
from concurrent.futures import ThreadPoolExecutor

from absl import app, logging

from . import singleton
from .api_v3.okex_sdk.futures_api import FutureAPI
from .api_v3_key_reader import API_KEY, KEY_SECRET, PASS_PHRASE


class RestApiV3:
    def __init__(self):
        self.future_sdk = FutureAPI(API_KEY, KEY_SECRET, PASS_PHRASE)

        # If max_workers is None or not given, it will default to the number of
        # processors on the machine, multiplied by 5, assuming that
        # ThreadPoolExecutor is often used to overlap I/O instead of CPU work
        # and the number of workers should be higher than the number of workers
        # for ProcessPoolExecutor.
        self._executor = ThreadPoolExecutor(max_workers=None)

    def ticker(self, instrument_id):
        # use print to show it's non-blocking
        from datetime import datetime
        print(f'starting {instrument_id} at {datetime.now()}')
        data = self.future_sdk.get_specific_ticker(instrument_id)
        print(f'{instrument_id} finished at {datetime.now()}')
        return json.dumps(data)

    def _all_instrument_ids(self, currency):
        instruments = self.future_sdk.get_ticker()
        ret = []
        for instrument in instruments:
            # Crash if regex failed to match, as it is fatal
            if re.match(r'([A-Z]{3})-USD-[0-9]{6}',
                        instrument['instrument_id']).group(1) == currency:
                ret.append(instrument['instrument_id'])
        return sorted(ret)

    def all_instrument_ids(self, currency):
        return singleton.loop.run_in_executor(
            self._executor, self._all_instrument_ids, currency)

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
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            1,
            amount,
            price,
            is_market_order
        )

    def open_short_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            2,
            amount,
            price,
            is_market_order
        )

    def close_long_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            3,
            amount,
            price,
            is_market_order
        )

    def close_short_order(self, instrument_id, amount, price, custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            4,
            amount,
            price,
            is_market_order
        )

    def revoke_order(self, instrument_id, order_id):
        return singleton.loop.run_in_executor(
            self._executor,
            self.future_sdk.revoke_order,
            instrument_id,
            order_id)

    def get_order_info(self, order_id, instrument_id):
        return singleton.loop.run_in_executor(
            self._executor,
            self.future_sdk.get_order_info,
            order_id,
            instrument_id)


async def _testing_coroutine(i, api):
    logging.info('start %s', i)
    r = await api.all_instrument_ids('ETH')
    logging.info('end %s = %s', i, r)


def _testing(_):
    singleton.loop = asyncio.get_event_loop()
    api = RestApiV3()
    coroutines = [_testing_coroutine(i, api) for i in range(5)]
    singleton.loop.run_until_complete(asyncio.gather(*coroutines))


if __name__ == '__main__':
    app.run(_testing)
