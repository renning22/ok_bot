import asyncio
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor

from . import constants, singleton
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

    def get_all_instrument_ids_blocking(self, currency):
        instruments = self.future_sdk.get_ticker()
        ret = []
        for instrument in instruments:
            # Crash if regex failed to match, as it is fatal
            if re.match(r'([A-Z]{3})-USD-[0-9]{6}',
                        instrument['instrument_id']).group(1) == currency:
                ret.append(instrument['instrument_id'])
        return sorted(ret)

    def create_order(self, client_oid, instrument_id, order_type, amount, price,
                     is_market_order=False, leverage=20):
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
        assert not is_market_order,\
            "Market order in OKEX is always inferior to limit order " \
            "and should never be placed"
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
            return None, -1

    def open_long_order(self, instrument_id, amount, price,
                        custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            constants.ORDER_TYPE_CODE__OPEN_LONG,
            amount,
            price,
            is_market_order
        )

    def open_short_order(self, instrument_id, amount, price,
                         custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            constants.ORDER_TYPE_CODE__OPEN_SHORT,
            amount,
            price,
            is_market_order
        )

    def close_long_order(self, instrument_id, amount, price,
                         custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            constants.ORDER_TYPE_CODE__CLOSE_LONG,
            amount,
            price,
            is_market_order
        )

    def close_short_order(self, instrument_id, amount, price,
                          custom_order_id=None, is_market_order=False):
        return singleton.loop.run_in_executor(
            self._executor,
            self.create_order,
            custom_order_id,
            instrument_id,
            constants.ORDER_TYPE_CODE__CLOSE_SHORT,
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

    def all_ledgers(self, currency):
        """
        :param currency: BTC | ETH, etc
        :return: All bills for the currency, API of
                 GET /api/futures/v3/accounts/<currency>/ledger)
        """
        page = 1
        ret = []
        while True:
            logging.debug('Querying bill history page %d for %s',
                          page, currency)
            time.sleep(1)  # Sleep for one second
            resp = self.future_sdk.get_ledger(currency,
                                              page_from=page,
                                              page_to=page,
                                              limit=100)
            if len(resp) == 0:
                break
            ret.extend(resp)
            page += 1
        return ret

    def completed_orders(self, instruments):
        orders = [self.all_completed_orders_for_instrument(
            instrument_id=instrument_id)
            for instrument_id in instruments]
        return [t for lst in orders for t in lst]

    def all_completed_orders_for_instrument(self, instrument_id):
        page = 1
        ret = []
        while True:
            logging.debug('Querying order history page %d for %s',
                          page, instrument_id)
            time.sleep(1)  # sleep 1 second
            resp = self.future_sdk.get_order_list(
                instrument_id,
                status=7,  # fulfilled and canceled)
                froms=page,
                to=page,
                limit=100
            )['order_info']
            ret.extend(resp)
            page += 1
            if len(resp) == 0:
                break
        return ret


async def _testing_coroutine(api, instrument):
    logging.info('start %s', instrument)
    r = await api.close_long_order('ETH', 1, 1000)
    logging.info('end %s = %s', instrument, r)


def _testing():
    from .logger import init_global_logger
    init_global_logger()
    singleton.loop = asyncio.get_event_loop()
    api = RestApiV3()
    instruments = api.get_all_instrument_ids_blocking('ETH')
    coroutines = [_testing_coroutine(api, instrument)
                  for instrument in instruments]
    singleton.loop.run_until_complete(asyncio.gather(*coroutines))


if __name__ == '__main__':
    _testing()
