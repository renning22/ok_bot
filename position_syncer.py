from concurrent.futures import ProcessPoolExecutor
from absl import logging
import functools
import asyncio
import time

import constants


def fetch_position_sub(period):
    print(f"mock fetching for {period}")
    time.sleep(3)

class PositionSyncer(object):
    def __init__(self, symbol, api, order_book):
        self.api = api
        self.symbol = "%s/USD" % symbol.upper()
        self.order_book = order_book
        self.executor = ProcessPoolExecutor(max_workers=len(constants.PERIOD_TYPES))

    def fetch_position(self, period):
        print(f"fetching for {period}")
        latest_position = self.api.get_position(period)

        for p in latest_position:
            assert period == p['contract_type']
            logging.log("synced position %s %s %d %.2f" %
                        (period, p['side'], p['amount'], p['open_price']))
        self.order_book.update_position(period, latest_position)
        time.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)



    async def read_loop(self):
        while True:
            for period in constants.PERIOD_TYPES:
                await asyncio.get_event_loop().run_in_executor(self.executor,
                                                               fetch_position_sub, period)


if __name__ == "__main__":
    syncer = PositionSyncer('btc', None, None)
    asyncio.ensure_future(syncer.read_loop())
    asyncio.get_event_loop().run_forever()


