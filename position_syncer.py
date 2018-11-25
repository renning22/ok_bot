from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from absl import logging
import asyncio
import time

import constants


class PositionSyncer(object):
    def __init__(self, symbol, api, order_book):
        self.api = api
        self.symbol = "%s/USD" % symbol.upper()
        self.order_book = order_book
        self.executor = ProcessPoolExecutor(max_workers=len(constants.PERIOD_TYPES))

    async def fetch_position(self, period):
        latest_position = self.api.get_position(period)
        logging.info(f"fetched for {period} and got {len(latest_position)} updates")
        for p in latest_position:
            assert period == p['contract_type']
            logging.log("synced position %s %s %d %.2f" %
                        (period, p['side'], p['amount'], p['open_price']))
        self.order_book.update_position(period, latest_position)

    async def read_loop(self):
        while True:
            futures = []
            for period in constants.PERIOD_TYPES:
                futures.append(self.fetch_position(period))
            await asyncio.gather(*futures)
            await asyncio.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)


if __name__ == "__main__":
    from rest_api import OKRest
    syncer = PositionSyncer('btc', OKRest('btc'), None)
    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(syncer.read_loop()))


