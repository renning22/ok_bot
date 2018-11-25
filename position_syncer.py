from absl import logging
import asyncio
import traceback

import constants


class PositionSyncer:
    def __init__(self, symbol, api, order_book):
        self.api = api
        self.symbol = "%s/USD" % symbol.upper()
        self.order_book = order_book

    async def fetch_position(self, period):
        latest_position = self.api.get_position(period)
        logging.info(f"fetched for {period} and got {len(latest_position)} updates")
        for p in latest_position:
            assert period == p['contract_type']
            logging.log("synced position %s %s %d %.2f" %
                        (period, p['side'], p['amount'], p['open_price']))
        self.order_book.update_position(period, latest_position)

    async def read_loop_impl(self):
        while True:
            futures = [self.fetch_position(period) for period in constants.PERIOD_TYPES]
            await asyncio.gather(*futures)  # block for each batch. One batch contains all periods
            await asyncio.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)

    async def read_loop(self):
        while True:
            try:
                await self.read_loop_impl()
            except Exception as ex:
                logging.error("get position read_loop encountered error:%s\n%s" %
                              (str(ex), traceback.format_exc()))


if __name__ == "__main__":
    from rest_api import OKRest
    syncer = PositionSyncer('btc', OKRest('btc'), None)
    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(syncer.read_loop()))


