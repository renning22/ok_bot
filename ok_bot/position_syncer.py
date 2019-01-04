import pprint
import traceback

import eventlet
from absl import app, logging

from . import constants


class PositionSyncer:
    def __init__(self, green_pool, symbol, api, order_book):
        self.green_pool = green_pool
        self.api = api
        self.symbol = f'{symbol.upper()}/USD'
        self.order_book = order_book

    def fetch_position(self, period):
        logging.info(f'start syncing for {period}')
        latest_position = self.api.get_position(period)
        logging.info(
            f'fetched for {period} and got {len(latest_position)} updates')
        for p in latest_position:
            assert period == p['contract_type']
            logging.info(
                f'synced position:\n%s', pprint.pformat(p))
        if len(latest_position) > 0:
            self.order_book.update_position(period, latest_position)
        logging.info(f'done syncing for {period}')

    def read_loop(self):
        while True:
            try:
                for period in constants.PERIOD_TYPES:
                    self.green_pool.spawn_n(self.fetch_position, period)
                eventlet.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)
            except:
                logging.error(
                    'get position read_loop encountered error:\n%s', traceback.format_exc())


def _testing(_):
    from .order_book import MockOrderBook
    rest_api = eventlet.import_patched('ok_bot.rest_api')
    pool = eventlet.GreenPool(10)
    syncer = PositionSyncer(
        pool, 'ETH', rest_api.RestApi('ETH'), MockOrderBook())
    pool.spawn_n(syncer.read_loop)
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
