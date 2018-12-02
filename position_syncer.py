from absl import logging
import traceback
import eventlet

import constants


class PositionSyncer:
    def __init__(self, green_pool, symbol, api, order_book):
        self.green_pool = green_pool
        self.api = api
        self.symbol = f'{symbol.upper()}/USD'
        self.order_book = order_book

    def fetch_position(self, period):
        logging.info(f'start syncing for {period}')
        latest_position = self.api.get_position(period)
        logging.info(f'fetched for {period} and got {len(latest_position)} updates')
        for p in latest_position:
            assert period == p['contract_type']
            logging.log('synced position {period} {p["side"]} p["amount"] %.2f' % p['open_price'])
        if len(latest_position) > 0:
            self.order_book.update_position(period, latest_position)
        logging.info(f'done syncing for {period}')

    def read_loop(self):
        while True:
            try:
                for period in constants.PERIOD_TYPES:
                    self.green_pool.spawn_n(self.fetch_position, period)
                eventlet.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)
            except Exception as ex:
                logging.error(f'get position read_loop encountered error:{str(ex)}\n'
                              f'{traceback.format_exc()}')


if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    import eventlet
    rest_api = eventlet.import_patched('rest_api')
    from rest_api import OKRest
    pool = eventlet.GreenPool(10)
    syncer = PositionSyncer(pool, 'btc', OKRest('btc'), None)
    syncer.read_loop()


