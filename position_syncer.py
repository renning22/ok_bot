from absl import logging
import traceback
import eventlet


import constants


class PositionSyncer:
    def __init__(self, symbol, api, order_book):
        self.api = api
        self.symbol = f'{symbol.upper()}/USD'
        self.order_book = order_book

    def fetch_position(self, period):
        print(f'start syncing for {period}')
        latest_position = self.api.get_position(period)
        logging.info(f'fetched for {period} and got {len(latest_position)} updates')
        for p in latest_position:
            assert period == p['contract_type']
            logging.log('synced position {period} {p["side"]} p["amount"] %.2f' % p['open_price'])
        if len(latest_position) > 0:
            self.order_book.update_position(period, latest_position)
        print(f'done syncing for {period}')

    def read_loop_impl(self):
        pool = eventlet.GreenPool(size=3)
        pool.imap(self.fetch_position, constants.PERIOD_TYPES)
        while True:
            for period in constants.PERIOD_TYPES:
                pool.spawn_n(self.fetch_position, period)
            eventlet.sleep(constants.POSITION_SYNC_SLEEP_IN_SECOND)
            pool.waitall()

    def read_loop(self):
        while True:
            try:
                self.read_loop_impl()
            except Exception as ex:
                logging.error(f'get position read_loop encountered error:{str(ex)}\n'
                              f'{traceback.format_exc()}')


if __name__ == '__main__':
    rest_api = eventlet.import_patched('rest_api')
    OKRest = rest_api.OKRest
    from rest_api import OKRest
    syncer = PositionSyncer('btc', OKRest('btc'), None)
    syncer.read_loop()


