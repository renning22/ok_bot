import traceback
from datetime import datetime

import eventlet
import numpy as np
from absl import logging

from . import constants


class OrderCanceller:
    def __init__(self, green_pool, api):
        self.green_pool = green_pool
        self.api = api

    def read_cancel_loop(self):
        while True:
            try:
                for period in constants.PERIOD_TYPES:
                    self.green_pool.spawn_n(self.cancel_pending_orders,
                                            period,
                                            constants.PENDING_ORDER_TTL_IN_SECOND)
                eventlet.sleep(constants.ORDER_CANCELLER_SLEEP_IN_SECOND)
            except Exception as ex:
                logging.error('order canceller loop encountered error:\n %s',
                              traceback.format_exc())

    def cancel_pending_orders(self, period, ttl):
        logging.info(f'fetching pending orders for {period}')
        now = np.datetime64(datetime.utcnow())
        orders = self.api.fetch_open_orders(period)
        orders_to_be_cancelled = []
        logging.info(f'found {len(orders)} pending orders for {period}')
        for order in orders:
            order_id = order['id']
            if now - np.datetime64(order['datetime']) > np.timedelta64(ttl, 's'):
                self.api.notify_slack(order)
                orders_to_be_cancelled.append(order_id)
        for order_list in OrderCanceller._every_five(orders_to_be_cancelled):
            self.api.cancel_order(order_list, period)

    @staticmethod
    def _every_five(l):
        if not len(l):
            return
        yield l[:5]
        yield from OrderCanceller._every_five(l[5:])


def main(argv):
    from rest_api import RestApi
    canceller = OrderCanceller(eventlet.GreenPool(), RestApi('btc'))
    canceller.read_cancel_loop()


if __name__ == '__main__':
    from absl import app
    app.run(main)
