from concurrent.futures import ThreadPoolExecutor
import asyncio
from absl import logging
import time


class OrderExecutor:
    def __init__(self, api):
        self.api = api
        self.open_order_executors = {
         'this_week': ThreadPoolExecutor(max_workers=1),
         'next_week': ThreadPoolExecutor(max_workers=1),
         'quarter': ThreadPoolExecutor(max_workers=1)
        }
        self.close_order_executors = {
         'this_week': ThreadPoolExecutor(max_workers=1),
         'next_week': ThreadPoolExecutor(max_workers=1),
         'quarter': ThreadPoolExecutor(max_workers=1)
        }

    def open_long(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.open_order_executors[period],
                                                 self.api.open_long_order,
                                                 period, volume, price)
        logging.info(f'scheduled to long {period} @ {price} vol:{volume}')

    def close_long(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.close_order_executors[period],
                                                 self.api.close_long_order,
                                                 period, volume, price)
        logging.info(f'scheduled to close long {period} @ {price} vol:{volume}')

    def open_short(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.open_order_executors[period],
                                                 self.api.open_short_order,
                                                 period, volume, price)
        logging.info(f'scheduled to short {period} @ {price} vol:{volume}')

    def close_short(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.close_order_executors[period],
                                                 self.api.close_short_order,
                                                 period, volume, price)
        logging.info(f'scheduled to close short {period} @ {price} vol:{volume}')

    def _open_short_order_stub(self, *args):  # for dev only
        time.sleep(2)
        logging.info('open short executed for ' + str(args))

    def _open_long_order_stub(self, *args):  # for dev only
        time.sleep(4)
        logging.info('open long executed for ' + str(args))