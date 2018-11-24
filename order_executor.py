from concurrent.futures import ProcessPoolExecutor
import asyncio
import functools
from absl import logging


class OrderExecutor(object):
    def __init__(self, api):
        self.api = api
        self.open_order_executors = {
            'this_week': ProcessPoolExecutor(max_workers=1),
            'next_week': ProcessPoolExecutor(max_workers=1),
            'quarter': ProcessPoolExecutor(max_workers=1)
        }
        self.close_order_executors = {
            'this_week': ProcessPoolExecutor(max_workers=1),
            'next_week': ProcessPoolExecutor(max_workers=1),
            'quarter': ProcessPoolExecutor(max_workers=1)
        }

    def long(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.open_order_executors[period],
                                                 functools.partial(self.api.open_long_order,
                                                                   period, volume, price))
        logging.info(f"scheduled to long {period} @ {price} vol:{volume}")

    def short(self, period, price, volume):
        asyncio.get_event_loop().run_in_executor(self.open_order_executors[period],
                                                 functools.partial(self.api.open_short_order,
                                                                   period, volume, price))
        logging.info(f"scheduled to short {period} @ {price} vol:{volume}")
