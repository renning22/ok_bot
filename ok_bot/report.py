import asyncio
import concurrent
import logging
import pprint
from collections import namedtuple

from . import constants, singleton


class Report:
    def __init__(self, transaction_id):
        self.transaction_id = transaction_id
        self.slow_open_order_id = None
        self.slow_close_order_id = None
        self.fast_open_order_id = None
        self.fast_close_order_id = None


def _testing():
    pass


if __name__ == '__main__':
    _testing()
