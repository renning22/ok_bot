import asyncio
import datetime
import logging
import pprint
import time
from unittest.mock import MagicMock

import numpy as np


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MockTrader:
    def __init__(self, *argv, **kwargv):
        self.on_going_arbitrage_count = 0

    def new_tick_received(self,
                          instrument_id,
                          ask_prices,
                          ask_vols,
                          bid_prices,
                          bid_vols):
        logging.log_every_n_seconds(
            logging.INFO,
            'mock trader got new tick: %s, best ask: %.3f@%d,'
            'best bid: %.3f@%d, on_going_arbitrage_count: %s',
            10,
            instrument_id,
            ask_prices[0],
            ask_vols[0],
            bid_prices[0],
            bid_vols[0],
            self.on_going_arbitrage_count)


class MockOrderBook:
    def contains_gap_hisotry(self, *args):
        return True

    def historical_mean_spread(self, *args):
        return 0

    def current_spread(self, *args):
        return -500

    def ask_price(self, *args):
        return 1000

    def bid_price(self, *args):
        return 2000

    def ask_volume(self, *args):
        return 100

    def bid_volume(self, *args):
        return 200

    @property
    def row_num(self):
        return 100000

    @property
    def time_window(self):
        return np.timedelta64(60 * 60, 's')

    def update_book(self, market, data):
        logging.info(
            'MockOrderBook.update_book:\n %s\n %s', market, data)


class MockBookListener:
    def received_futures_depth5(self, *argv):
        logging.info('MockBookListener:\n%s', pprint.pformat(argv))


class MockBookListerner_constantPriceGenerator:
    def __init__(self, price, vol):
        self._price = price
        self._vol = vol
        self._subscribers = {}
        self._running = True
        self._broadcast_loop = asyncio.get_event_loop().create_task(
            self._kick_off_broadcast_loop())

    async def shutdown_broadcast_loop(self):
        assert self._broadcast_loop is not None
        self._running = False
        logging.info('shutting down broadcast loop')
        await self._broadcast_loop
        logging.info('broadcast_loop has been shut down')

    def subscribe(self, instrument_id, subscriber):
        logging.info('subscribed %s', instrument_id)
        self._subscribers[instrument_id] = (
            lambda: subscriber.tick_received(
                instrument_id=instrument_id,
                ask_prices=[self._price],
                ask_vols=[self._vol],
                bid_prices=[self._price],
                bid_vols=[self._vol],
                timestamp=datetime.datetime.now().isoformat())
        )

    def unsubscribe(self, instrument_id, subscriber):
        logging.info('unsubscribe %s', instrument_id)
        del self._subscribers[instrument_id]

    async def _kick_off_broadcast_loop(self):
        while self._running:
            logging.info('_kick_off_broadcast_loops')
            for subscriber, callback in self._subscribers.items():
                logging.info('sending tick_received to %s', subscriber)
                callback()
            await asyncio.sleep(1)
