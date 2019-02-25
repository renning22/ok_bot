import asyncio
import unittest
from unittest.mock import Mock

from ok_bot import singleton
from ok_bot.logger import init_global_logger
from ok_bot.mock import AsyncMock


class TestOrderBook(unittest.TestCase):
    def setUp(self):
        init_global_logger(log_to_stderr=False)
        singleton.initialize_objects_with_dev_db('ETH')
        singleton.rest_api = Mock()
        singleton.trader = Mock()

    def tearDown(self):
        singleton.db.shutdown(wait=True)
        for task in asyncio.all_tasks(singleton.loop):
            task.cancel()

    def test_market_depth_is_correctly_ordered(self):
        async def _test():
            await singleton.order_book.ready
            instrument = singleton.schema.all_instrument_ids[0]
            market_depth = singleton.order_book.market_depth(instrument)
            ask = market_depth.ask()
            bid = market_depth.bid()
            self.assertEqual(sorted(ask), ask)
            self.assertEqual(sorted(bid, reverse=True), bid)

        singleton.loop.create_task(singleton.websocket.read_loop())
        singleton.loop.run_until_complete(_test())


if __name__ == '__main__':
    unittest.main()
