import unittest
import asyncio
from unittest.mock import Mock

from ok_bot.logger import init_global_logger
from ok_bot.mock import AsyncMock
from ok_bot import singleton


class TestOrderBook(unittest.TestCase):
    def setUp(self):
        init_global_logger(log_to_stderr=False)
        singleton.initialize_objects_with_dev_db('ETH')
        singleton.rest_api = AsyncMock()
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
            for i in range(1, len(ask)):
                self.assertLess(ask[i-1].price, ask[i].price)
            for i in range(1, len(bid)):
                self.assertGreater(bid[i-1].price, bid[i].price)

        singleton.loop.create_task(singleton.websocket.read_loop())
        singleton.loop.run_until_complete(_test())


if __name__ == '__main__':
    unittest.main()
