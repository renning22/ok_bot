import asyncio
import pprint
from concurrent.futures import ThreadPoolExecutor
import unittest

import requests
import logging

from ok_bot import singleton

_URL = 'http://www.google.com'
_CRAWL_TIMES = 10
_CRAWL_TEST_TIMEOUT_SEC = 15


class TestWebsocketNonblocking(unittest.TestCase):
    def test_non_blocking(self):
        async def crawl_job():
            crawled_times = 0
            for _ in range(_CRAWL_TIMES):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    resp = await singleton.loop.run_in_executor(
                        executor, requests.get, _URL)
                logging.info('get %s from %s', resp.status_code, _URL)
                await asyncio.sleep(1)
                crawled_times += 1
            logging.info('all crawl finished')
            return crawled_times

        async def sleeper():
            await asyncio.sleep(_CRAWL_TEST_TIMEOUT_SEC)

        async def _testing_coroutine(test_class):
            with self.assertLogs(logging.getLogger(), level='INFO') as cm:
                t = await asyncio.gather(crawl_job(), sleeper())
                crawled_times = t[0]
                log_message = cm.output

            print(pprint.pformat(log_message))
            test_class.assertIn(
                'INFO:root:Sending heartbeat message', log_message)
            test_class.assertIn(
                'INFO:root:Received heartbeat message', log_message)
            test_class.assertEqual(
                crawled_times, _CRAWL_TIMES,
                f'crawled {crawled_times} expected {_CRAWL_TIMES}')
            test_class.assertGreaterEqual(
                singleton.websocket.heartbeat_ping, 1,
                f'{singleton.websocket.heartbeat_ping}')
            test_class.assertGreaterEqual(
                singleton.websocket.heartbeat_pong, 1,
                f'{singleton.websocket.heartbeat_pong}')

        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        # make sure nothing will return from websocket subscription
        singleton.websocket.book_listener = None
        singleton.websocket.start_read_loop()
        singleton.loop.run_until_complete(_testing_coroutine(self))


if __name__ == '__main__':
    from ok_bot.logger import init_global_logger
    init_global_logger()
    unittest.main()
