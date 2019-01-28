import pprint
import unittest
from datetime import datetime

import eventlet
from absl import logging

import greenlet
from ok_bot import patched_io_modules, singleton

_URL = 'http://www.google.com'
_CRAWL_TIMES = 10
_CRAWL_TEST_TIMEOUT_SEC = 15


class TestWebsocketNonblocking(unittest.TestCase):
    def setUp(self):
        logging.get_absl_logger().setLevel(logging.DEBUG)

    def test_identical(self):
        requests = eventlet.import_patched('requests')
        self.assertIs(requests, patched_io_modules.requests)

    def test_non_blocking(self):
        def crawl_job():
            crawled_times = 0
            requests = eventlet.import_patched('requests')
            for _ in range(_CRAWL_TIMES):
                resp = requests.get(_URL)
                logging.info(f'get %s from %s', resp.status_code, _URL)
                eventlet.sleep(1)
                crawled_times += 1
            logging.info('all crawl finished')
            return crawled_times

        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        # make sure nothing will return from websocket subscription
        singleton.websocket.book_listener = None
        singleton.websocket.start_read_loop()

        crawled_job_thread = eventlet.spawn(crawl_job)

        log_message = None
        with self.assertLogs(logging.get_absl_logger(), level='INFO') as cm:
            eventlet.sleep(_CRAWL_TEST_TIMEOUT_SEC)
            log_message = cm.output

        crawled_times = crawled_job_thread.wait()

        print(pprint.pformat(log_message))
        self.assertIn('INFO:absl:Sending heartbeat message', log_message)
        self.assertIn(
            'INFO:absl:Received heartbeat message', log_message)
        self.assertEqual(crawled_times, _CRAWL_TIMES,
                         f'crawled {crawled_times} expected {_CRAWL_TIMES}')
        self.assertGreaterEqual(singleton.websocket.heartbeat_ping, 1,
                                f'{singleton.websocket.heartbeat_ping}')
        self.assertGreaterEqual(singleton.websocket.heartbeat_pong, 1,
                                f'{singleton.websocket.heartbeat_pong}')


if __name__ == '__main__':
    unittest.main()
