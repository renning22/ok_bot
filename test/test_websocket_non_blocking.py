import unittest
from datetime import datetime

import eventlet

import greenlet
from ok_bot import patched_io_modules, singleton

_URL = 'http://www.google.com'
_CRAWL_TIMES = 5
_CRAWL_TEST_TIMEOUT_SEC = 15


class TestWebsocketNonblocking(unittest.TestCase):
    def test_identical(self):
        from ok_bot import patched_io_modules
        requests = eventlet.import_patched('requests')
        assert requests is patched_io_modules.requests

    def test_non_blocking(self):
        def crawl():
            crawled_times = 0
            requests = eventlet.import_patched('requests')
            for _ in range(_CRAWL_TIMES):
                resp = requests.get(_URL)
                print(f'get {resp.status_code} from {_URL}')
                eventlet.sleep(1)
                crawled_times += 1
            print('all crawl finished')
            return crawled_times

        singleton.initialize_objects('ETH')
        # make sure nothing will return from websocket subscription
        singleton.websocket.book_listener = None
        singleton.websocket.start_read_loop()

        crawl_thread = singleton.green_pool.spawn(crawl)
        wait_thread = eventlet.greenthread.spawn(
            lambda: singleton.green_pool.waitall())

        # kill the websocket subscription after 15 seconds
        eventlet.greenthread.spawn_after_local(
            _CRAWL_TEST_TIMEOUT_SEC, lambda: wait_thread.kill())

        # kill after crawled 5 times
        crawl_thread.link(lambda gt: wait_thread.kill())

        begin_time = datetime.now()
        try:
            crawled_times = crawl_thread.wait()
        except greenlet.GreenletExit:
            pass
        end_time = datetime.now()
        assert (end_time - begin_time).seconds <= _CRAWL_TEST_TIMEOUT_SEC,\
            f'begin at {begin_time}, end at {end_time}, should less than 15 ' \
            f'seconds'
        assert crawled_times == _CRAWL_TIMES,\
            f'crawled {crawled_times} expected {_CRAWL_TIMES}'
        assert singleton.websocket.heartbeat_ping >= 1,\
            f'{singleton.websocket.heartbeat_ping}'
        assert singleton.websocket.heartbeat_pong >= 1,\
            f'{singleton.websocket.heartbeat_pong}'


if __name__ == '__main__':
    unittest.main()
