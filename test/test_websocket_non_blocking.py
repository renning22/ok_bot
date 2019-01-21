import unittest
import ok_bot.singleton as singleton
import eventlet
import greenlet
from datetime import datetime


class TestWebsocketNonblocking(unittest.TestCase):
    def test_non_blocking(self):
        CRAWL_TIMES = 5
        crawled_times = 0

        def crawl(url, times):
            nonlocal crawled_times
            requests = eventlet.import_patched('requests')
            for _ in range(times):
                resp = requests.get(url)
                print(f'get {resp.status_code} from {url}')
                eventlet.sleep(1)
            print('all crawl finished')
            crawled_times = times

        singleton.initialize_objects('ETH')
        # make sure nothing will return from websocket subscription
        singleton.websocket.book_listener = None
        singleton.websocket.start_read_loop()

        singleton.green_pool.spawn(crawl,
                                   'http://www.google.com',
                                   CRAWL_TIMES)
        wait_thread = eventlet.greenthread.spawn(
            lambda : singleton.green_pool.waitall())
        # kill the websocket subscription after 15 seconds
        eventlet.greenthread.spawn_after_local(
            15, lambda: wait_thread.kill())
        begin_time = datetime.now()
        try:
            wait_thread.wait()
        except greenlet.GreenletExit:
            pass
        end_time = datetime.now()
        assert 14 <= (end_time - begin_time).seconds <= 16,\
            f'begin at {begin_time}, end at {end_time}, should be 15 ' \
            f'seconds in between'
        assert crawled_times == CRAWL_TIMES,\
            f'crawled {crawled_times} expected {CRAWL_TIMES}'
        assert singleton.websocket.heartbeat_ping > 2
        assert singleton.websocket.heartbeat_pong > 2


if __name__ == '__main__':
    unittest.main()
