import logging
import unittest
from decimal import Decimal
from unittest.mock import ANY, MagicMock, patch

import eventlet

from ok_bot import order_executor, singleton


class MockOrderListerner_cancelAfterSubscribe:
    def subscribe(self, order_id, subscriber):
        print('subscribe')
        eventlet.greenthread.spawn_after_local(
            1, lambda: subscriber.order_cancelled(order_id))

    def unsubscribe(self, order_id, subscriber):
        print('unsubscribed')


class TestOrderExecutor(unittest.TestCase):

    def setUp(self):
        singleton.green_pool = eventlet.GreenPool()
        singleton.rest_api = MagicMock()
        singleton.rest_api.open_long_order.__name__ = 'fake_open_long_order'
        singleton.order_listener = MockOrderListerner_cancelAfterSubscribe()

    def test_order_cancelled(self):
        def _testing_thread():
            executor = order_executor.OrderExecutor('instrument_id',
                                                    amount=1,
                                                    price=100.0,
                                                    timeout_sec=20,
                                                    is_market_order=False,
                                                    logger=logging)
            order_status_future = executor.open_long_position()
            print('open_long_position has been called')
            result = order_status_future.get()
            print(f'result: {result}')
            assert result is order_executor.OPEN_POSITION_STATUS__CANCELLED

        testing_thread = singleton.green_pool.spawn(_testing_thread)
        testing_thread.wait()


if __name__ == '__main__':
    unittest.main()
