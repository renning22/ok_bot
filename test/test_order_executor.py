import unittest
from unittest.mock import MagicMock

import eventlet
from absl import logging

from ok_bot import constants, db, order_executor, singleton

_FAKE_ORDER_ID = 12345
_SIZE = 1
_PRICE = 100.0


class MockOrderListerner_cancelAfterSubscribe:
    def __init__(self):
        self.last_subscribed_order_id = None

    def subscribe(self, order_id, subscriber):
        self.last_subscribed_order_id = order_id
        eventlet.greenthread.spawn_after_local(
            1, lambda: subscriber.order_cancelled(order_id))

    def unsubscribe(self, order_id, subscriber):
        pass


class TestOrderExecutor(unittest.TestCase):

    def setUp(self):
        logging.get_absl_logger().setLevel(logging.DEBUG)
        singleton.db = db.DevDb()
        singleton.green_pool = eventlet.GreenPool()
        singleton.rest_api = MagicMock()
        singleton.rest_api.open_long_order.__name__ = 'fake_open_long_order'
        singleton.rest_api.open_long_order.return_value = (
            _FAKE_ORDER_ID, None)
        singleton.rest_api.get_order_info.return_value = {
            'status': constants.ORDER_STATUS_CODE__CANCELLED,
            'order_id': _FAKE_ORDER_ID,
            'size': _SIZE,
            'filled_qty': 0,
            'price': _PRICE,
            'price_avg': 0,
            'fee': 0,
            'type': 1,
            'timestamp': '2019-01-27 15:38:24',
        }
        singleton.order_listener = MockOrderListerner_cancelAfterSubscribe()

    def test_order_cancelled(self):

        def _testing_thread():
            executor = order_executor.OrderExecutor(
                instrument_id='instrument_id',
                amount=_SIZE,
                price=_PRICE,
                timeout_sec=20,
                is_market_order=False,
                logger=logging)
            order_status_future = executor.open_long_position()
            logging.info('open_long_position has been called')
            result = order_status_future.get()
            logging.info('result: %s', result)
            return result

        testing_thread = singleton.green_pool.spawn(_testing_thread)
        result = testing_thread.wait()

        self.assertEqual(
            singleton.order_listener.last_subscribed_order_id, _FAKE_ORDER_ID)
        self.assertIs(result, order_executor.OPEN_POSITION_STATUS__CANCELLED)


if __name__ == '__main__':
    unittest.main()
