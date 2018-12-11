import eventlet
from absl import app, logging


class OrderExecutor:
    def __init__(self, api):
        self.api = api
        self.executor_pool = eventlet.GreenPool(2)
        self.api._open_short_order_stub = self._open_short_order_stub
        self.api._open_long_order_stub = self._open_long_order_stub

    def open_arbitrage_position(self, long_period, long_price,
                                short_period, short_price, volume):
        self.executor_pool.spawn_n(
            self.api.open_long_order, long_period, volume, long_price)
        self.executor_pool.spawn_n(
            self.api.open_short_order, short_period, volume, short_price)
        self.executor_pool.waitall()  # block for order submission
        logging.info('finished waiting')

    def close_arbitrage_position(self, long_period, sell_price,
                                 short_period, buy_price, volume):
        self.executor_pool.spawn_n(
            self.api.close_long_order, long_period, volume, sell_price)
        self.executor_pool.spawn_n(
            self.api.close_short_order, short_period, volume, buy_price)
        self.executor_pool.waitall()  # block for order submission

    def _open_short_order_stub(self, *args):  # for dev only
        logging.info('opening short executed for ' + str(args))
        eventlet.sleep(2)
        logging.info('opened short executed for ' + str(args))

    def _open_long_order_stub(self, *args):  # for dev only
        logging.info('opening long executed for ' + str(args))
        eventlet.sleep(4)
        logging.info('opened long executed for ' + str(args))


if __name__ == '__main__':
    rest_api = eventlet.import_patched('rest_api')

    def testing(_):
        executor = OrderExecutor(rest_api.OKRest('eth'))
        executor.open_arbitrage_position(
            'this_week', 30, 'next_week', 300, 1)
        logging.info('end')
    app.run(testing)
