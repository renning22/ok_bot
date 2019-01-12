import collections

from absl import app, logging

from . import singleton
from .order_executor import OrderExecutor, OPEN_POSITION_STATUS__SUCCEEDED
from .constants import LONG, SHORT, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND, FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND

ArbitrageLeg = collections.namedtuple('ArbitrageLeg',
                                      ['instrument_id', 'side', 'volume', 'price'])


class ArbitrageTransaction:
    def __init__(self,
                 arbitrage_id,
                 slow_leg,
                 fast_leg):
        self.id = arbitrage_id
        self.slow_leg = slow_leg
        self.fast_leg = fast_leg

    def open_position(self, leg, timeout_in_sec):
        assert leg.side in [LONG, SHORT]
        order_executor = OrderExecutor()
        if leg.side == LONG:
            return order_executor.open_long_position(leg.instrument_id,
                                                     leg.volume,
                                                     leg.price,
                                                     timeout_in_sec)
        else:
            # short order
            return order_executor.open_short_position(leg.instrument_id,
                                                      leg.volume,
                                                      leg.price,
                                                      timeout_in_sec)

    def process(self):
        logging.info('starting arbitrage transaction on '
                     f'slow:{self.slow_leg} and fast{self.fast_leg}')

        slow_leg_order_status =\
            self.open_position(self.slow_leg, SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND)
        if slow_leg_order_status != OPEN_POSITION_STATUS__SUCCEEDED:

            return
        # # slow_side_order_future is type of Eventlet.event.
        # slow_side_order_future = OpenOrder(rest_api, order_reader, instrument_id, price)
        # result = slow_side_order_future.wait_until_fulfilled(timeout='1s')
        # if result == 'no':
        #     return
        #
        # fast_side_order_future = OpenOrder(rest_api, order_reader, instrument_id, price)
        # result = fast_side_order_future.wait_until_fulfilled(timeout='1s')
        # if result == 'yes':
        #     return
        #
        # # State: slow-side leg + fast-side failed
        # # To close slow-side position.
        # while True:
        #     close_order_future = CloseOrder(..., market=True)
        #     result = close_order_future.wait_until_fulfilled(timeout='30s')
        #     if result == 'yes':
        #         return
        #
        # # Check and close
        # close_condition_future = PriceObserver(
        #     price_listener, target_price1, target_price2)
        # result = close_condition_future.wait_until_price_matched(timeout='30m')
        # if result == 'yes':
        # # Transaction succeeded logging
        # else:
        # # Transaction timeout logging
        # order1 = CloseOrder('slow_side', market=True)
        # order2 = CloseOrder('fast_side', market=True)
        # wait(order1, order2)


def _testing(_):
    singleton.initialize_objects('ETH')
    short_instrument = singleton.schema.all_instrument_ids[0]
    long_instrument = singleton.schema.all_instrument_ids[1]

    transaction = ArbitrageTransaction(
        'test-transaction',
        slow_leg=ArbitrageLeg(instrument_id=short_instrument,
                              side=SHORT,
                              volume=1,
                              price=500),
        fast_leg=ArbitrageLeg(instrument_id=long_instrument,
                              side=LONG,
                              volume=1,
                              price=10),
    )
    transaction.process()

if __name__ == '__main__':
    app.run(_testing)