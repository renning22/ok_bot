import asyncio
from collections import defaultdict

from absl import app, logging

from . import constants, singleton


class OrderListener:
    def __init__(self):
        logging.info('OrderListener initiated')
        self._subscribers = defaultdict(set)

        # There is no guarantee Websocket order notification always comes
        # after REST API http responses (for the same order). The buffer makes
        # sure no Websocket order notification is missed for the subscriber.
        self._buffer = defaultdict(list)

    def subscribe(self, order_id, responder):
        """
        :param order_id:
        :param responder: responder needs to implement:
                      order_pending(order_id)
                      order_cancelled(order_id)
                      order_fulfilled(order_id,
                                      size,
                                      filled_qty,
                                      fee,
                                      price,
                                      price_avg)
                      order_partially_filled(order_id,
                                             size,
                                             filled_qty)
        :return: None
        """
        order_id = int(order_id)
        assert hasattr(responder, 'order_pending')
        assert hasattr(responder, 'order_cancelled')
        assert hasattr(responder, 'order_fulfilled')
        assert hasattr(responder, 'order_partially_filled')
        self._subscribers[order_id].add(responder)
        self._dispatch_buffer(order_id)

    def unsubscribe(self, order_id, responder):
        self._subscribers[order_id].discard(responder)
        if len(self._subscribers[order_id]) == 0:
            del self._subscribers[order_id]

    def received_futures_order(self,
                               leverage,
                               size,
                               filled_qty,
                               price,
                               fee,
                               contract_val,
                               price_avg,
                               type,
                               instrument_id,
                               order_id,
                               timestamp,
                               status):
        order_id = int(order_id)
        if status == constants.ORDER_STATUS_CODE__CANCELLED:
            self._buffer[order_id].append(
                lambda trader: trader.order_cancelled(order_id)
            )
        elif status == constants.ORDER_STATUS_CODE__PENDING:
            self._buffer[order_id].append(
                lambda trader: trader.order_pending(order_id),
            )
        elif status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
            self._buffer[order_id].append(
                lambda trader: trader.order_partially_filled(order_id,
                                                             size,
                                                             filled_qty)
            )
        elif status == constants.ORDER_STATUS_CODE__FULFILLED:
            self._buffer[order_id].append(
                lambda trader: trader.order_fulfilled(order_id,
                                                      size,
                                                      filled_qty,
                                                      fee,
                                                      price,
                                                      price_avg)
            )
        else:
            raise Exception(f'unknown order update message type: {status}')

        self._dispatch_buffer(order_id)

    def _dispatch_buffer(self, order_id):
        if len(self._subscribers[order_id]) == 0:
            return

        for func in self._buffer[order_id]:
            for responder in self._subscribers[order_id]:
                func(responder)
        self._buffer[order_id].clear()


class MockTrader:
    def order_pending(self, order_id):
        logging.info('order_pending: %s', order_id)

    def order_cancelled(self, order_id):
        logging.info('order_cancelled: %s', order_id)

    def order_fulfilled(self,
                        order_id,
                        size,
                        filled_qty,
                        fee,
                        price,
                        price_avg):
        logging.info('order_fulfilled: %s', order_id)

    def order_partially_filled(self,
                               order_id,
                               size,
                               filled_qty,
                               price_avg):
        logging.info('order_partially_filled: %s', order_id)


async def _testing_coroutine(instrument_id):
    await singleton.websocket.ready
    order_id, error_code = await singleton.rest_api.open_long_order(
        instrument_id, amount=1, price=50)
    logging.info('order has been placed order_id: %s', order_id)

    # Leave time to manually cancel order from website.
    await asyncio.sleep(5)

    trader = MockTrader()
    singleton.order_listener.subscribe(order_id, trader)


def _testing(_):
    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    logging.info('instruments: %s',
                 singleton.schema.all_instrument_ids)
    instrument_id = singleton.schema.all_instrument_ids[0]
    asyncio.ensure_future(_testing_coroutine(
        instrument_id=instrument_id))
    singleton.start_loop()


if __name__ == '__main__':
    app.run(_testing)
