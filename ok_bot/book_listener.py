from collections import defaultdict
from decimal import Decimal

import eventlet
from absl import app, logging

from . import singleton


class BookListener:
    def __init__(self):
        logging.info('BookListener initiated')
        self.subscribers = defaultdict(set)

    def subscribe(self, instrument_id, responder):
        '''
        :param instrument_id:
        :param responder: responder need to implement
                      tick_received(instrument_id,
                                    ask_prices,
                                    ask_vols,
                                    bid_prices,
                                    bid_vols,
                                    timestamp)
        :return: None
        '''
        if not hasattr(responder, 'tick_received'):
            raise Exception(
                f"{type(responder)} doesn't have tick_received method")
        self.subscribers[instrument_id].add(responder)

    def unsubscribe(self, instrument_id, responder):
        self.subscribers[instrument_id].discard(responder)
        if len(self.subscribers[instrument_id]) == 0:
            del self.subscribers[instrument_id]

    def received_futures_depth5(self,
                                asks,
                                bids,
                                instrument_id,
                                timestamp):
        asks = sorted(asks)
        bids = sorted(bids, reverse=True)
        ask_prices = [Decimal(str(i[0])) for i in asks]
        ask_vols = [int(i[1]) for i in asks]
        bid_prices = [Decimal(str(i[0])) for i in bids]
        bid_vols = [int(i[1]) for i in bids]
        for responder in self.subscribers[instrument_id]:
            responder.tick_received(instrument_id,
                                    ask_prices,
                                    ask_vols,
                                    bid_prices,
                                    bid_vols,
                                    timestamp)


def _testing(_):
    singleton.initialize_objects_with_mock_trader('ETH')
    singleton.websocket.start_read_loop()
    singleton.green_pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
