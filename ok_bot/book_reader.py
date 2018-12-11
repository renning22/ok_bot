import json
import pprint
import traceback
import zlib
from decimal import Decimal
from typing import Dict

import eventlet
from absl import app, logging

from order_book import OrderBook

websocket = eventlet.import_patched('websocket')

OK_WEB_SOCKET_ADDRESS = 'wss://real.okex.com:10440/ws/v1'


class BookReader:
    def __init__(self, green_pool, order_book, trader, currency):
        self.green_pool = green_pool
        self.order_book = order_book
        self.currency = currency
        self.trader = trader

        self.subscribed_channels = {
            f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
            f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
            f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
        }
        logging.info('BookReader initiated')

    def _read_loop_impl(self):
        while True:
            try:
                ws = websocket.create_connection(OK_WEB_SOCKET_ADDRESS)
                for channel in self.subscribed_channels.keys():
                    msg = json.dumps({
                        'event': 'addChannel',
                        'channel': channel
                    })
                    ws.send(msg)
                    logging.info(f'subscribed with {msg}')
                while True:
                    response = BookReader._parse_response(ws.recv())
                    channel = response['channel']
                    if channel not in self.subscribed_channels.keys():
                        continue
                    period = self.subscribed_channels[channel]

                    asks = sorted(response['data']['asks'])
                    bids = sorted(response['data']['bids'], reverse=True)
                    asks_bids = {
                        f'{period}_ask_price': Decimal(str(asks[0][0])),
                        f'{period}_ask_vol': Decimal(str(asks[0][4])),
                        f'{period}_bid_price': Decimal(str(bids[0][0])),
                        f'{period}_bid_vol': Decimal(str(bids[0][4])),
                        f'{period}_ask2_price': Decimal(str(asks[1][0])),
                        f'{period}_ask2_vol': Decimal(str(asks[1][4])),
                        f'{period}_bid2_price': Decimal(str(bids[1][0])),
                        f'{period}_bid2_vol': Decimal(str(bids[1][4])),
                        f'{period}_ask3_price': Decimal(str(asks[2][0])),
                        f'{period}_ask3_vol': Decimal(str(asks[2][4])),
                        f'{period}_bid3_price': Decimal(str(bids[2][0])),
                        f'{period}_bid3_vol': Decimal(str(bids[2][4])),
                    }
                    logging.debug('new tick:\n %s', pprint.pformat(asks_bids))
                    self.order_book.update_book(period, asks_bids)
                    self.trader.new_tick_received(self.order_book)
            except Exception as ex:
                logging.error(
                    'book reader reading loop encountered error:\n %s',
                    traceback.format_exc())

    def read_loop(self):
        self.green_pool.spawn_n(self._read_loop_impl)

    @staticmethod
    def _parse_response(response_bin):
        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompressor.decompress(response_bin) + decompressor.flush()
        try:
            resp = json.loads(inflated)
            assert len(resp) > 0
            return resp[0]
        except Exception as ex:
            logging.error('failed to parse OK web socket result' + ex)
            return {}


def _testing(_):
    logging.info('Testing BookReader')
    order_book = OrderBook()

    class TraderMock:
        def new_tick_received(*argv):
            print('mock trader got new tick')
    pool = eventlet.GreenPool(100)
    reader = BookReader(pool, order_book, TraderMock(), 'btc')

    reader.read_loop()

    def echo(id):
        while True:
            print(f'start echo {id}')
            eventlet.sleep(id)
            print(f'finished echo {id}')
    for i in range(2):
        pool.spawn_n(echo, i + 1)
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
