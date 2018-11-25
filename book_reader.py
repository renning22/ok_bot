import asyncio
from typing import Dict
from decimal import Decimal
import websockets
import zlib
import json
import traceback
from absl import logging, app
from order_book import OrderBook

OK_WEB_SOCKET_ADDRESS = 'wss://real.okex.com:10440/ws/v1'


class BookReader(object):
    SUBSCRIBED_CHANNELS: Dict[str, str]

    def __init__(self, order_book, trader, currency):
        self.order_book = order_book
        self.currency = currency
        self.trader = trader

        self.SUBSCRIBED_CHANNELS = {
            f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
            f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
            f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
        }
        logging.info("BookReader initiated")

    async def read_loop(self):
        while True:
            try:
                await self.read_loop_impl()
            except Exception as ex:
                logging.error("read_loop encountered error: %s\n%s" %
                              (str(ex), traceback.format_exc()))

    async def read_loop_impl(self):
        async with websockets.connect(OK_WEB_SOCKET_ADDRESS) as ws:
            for channel in self.SUBSCRIBED_CHANNELS.keys():
                msg = json.dumps({
                    'event': 'addChannel',
                    'channel': channel
                })
                await ws.send(msg)
                logging.info(f"subscribed with {msg}")
            while True:
                response = self.parse_response(await ws.recv())
                channel = response['channel']
                if channel not in self.SUBSCRIBED_CHANNELS.keys():
                    continue
                period = self.SUBSCRIBED_CHANNELS[channel]

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

                self.order_book.update_book(period, asks_bids)
                self.trader.new_tick_received(self.order_book)
                #asyncio.ensure_future(update(period, asks_bids))
                #print(asks_bids)

    def parse_response(self, response_bin):
        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompressor.decompress(response_bin) + decompressor.flush()
        try:
            resp = json.loads(inflated)
            assert len(resp) > 0
            return resp[0]
        except Exception as ex:
            logging.error("failed to parse OK web socket result" + ex)
            return {}


def testing(_):
    logging.info("Testing BookReader")
    order_book = OrderBook()
    reader = BookReader(order_book, 'btc')
    asyncio.ensure_future(reader.read_loop())
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    app.run(testing)
