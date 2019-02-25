"""Websocket API V3"""
import asyncio
import base64
import concurrent
import hmac
import json
import logging
import pprint
import zlib
from concurrent.futures import CancelledError

import websockets

from . import api_v3_key_reader, server_time, singleton
from .quant import Quant

OK_WEBSOCKET_ADDRESS = 'wss://real.okex.com:10442/ws/v3'


def _create_login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'
    mac = hmac.new(bytes(secret_key, encoding='utf-8'),
                   bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {'op': 'login', 'args': [
        api_key, passphrase, timestamp, sign.decode('utf-8')]}
    login_str = json.dumps(login_param)
    return login_str


def _inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class WebsocketApi:
    def __init__(self,
                 schema,
                 book_listener=None,
                 order_listener=None):
        self._schema = schema
        self.book_listener = book_listener
        self.order_listener = order_listener
        self._currency = schema.currency
        self._conn = None
        self._subscribed_channels = None
        self._acked_channels = set()
        self.ready = singleton.loop.create_future()
        self.heartbeat_ping = 0  # keeping track of number of heartbeat sent
        self.heartbeat_pong = 0  # keeping track of number of heartbeat received

    async def _recv(self, timeout_sec):
        try:
            res = await asyncio.wait_for(self._conn.recv(), timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            return None
        else:
            return res

    async def _create_and_login(self):
        timestamp = str(server_time.get_server_timestamp())
        login_str = _create_login_params(
            str(timestamp),
            api_v3_key_reader.API_KEY,
            api_v3_key_reader.PASS_PHRASE,
            api_v3_key_reader.KEY_SECRET)

        await self._conn.send(login_str)

        # Consume login response.
        login_res = await self._recv(timeout_sec=10)
        login_res_text = _inflate(login_res).decode()
        logging.info('websocket login response:\n%s',
                     pprint.pformat(login_res_text))

    async def _subscribe(self, channels):
        sub_param = {'op': 'subscribe', 'args': list(channels)}
        sub_str = json.dumps(sub_param)
        await self._conn.send(sub_str)

    async def _subscribe_all_interested(self):
        interested_channels = []
        if self.book_listener is not None:
            interested_channels.append('futures/depth5')
        if self.order_listener is not None:
            interested_channels.append('futures/order')

        self._subscribed_channels = set(
            [f'{channel}:{id}'
             for id in self._schema.all_instrument_ids
             for channel in interested_channels])
        await self._subscribe(self._subscribed_channels)

    async def _receive_and_dispatch(self):
        # 连接限制
        # 连接限制：1次/s
        #
        # 订阅限制：每小时240次
        #
        # 连接上ws后如果一直没有数据返回，30s 后自动断开链接， 建议用户进行以下操作:
        #
        # 1，每次接收到消息后，用户设置一个定时器 ，定时N秒。
        #
        # 2，如果定时器被触发（N 秒内没有收到新消息），发送字符串 'ping'。
        #
        # 3，期待一个文字字符串'pong'作为回应。如果在 N秒内未收到，请发出错误或重新连接。
        #
        # 出现网络问题会自动断开连接
        res_bin = await self._recv(timeout_sec=10)
        if res_bin is None:
            logging.info('Sending heartbeat message')
            await self._conn.send('ping')
            self.heartbeat_ping += 1
            return

        res_text = _inflate(res_bin).decode()
        if res_text == 'pong':
            logging.info('Received heartbeat message')
            self.heartbeat_pong += 1
            return

        res = json.loads(res_text)

        # This is event message that ACKs to subscriptions.
        if 'event' in res:
            assert res['event'] == 'subscribe'
            self._acked_channels.add(res['channel'])
            logging.info('Confirmed "%s" is subscribed', res['channel'])
            if self._acked_channels == self._subscribed_channels:
                logging.info('All channel subscriptions got ACK')
                if not self.ready.done():
                    self.ready.set_result(True)
            return

        # Otherwise it's data message.
        if ('table' not in res) or ('data' not in res):
            raise Exception(
                f'unrecognized websocket response:\n{pprint.pformat(res)}')

        table = res['table']
        data_list = res['data']

        if table == 'futures/depth5':
            for data in data_list:
                self._received_futures_depth5(**data)
        elif table == 'futures/order':
            for data in data_list:
                self._received_futures_order(**data)
        else:
            raise Exception(
                f'received unsubscribed event:\n{pprint.pformat(res)}')

    def _received_futures_depth5(self,
                                 asks,
                                 bids,
                                 instrument_id,
                                 timestamp):
        """
             {'asks': [[3635.45, 1, 0, 1],
                      [3635.46, 51, 0, 2],
                      [3635.49, 3, 0, 1],
                      [3635.72, 41, 0, 2],
                      [3635.8, 3, 0, 1]],
             'bids': [[3635.11, 1, 0, 1],
                      [3634.87, 3, 0, 1],
                      [3634.81, 26, 0, 1],
                      [3634.8, 16, 0, 1],
                      [3634.47, 16, 0, 1]],
             'instrument_id': 'BTC-USD-190329',
             'timestamp': '2018-12-25T12:14:50.085Z'}

             asks	String	卖方深度
             bids	String	买方深度
             timestamp	String	时间戳
             instrument_id	String	合约ID BTC-USD-170310
             [411.8,10,8,4][double ,int ,int ,int] 411.8为深度价格，10为此价格数量，8为此价格的爆仓单数量，4为此深度由几笔订单组成
        """
        self.book_listener.received_futures_depth5(
            asks, bids, instrument_id, timestamp)

    def _received_futures_order(self,
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
        """
            instrument_id	String	合约ID，如BTC-USDT-180213
            size	String	数量
            timestamp	String	委托时间
            filled_qty	String	成交数量
            fee	String	手续费
            order_id	String	订单ID
            price	String	订单价格
            price_avg	String	平均价格
            status	String	订单状态(-1.撤单成功；0:等待成交 1:部分成交 2:全部成交 6：未完成（等待成交+部分成交）7：已完成（撤单成功+全部成交））
            type	String	订单类型(1:开多 2:开空 3:平多 4:平空)
            instrument_id_val	String	合约面值
            leverage	String	杠杆倍数 value:10/20 默认10
        """
        self.order_listener.received_futures_order(
            int(leverage),
            int(size),
            int(filled_qty),
            Quant(price),
            Quant(fee),
            contract_val,
            Quant(price_avg),
            int(type),
            instrument_id,
            int(order_id),
            timestamp,
            int(status))

    def _received_futures_position(self,
                                   long_qty,
                                   long_avail_qty,
                                   long_avg_cost,
                                   long_settlement_price,
                                   realised_pnl,
                                   short_qty,
                                   short_avail_qty,
                                   short_avg_cost,
                                   short_settlement_price,
                                   liquidation_price,
                                   instrument_id,
                                   leverage,
                                   created_at,
                                   updated_at,
                                   margin_mode):
        """
            margin_mode	String	账户类型：全仓 crossed
            liquidation_price	String	预估爆仓价
            long_qty	String	多仓数量
            long_avail_qty	String	多仓可平仓数量
            long_avg_cost	String	开仓平均价
            long_settlement_price	String	结算基准价
            realized_pnl	String	已实现盈余
            leverage	String	杠杆倍数
            short_qty	String	空仓数量
            short_avail_qty	String	空仓可平仓数量
            short_avg_cost	String	开仓平均价
            short_settlement_price	String	结算基准价
            instrument_id	String	合约ID，如BTC-USDT-180213
            created_at	String	创建时间
            updated_at	String	更新时间
        """
        logging.info('_received_futures_position:')
        logging.info(long_qty)
        logging.info(long_avg_cost)
        logging.info(short_qty)
        logging.info(short_avg_cost)

    async def read_loop(self):
        while True:
            try:
                async with websockets.connect(OK_WEBSOCKET_ADDRESS) as self._conn:
                    await self._create_and_login()
                    await self._subscribe_all_interested()
                    while True:
                        await self._receive_and_dispatch()
            except websockets.exceptions.InvalidState:
                logging.error('websocket exception', exc_info=True)
            except CancelledError:
                logging.critical('Websocket loop is cancelled', exc_info=True)
                return
            except:
                logging.critical('Unhandled exception', exc_info=True)
                return


def _testing_non_blocking():
    from . import singleton, logger
    from datetime import datetime
    import requests

    logger.init_global_logger(log_level=logging.INFO, log_to_stderr=True)

    async def ping(url):
        await singleton.websocket.ready
        while True:
            start = datetime.now()
            r = requests.get(url)
            end = datetime.now()
            elapse = end - start
            logging.info(
                f'{url} GET response: {r.status_code}, start: {start}, '
                f'end: {end}'
                f', elapse: {elapse.seconds} seconds, will sleep for 5 sec')
            await asyncio.sleep(5)

    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    singleton.loop.create_task(ping('http://www.google.com'))
    singleton.loop.create_task(ping('http://www.baidu.com'))
    singleton.loop.create_task(ping('http://www.okex.com'))

    singleton.start_loop()


if __name__ == '__main__':
    _testing_non_blocking()
