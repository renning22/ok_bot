# V3 websocket API.
import base64
import hmac
import json
import pprint
import traceback
import zlib
from decimal import Decimal

import dateutil.parser as dp
import eventlet
from absl import app, logging

from . import api_key_v3, decorator

requests = eventlet.import_patched('requests')
websocket = eventlet.import_patched('websocket')

OK_WEBSOCKET_ADDRESS = 'wss://real.okex.com:10442/ws/v3'
OK_TIMESERVER_ADDRESS = 'http://www.okex.com/api/general/v3/time'
OK_TICKER_ADDRESS = 'http://www.okex.com/api/futures/v3/instruments/ticker'


def _get_all_instrument_ids(symbol):
    response = requests.get(OK_TICKER_ADDRESS)
    if response.status_code == 200:
        return [asset['instrument_id'] for asset in response.json()
                if symbol in asset['instrument_id']]
    else:
        logging.fatal('no trading assets')
        return []


def _get_server_time_ios():
    response = requests.get(OK_TIMESERVER_ADDRESS)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        logging.fatal('failed to request server time')
        return ''


def _get_server_timestamp():
    server_time = _get_server_time_ios()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp


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
    def __init__(self, green_pool=None, symbol='BTC'):
        self._green_pool = green_pool if green_pool else eventlet.GreenPool()
        self._symbol = symbol
        self._ws = None
        self._instrument_ids = None

    def _receive(self):
        res_bin = self._ws.recv()
        return json.loads(_inflate(res_bin).decode())

    def _create_and_login(self):
        self._instrument_ids = _get_all_instrument_ids(self._symbol)

        self._ws = websocket.create_connection(OK_WEBSOCKET_ADDRESS)
        timestamp = str(_get_server_timestamp())
        login_str = _create_login_params(
            str(timestamp),
            api_key_v3.API_KEY,
            api_key_v3.PASSPHRASE,
            api_key_v3.KEY_SECRET)

        self._ws.send(login_str)
        login_res = _inflate(self._ws.recv())

    def _subscribe(self, channels):
        sub_param = {'op': 'subscribe', 'args': channels}
        sub_str = json.dumps(sub_param)
        self._ws.send(sub_str)

    def _subscribe_all_interested(self):
        interested_channels = ['futures/depth5',
                               'futures/order',
                               'futures/position']
        self._subscribe(
            [f'{channel}:{id}'
             for id in self._instrument_ids
             for channel in interested_channels])

    def _receive_and_dispatch(self):
        res_bin = self._ws.recv()
        res = json.loads(_inflate(res_bin).decode())
        if 'event' in res:
            assert res['event'] == 'subscribe'
            logging.info('confirmed "%s" is subscribed', res['channel'])
            return

        if ('table' not in res) or ('data' not in res):
            logging.fatal('unrecgonized websocket response:\n%s',
                          pprint.pformat(res))

        table = res.get('table')

        if table == 'futures/depth5':
            for data in res.get('data', []):
                self._received_futures_depth5(**data)
        elif table == 'futures/order':
            for data in res.get('data', []):
                self._received_futures_order(**data)
        elif table == 'futures/position':
            for data in res.get('data', []):
                self._received_futures_position(**data)
        else:
            logging.error('received unsubscribed event:\n%s',
                          pprint.pformat(res))

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
        logging.info(asks)
        logging.info(bids)
        logging.info(instrument_id)

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

    @decorator.try_catch_loop
    def _read_loop_impl(self):
        self._create_and_login()
        self._subscribe_all_interested()
        while True:
            self._receive_and_dispatch()

    def start_read_loop(self):
        self._green_pool.spawn_n(self._read_loop_impl)


def _testing(_):
    pool = eventlet.GreenPool()
    reader = WebsocketApi(pool, symbol='BTC')
    reader.start_read_loop()
    pool.waitall()


if __name__ == '__main__':
    app.run(_testing)
