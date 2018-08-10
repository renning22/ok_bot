import asyncio
from decimal import *

import ccxt
import numpy as np

import key
import slack
from util import current_time, delta, every_five, to_time

_api = ccxt.okex({
    'apiKey': key.api,
    'enableRateLimit': True,
    # 'verbose': True,
    'secret': key.secret})

symbol = None


def init(currency):
    global symbol
    symbol = f'{currency.upper()}/USD'


def get_position(contract_type='this_week'):
    _api.load_markets()
    market = _api.market(symbol)
    native_symbol = market['id']
    response = _api.privatePostFuturePosition(params={
        'symbol': native_symbol,
        'contract_type': contract_type,
    })

    if not response['result']:
        raise ValueError('Unexpected response')

    # force_liqu_price = Decimal(response['force_liqu_price'])
    positions = []
    for item in response['holding']:
        if item['symbol'] == native_symbol and item['contract_type'] == contract_type:
            if item['buy_amount'] == 0 and item['sell_amount'] == 0:
                continue

            side = item['buy_amount'] > 0
            amount = Decimal(str(item['buy_amount'] if side else -
                                 item['sell_amount']))
            entry_price = Decimal(str(item['buy_price_avg'] if side else
                                      item['sell_price_avg']))
            positions.append({
                'symbol': symbol,
                'amount': amount,
                'entry_price': entry_price,
                # 'liquid_price': force_liqu_price,
            })
    return positions


def fetch_open_orders(contract_type='this_week'):
    _api.load_markets()
    market = _api.market(symbol)
    request = {
        'symbol': market['id'],
        'contract_type': contract_type,
        'status': 1,
        'order_id': -1,
        'current_page': 0,
        'page_length': 50,
    }
    response = _api.privatePostFutureOrderInfo(request)
    ordersField = _api.get_orders_field()
    return _api.parse_orders(response[ordersField], market, None, None)


def cancel_order(order_list, contract_type='this_week'):
    _api.load_markets()
    market = _api.market(symbol)
    request = {
        'symbol': market['id'],
        'contract_type': contract_type,
        'order_id': ','.join(map(str, order_list)),
    }
    return _api.privatePostFutureCancel(request)


def notify_slack(order):
    side = order['side']
    contract_type = order['info']['contract_name']
    price = order['price']
    amount = order['amount']
    symbol = order['symbol']
    timestamp = order['datetime']
    s = (f'CANCEL OPEN ORDER: {timestamp}  {side}  {contract_type}  {price}  '
         f'{amount}  {symbol}')
    slack.send_unblock(s)


def cancel_all_open_orders(ttl=1, contract_type='this_week'):
    now = current_time()
    orders = fetch_open_orders(contract_type)
    orders_to_be_cancelled = []
    for order in orders:
        order_id = order['id']
        if now - to_time(order['datetime']) > delta(ttl):
            notify_slack(order)
            orders_to_be_cancelled.append(order_id)
    for order_list in every_five(orders_to_be_cancelled):
        cancel_order(order_list, contract_type)


if __name__ == '__main__':
    init('btc')
    # print(get_position('this_week'))
    # r = fetch_open_orders()
    # print(r)
    # cancel_all_open_orders()
    # print(cancel_order([r[0]['id']]))
