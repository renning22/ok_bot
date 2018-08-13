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

    positions = []
    for item in response['holding']:
        if item['symbol'] == native_symbol and item['contract_type'] == contract_type:
            # {'buy_amount': 0,
            #  'buy_available': 0,
            #  'buy_price_avg': 6410,
            #  'buy_price_cost': 6410,
            #  'buy_profit_real': -0.00010779,
            #  'contract_id': 201808170000013,
            #  'contract_type': 'this_week',
            #  'create_date': 1533595502000,
            #  'lever_rate': 20,
            #  'sell_amount': 5,
            #  'sell_available': 5,
            #  'sell_price_avg': 6223,
            #  'sell_price_cost': 6223,
            #  'sell_profit_real': -0.00010779,
            #  'symbol': 'btc_usd'}
            #
            # {'buy_amount': 5,
            #  'buy_available': 5,
            #  'buy_price_avg': 6161.23,
            #  'buy_price_cost': 6161.23,
            #  'buy_profit_real': -0.00294234,
            #  'contract_id': 201809280000012,
            #  'contract_type': 'quarter',
            #  'create_date': 1529296637000,
            #  'lever_rate': 20,
            #  'sell_amount': 0,
            #  'sell_available': 0,
            #  'sell_price_avg': 6389.87,
            #  'sell_price_cost': 6389.87,
            #  'sell_profit_real': -0.00294234,
            #  'symbol': 'btc_usd'}
            contract_type = item['contract_type']
            if item['buy_available'] > 0:
                positions.append({
                    'side': 'long',
                    'symbol': symbol,
                    'amount': Decimal(str(item['buy_available'])),
                    'open_price': Decimal(str(item['buy_price_avg'])),
                    'contract_type': contract_type,
                })
            if item['sell_available'] > 0:
                positions.append({
                    'side': 'short',
                    'symbol': symbol,
                    'amount': Decimal(str(item['sell_available'])),
                    'open_price': Decimal(str(item['sell_price_avg'])),
                    'contract_type': contract_type,
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
    print(get_position('this_week'))
    print(get_position('quarter'))
    # r = fetch_open_orders()
    # print(r)
    # cancel_all_open_orders()
    # print(cancel_order([r[0]['id']]))
