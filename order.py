from decimal import *

import ccxt

import key
import slack

_api = ccxt.okex({
    'apiKey': key.api,
    # 'enableRateLimit': True,
    'secret': key.secret})

symbol = None


def init(currency):
    global symbol
    symbol = f'{currency.upper()}/USD'


def _create_order(contract_type, type, side, amount, price=None, params={}):
    try:
        _api.load_markets()
        market = _api.market(symbol)
        method = 'privatePost'
        order = {
            'symbol': market['id'],
            'type': side,
        }
        if market['future']:
            method += 'Future'
            order = _api.extend(order, {
                # this_week, next_week, quarter
                'contract_type': contract_type,
                'match_price': 0,  # match best counter party price? 0 or 1, ignores price if 1
                # 'lever_rate': 20,
                'price': price,
                'amount': amount,
            })
        else:
            order['price'] = price
            order['amount'] = amount
        params = _api.omit(params, 'cost')
        method += 'Trade'
        response = getattr(_api, method)(_api.extend(order, params))
        timestamp = _api.milliseconds()
        return {
            'info': response,
            'id': str(response['order_id']),
            'timestamp': timestamp,
            'datetime': _api.iso8601(timestamp),
            'lastTradeTimestamp': None,
            'status': None,
            'symbol': symbol,
            'contract_type': contract_type,
            'type': type,
            'side': side,
            'price': price,
            'amount': amount,
            'filled': None,
            'remaining': None,
            'cost': None,
            'trades': None,
            'fee': None,
        }
    except Exception as e:
        print(e)
    return None


def notify_slack(result):
    if result:
        side_map = {1: 'OPEN LONG',
                    2: 'OPEN SHORT',
                    3: 'CLOSE LONG',
                    4: 'CLOSE SHORT'}
        side = side_map[result['side']]
        contract_type = result['contract_type']
        price = result['price']
        amount = result['amount']
        symbol = result['symbol']
        timestamp = result['datetime']
        s = f'{timestamp}  {side}  {contract_type}  {price}  {amount}  {symbol}'
        slack.send_unblock(s)


def open_long_order(contract_type, amount, price):
    result = _create_order(
        contract_type=contract_type,
        type='limit',
        side=1,  # 1:开多 2:开空 3:平多 4:平空
        amount=amount,
        price=price)
    notify_slack(result)
    return result


def open_short_order(contract_type, amount, price):
    result = _create_order(
        contract_type=contract_type,
        type='limit',
        side=2,  # 1:开多 2:开空 3:平多 4:平空
        amount=amount,
        price=price)
    notify_slack(result)
    return result


def close_long_order(contract_type, amount, price):
    result = _create_order(
        contract_type=contract_type,
        type='limit',
        side=3,  # 1:开多 2:开空 3:平多 4:平空
        amount=amount,
        price=price)
    notify_slack(result)
    return result


def close_short_order(contract_type, amount, price):
    result = _create_order(
        contract_type=contract_type,
        type='limit',
        side=4,  # 1:开多 2:开空 3:平多 4:平空
        amount=amount,
        price=price)
    notify_slack(result)
    return result


def f(x):
    open_long_order('this_week', 1, 3000 + x)


if __name__ == '__main__':
    init('btc')

    from multiprocessing import Pool
    p = Pool(6)
    p.map(f, range(6))
