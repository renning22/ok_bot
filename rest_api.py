# TODO: make sure load_markets call is necessary or not

import ccxt

import key
import slack
from absl import logging
from decimal import Decimal


class OKRest(object):
    def __init__(self, currency):
        self.symbol = f'{currency.upper()}/USD'
        self.ccxt = ccxt.okex({
            'apiKey': key.api,
            'secret': key.secret})
        self.ccxt.load_markets()

    def create_order(self, contract_type, type, side, amount, price=None, params={}):
        logging.info(f"executing order {contract_type} {type} {side} vol: {amount}, price: {price}")
        try:
            market = self.ccxt.market(self.symbol)
            method = 'privatePost'
            order = {
                'symbol': market['id'],
                'type': side,
            }
            if market['future']:
                method += 'Future'
                order = self.ccxt.extend(order, {
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
            params = self.ccxt.omit(params, 'cost')
            method += 'Trade'
            response = getattr(self.ccxt, method)(self.ccxt.extend(order, params))
            timestamp = self.ccxt.milliseconds()
            ret = {
                'info': response,
                'id': str(response['order_id']),
                'timestamp': timestamp,
                'datetime': self.ccxt.iso8601(timestamp),
                'lastTradeTimestamp': None,
                'status': None,
                'symbol': self.symbol,
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
            logging.info("executed order result: " + ret)
            return ret
        except Exception as e:
            logging.error(f"failed to execute order[{contract_type} {type} {side} vol: {amount}, price: {price}]: " +
                          str(e))
        return None
    
    def notify_slack(self, result):
        if result is not None:
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
            s = f'{side}  {contract_type}  {price}  {amount}  {symbol}'
            slack.send_unblock(s)
    
    def open_long_order(self, contract_type, amount, price):
        result = self.create_order(
            contract_type=contract_type,
            type='limit',
            side=1,  # 1:开多 2:开空 3:平多 4:平空
            amount=amount,
            price=price)
        self.notify_slack(result)
        return result
    
    def open_short_order(self, contract_type, amount, price):
        result = self.create_order(
            contract_type=contract_type,
            type='limit',
            side=2,  # 1:开多 2:开空 3:平多 4:平空
            amount=amount,
            price=price)
        self.notify_slack(result)
        return result
    
    def close_long_order(self, contract_type, amount, price):
        result = self.create_order(
            contract_type=contract_type,
            type='limit',
            side=3,  # 1:开多 2:开空 3:平多 4:平空
            amount=amount,
            price=price)
        self.notify_slack(result)
        return result
    
    def close_short_order(self, contract_type, amount, price):
        result = self.create_order(
            contract_type=contract_type,
            type='limit',
            side=4,  # 1:开多 2:开空 3:平多 4:平空
            amount=amount,
            price=price)
        self.notify_slack(result)
        return result

    def get_position(self, contract_type):
        market = self.ccxt.market(self.symbol)
        native_symbol = market['id']
        response = self.ccxt.privatePostFuturePosition(params={
            'symbol': native_symbol,
            'contract_type': contract_type,
        })

        if not response['result']:
            raise ValueError('privatePostFuturePosition returned response has no result field')

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
                if item['buy_available'] > 0:
                    positions.append({
                        'side': 'long',
                        'symbol': self.symbol,
                        'amount': Decimal(str(item['buy_available'])),
                        'open_price': Decimal(str(item['buy_price_avg'])),
                        'contract_type': contract_type,
                    })
                if item['sell_available'] > 0:
                    positions.append({
                        'side': 'short',
                        'symbol': self.symbol,
                        'amount': Decimal(str(item['sell_available'])),
                        'open_price': Decimal(str(item['sell_price_avg'])),
                        'contract_type': contract_type,
                    })
        return positions
    
    def test(self, x):
        self.open_long_order('this_week', 1, 1000 + x)


if __name__ == '__main__':
    import pprint
    api = OKRest('btc')
    pprint.pprint(api.get_position('next_week'))
