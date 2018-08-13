import asyncio
import functools
import json
from concurrent.futures import ProcessPoolExecutor
from decimal import *

import ccxt
import pandas as pd
import websockets
from scipy import stats

import order
import position
from schema import (columns, columns_best_asks, columns_best_bids,
                    columns_cross, contract_types)
from slack import send_unblock
from util import Cooldown, current_time, delta

order_executors = {
    'this_week': ProcessPoolExecutor(max_workers=1),
    'next_week': ProcessPoolExecutor(max_workers=1),
    'quarter': ProcessPoolExecutor(max_workers=1)
}
get_position_executor = ProcessPoolExecutor(max_workers=1)

currency = 'btc'
window_length_max = 60 * 10
window_length_min = 60 * 3
# zscore_threshold = -3.0
spread_minus_avg_threshold = -30
gap_threshold = 8
close_position_zscore_threshold = 0.2
close_position_take_profit_threshold = 100  # price_diff
max_order_amount = Decimal('5')

channels = {
    f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
    f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
    f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
}

last_position = {'this_week': {}, 'next_week': {}, 'quarter': {}}
last_record = {}
table = pd.DataFrame()


log_cooldown = Cooldown(interval_sec=5)
log2_cooldown = Cooldown(interval_sec=1)
arbitrage_cooldown = Cooldown(interval_sec=1)
close_position_cooldown = Cooldown(interval_sec=1)


def rchop(s, ending):
    if s.endswith(ending):
        return s[:-len(ending)]
    return s


def trigger_arbitrage(ask_type, bid_type):
    best_ask_price = last_record[f'{ask_type}_ask_price']
    best_bid_price = last_record[f'{bid_type}_bid_price']
    ask_price = last_record[f'{ask_type}_ask3_price']
    ask_vol = last_record[f'{ask_type}_ask3_vol']
    bid_price = last_record[f'{bid_type}_bid3_price']
    bid_vol = last_record[f'{bid_type}_bid3_vol']
    amount = max_order_amount
    amount = min(amount, ask_vol)
    amount = min(amount, bid_vol)

    gap = (ask_price - best_ask_price) + (best_bid_price - bid_price)
    if gap > gap_threshold:
        if log2_cooldown.check():
            send_unblock(
                f'Drop {ask_type}-{bid_type}: price gap too large: {gap} = '
                f'{ask_price} - {best_ask_price}, '
                f'{best_bid_price} - {bid_price}')
        return

    if not arbitrage_cooldown.check():
        return

    send_unblock(
        f'LONG on {ask_type} at {ask_price} for {amount} (available vol {ask_vol})')
    send_unblock(
        f'SHORT on {bid_type} at {bid_price} for {amount} (available vol {bid_vol})')

    long_order = functools.partial(
        order.open_long_order, ask_type, amount, ask_price)
    short_order = functools.partial(
        order.open_short_order, bid_type, amount, bid_price)

    asyncio.get_event_loop().run_in_executor(
        order_executors[ask_type], long_order)
    asyncio.get_event_loop().run_in_executor(
        order_executors[bid_type], short_order)


def trigger_close_position(ask_type, bid_type):
    if 'long' in last_position[bid_type] and 'short' in last_position[ask_type]:
        long_amount, long_price = last_position[bid_type]['long']
        short_amount, short_price = last_position[ask_type]['short']
        best_ask_price = last_record[f'{ask_type}_ask_price']
        best_bid_price = last_record[f'{bid_type}_bid_price']
        amount = Decimal('1')
        estimate_price_diff = (best_ask_price - long_price) + \
            (short_price - best_bid_price)
        if estimate_price_diff > close_position_take_profit_threshold:
            if not close_position_cooldown.check():
                return
            send_unblock(
                f'Close Position: {ask_type}-{ask_type}, '
                f'({best_ask_price} - {long_price}) + '
                f'({short_price} - {best_bid_price}) = {estimate_price_diff}')
            order.close_long_order(bid_type, amount, best_bid_price)
            order.close_short_order(ask_type, amount, best_ask_price)


def calculate():
    log_cooldown_ready = log_cooldown.check()
    time_window = table.index[-1] - table.index[0]
    if log_cooldown_ready:
        print(f'{time_window}')
    if time_window < delta(window_length_min):
        return

    for pair in table.columns:
        if '-' in pair:
            history = table[pair].astype('float64')
            spread = history[-1]
            spread_minus_avg = spread - history.mean()
            zscores = stats.zscore(history)

            if log_cooldown_ready:
                print('{:<50} {:>10.4} {:>10.4} {:>10.4}'.format(
                    pair, spread, spread_minus_avg, zscores[-1]))

            left, right = tuple(pair.split('-'))
            ask_type = rchop(left, '_ask_price')
            bid_type = rchop(right, '_bid_price')

            if abs(zscores[-1]) < close_position_zscore_threshold:
                trigger_close_position(ask_type, bid_type)

            # if zscores[-1] < zscore_threshold and diff < spread_minus_avg_threshold:
            if spread_minus_avg < spread_minus_avg_threshold:
                trigger_arbitrage(ask_type, bid_type)


def last_record_to_row():
    data = {}
    for c in columns:
        if c != 'timestamp':
            data[c] = [last_record[c]]
    for pair in columns_cross:
        i, j = tuple(pair.split('-'))
        data[f'{i}-{j}'] = [last_record[i] - last_record[j]]
    return pd.DataFrame(data, index=[last_record['timestamp']])


def update_table():
    global table
    table = table.append(last_record_to_row())
    time_window_length = delta(window_length_max)
    start = table.index[-1] - time_window_length
    table = table.loc[table.index >= start]


def update_last_record(contract, asks_bids):
    last_record['timestamp'] = current_time()
    last_record['source'] = contract
    for key, value in asks_bids.items():
        assert(key in columns)
        last_record[key] = value

    if len(last_record) == len(columns):
        update_table()
        calculate()


async def update(contract, asks_bids):
    # print(f'{asks_bids}')
    update_last_record(contract, asks_bids)


async def order_book_loop():
    async with websockets.connect(
            'wss://real.okex.com:10440/websocket/okexapi') as websocket:
        for channel, _ in channels.items():
            await websocket.send(json.dumps({'event': 'addChannel',
                                             'channel': channel}))

        while True:
            response_json = await websocket.recv()
            response = json.loads(response_json)
            d = response[0]
            channel = d['channel']
            if channel not in channels:
                continue
            contract = channels[channel]

            asks = sorted(d['data']['asks'])
            bids = sorted(d['data']['bids'], reverse=True)
            asks_bids = {
                f'{contract}_ask_price': Decimal(str(asks[0][0])),
                f'{contract}_ask_vol': Decimal(str(asks[0][4])),
                f'{contract}_bid_price': Decimal(str(bids[0][0])),
                f'{contract}_bid_vol': Decimal(str(bids[0][4])),
                f'{contract}_ask2_price': Decimal(str(asks[1][0])),
                f'{contract}_ask2_vol': Decimal(str(asks[1][4])),
                f'{contract}_bid2_price': Decimal(str(bids[1][0])),
                f'{contract}_bid2_vol': Decimal(str(bids[1][4])),
                f'{contract}_ask3_price': Decimal(str(asks[2][0])),
                f'{contract}_ask3_vol': Decimal(str(asks[2][4])),
                f'{contract}_bid3_price': Decimal(str(bids[2][0])),
                f'{contract}_bid3_vol': Decimal(str(bids[2][4])),
            }

            asyncio.ensure_future(update(contract, asks_bids))


async def get_position_loop():
    while True:
        for contract in contract_types:
            fetcher = functools.partial(position.get_position, contract)
            result = await asyncio.get_event_loop().run_in_executor(
                get_position_executor, fetcher)
            global last_position
            last_position[contract] = {}
            for p in result:
                assert(contract == p['contract_type'])
                last_position[contract][p['side']] = (
                    p['amount'], p['open_price'])
            await asyncio.sleep(2)


async def order_book_source():
    while True:
        try:
            await order_book_loop()
        except Exception as e:
            print(e)


async def get_position_source():
    while True:
        try:
            await get_position_loop()
        except Exception as e:
            print(e)


def main():
    order.init(currency)
    position.init(currency)
    asyncio.ensure_future(order_book_source())
    asyncio.ensure_future(get_position_source())
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
