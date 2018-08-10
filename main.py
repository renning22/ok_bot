import asyncio
import functools
import itertools
import json
from concurrent.futures import ProcessPoolExecutor
from decimal import *

import ccxt
import pandas as pd
import websockets
from scipy import stats

import order
from slack import send_unblock
from util import CheckPoint, current_time, delta

order_executors = {
    'this_week': ProcessPoolExecutor(max_workers=1),
    'next_week': ProcessPoolExecutor(max_workers=1),
    'quarter': ProcessPoolExecutor(max_workers=1)
}

currency = 'btc'
window_length_max = 60 * 10
window_length_min = 60 * 5
# zscore_threshold = -3.0
spread_minus_avg_threshold = -28

columns = ['timestamp',
           'this_week_bid_price',
           'this_week_bid_vol',
           'this_week_ask_price',
           'this_week_ask_vol',
           'next_week_bid_price',
           'next_week_bid_vol',
           'next_week_ask_price',
           'next_week_ask_vol',
           'quarter_bid_price',
           'quarter_bid_vol',
           'quarter_ask_price',
           'quarter_ask_vol',
           'source']
columns_asks = [i for i in columns if 'ask_price' in i]
columns_bids = [i for i in columns if 'bid_price' in i]

channels = {
    f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
    f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
    f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
}

last_record = {}
table = pd.DataFrame()


log_check_point = CheckPoint(interval_sec=10)
arbitrage_check_point = CheckPoint(interval_sec=60)


def rchop(s, ending):
    if s.endswith(ending):
        return s[:-len(ending)]
    return s


def trigger_arbitrage(pair):
    if not arbitrage_check_point.check():
        return

    left, right = tuple(pair.split('-'))
    ask_type = rchop(left, '_ask_price')
    ask_price = last_record[left] + Decimal('1')
    ask_vol = last_record[f'{ask_type}_ask_vol']
    bid_type = rchop(right, '_bid_price')
    bid_price = last_record[right] - Decimal('1')
    bid_vol = last_record[f'{bid_type}_bid_vol']
    amount = Decimal('20')
    amount = min(amount, ask_vol)
    amount = min(amount, bid_vol)

    send_unblock(f'LONG on {ask_type} at {ask_price} for {amount}')
    send_unblock(f'SHORT on {bid_type} at {bid_price} for {amount}')

    long_order = functools.partial(
        order.place_long_order, ask_type, amount, ask_price)
    short_order = functools.partial(
        order.place_short_order, bid_type, amount, bid_price)

    asyncio.get_event_loop().run_in_executor(
        order_executors[ask_type], long_order)
    asyncio.get_event_loop().run_in_executor(
        order_executors[bid_type], short_order)


def calculate():
    log_check = log_check_point.check()
    time_window = table.index[-1] - table.index[0]
    if log_check:
        print(f'{time_window}')
    if time_window < delta(window_length_min):
        return

    for pair in table.columns:
        if '-' in pair:
            history = table[pair].astype('float64')
            spread = history[-1]
            spread_minus_avg = spread - history.mean()
            # zscores = stats.zscore(history)

            if log_check:
                print('{:<50} {:>10.4} {:>10.4}'.format(
                    pair, spread, spread_minus_avg))

            # if zscores[-1] < zscore_threshold and diff < spread_minus_avg_threshold:
            if spread_minus_avg < spread_minus_avg_threshold:
                trigger_arbitrage(pair)


def last_record_to_row():
    data = {}
    for c in columns:
        if c != 'timestamp':
            data[c] = [last_record[c]]
    for i, j in itertools.product(columns_asks, columns_bids):
        if i[:4] == j[:4]:
            continue
        data[f'{i}-{j}'] = [last_record[i] - last_record[j]]
    return pd.DataFrame(data, index=[last_record['timestamp']])


def update_table():
    global table
    table = table.append(last_record_to_row())
    time_window_length = delta(window_length_max)
    start = table.index[-1] - time_window_length
    table = table.loc[table.index >= start]


def update_last_record(contract_type, **kwargs):
    last_record['timestamp'] = current_time()
    last_record['source'] = contract_type
    for key, value in kwargs.items():
        k = f'{contract_type}_{key}'
        assert(k in columns)
        last_record[k] = value

    if len(last_record) == len(columns):
        update_table()
        calculate()


async def update(contract_type, ask_price, ask_vol, bid_price, bid_vol):
    # print(f'{contract_type}, {ask_price}, {ask_vol}, {bid_price}, {bid_vol}')
    kwargs = {'bid_price': bid_price,
              'bid_vol': bid_vol,
              'ask_price': ask_price,
              'ask_vol': ask_vol}

    update_last_record(contract_type, **kwargs)


async def ws_loop():
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

            contract_type = channels[channel]
            ask_price = Decimal(str(d['data']['asks'][-1][0]))
            ask_vol = Decimal(str(d['data']['asks'][-1][1]))
            bid_price = Decimal(str(d['data']['bids'][0][0]))
            bid_vol = Decimal(str(d['data']['bids'][0][1]))

            asyncio.ensure_future(
                update(contract_type, ask_price, ask_vol, bid_price, bid_vol))


async def hello():
    order.init(currency)
    while True:
        try:
            await ws_loop()
        except Exception as e:
            print(e)

asyncio.get_event_loop().run_until_complete(hello())
