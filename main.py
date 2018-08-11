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
from schema import columns, columns_best_asks, columns_best_bids, columns_cross
from slack import send_unblock
from util import Cooldown, current_time, delta

order_executors = {
    'this_week': ProcessPoolExecutor(max_workers=1),
    'next_week': ProcessPoolExecutor(max_workers=1),
    'quarter': ProcessPoolExecutor(max_workers=1)
}

currency = 'btc'
window_length_max = 60 * 10
window_length_min = 60 * 1
# zscore_threshold = -3.0
spread_minus_avg_threshold = -30
gap_threshold = 6
max_order_amount = Decimal('5')

channels = {
    f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
    f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
    f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
}

last_record = {}
table = pd.DataFrame()


log_cooldown = Cooldown(interval_sec=5)
log2_cooldown = Cooldown(interval_sec=1)
arbitrage_cooldown = Cooldown(interval_sec=60)


def rchop(s, ending):
    if s.endswith(ending):
        return s[:-len(ending)]
    return s


def place_arbitrage_order(pair):
    if not arbitrage_cooldown.check():
        return

    send_unblock(
        f'LONG on {ask_type} at {ask_price} for {amount} (available vol {ask_vol})')
    send_unblock(
        f'SHORT on {bid_type} at {bid_price} for {amount} (available vol {bid_vol})')

    long_order = functools.partial(
        order.place_long_order, ask_type, amount, ask_price)
    short_order = functools.partial(
        order.place_short_order, bid_type, amount, bid_price)

    asyncio.get_event_loop().run_in_executor(
        order_executors[ask_type], long_order)
    asyncio.get_event_loop().run_in_executor(
        order_executors[bid_type], short_order)


def trigger_arbitrage(pair):
    left, right = tuple(pair.split('-'))
    ask_type = rchop(left, '_ask_price')
    bid_type = rchop(right, '_bid_price')
    ask_price = last_record[f'{ask_type}_ask3_price']
    ask_vol = last_record[f'{ask_type}_ask3_vol']
    bid_price = last_record[f'{bid_type}_bid3_vol']
    bid_vol = last_record[f'{bid_type}_bid3_vol']
    amount = max_order_amount
    amount = min(amount, ask_vol)
    amount = min(amount, bid_vol)

    gap = (ask_price - last_record[left]) + (last_record[right] - bid_price)
    if gap > gap_threshold:
        if log2_cooldown.check():
            send_unblock(
                f'Drop: price gap too large: '
                'f{ask_price} - f{last_record[left]}, '
                'f{last_record[right]} - f{bid_price}')
        return

    place_arbitrage_order()


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

            # if zscores[-1] < zscore_threshold and diff < spread_minus_avg_threshold:
            if spread_minus_avg < spread_minus_avg_threshold:
                trigger_arbitrage(pair)


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


async def hello():
    order.init(currency)
    while True:
        try:
            await ws_loop()
        except Exception as e:
            print(e)

asyncio.get_event_loop().run_until_complete(hello())
