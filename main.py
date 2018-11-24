import asyncio
import functools
import json
import traceback
from concurrent.futures import ProcessPoolExecutor
from decimal import *

import pandas as pd
import websockets
from absl import flags, logging
from scipy import stats

from . import order, position
from .schema import (columns, columns_best_asks, columns_best_bids,
                     columns_cross, contract_types)
from .slack import send_unblock
from .util import Cooldown, current_time, delta, inflate

open_order_executors = {
    'this_week': ProcessPoolExecutor(max_workers=1),
    'next_week': ProcessPoolExecutor(max_workers=1),
    'quarter': ProcessPoolExecutor(max_workers=1)
}
close_order_executors = {
    'this_week': ProcessPoolExecutor(max_workers=1),
    'next_week': ProcessPoolExecutor(max_workers=1),
    'quarter': ProcessPoolExecutor(max_workers=1)
}
get_position_executor = ProcessPoolExecutor(max_workers=1)

currency = 'btc'
window_length_max = 60 * 10
window_length_min = 60 * 3
# zscore_threshold = -3.0
spread_minus_avg_threshold = -50
gap_threshold = 6
close_position_zscore_threshold = 0.2
close_position_take_profit_threshold = 40  # price_diff
max_order_amount = Decimal('1')

channels = {
    f'ok_sub_futureusd_{currency}_depth_this_week_5': 'this_week',
    f'ok_sub_futureusd_{currency}_depth_next_week_5': 'next_week',
    f'ok_sub_futureusd_{currency}_depth_quarter_5': 'quarter'
}

last_position = {'this_week': {}, 'next_week': {}, 'quarter': {}}
last_record = {}
table = pd.DataFrame()


log_cooldown = Cooldown(interval_sec=5)
log2_cooldown = Cooldown(interval_sec=2)
arbitrage_cooldown = Cooldown(interval_sec=60)
close_position_cooldown = Cooldown(interval_sec=1)


def rchop(s, ending):
    if s.endswith(ending):
        return s[:-len(ending)]
    return s


def trigger_arbitrage(ask_type, bid_type):
    best_ask_price = last_record[f'{ask_type}_ask_price']
    best_bid_price = last_record[f'{bid_type}_bid_price']
    ask_price = last_record[f'{ask_type}_ask_price']
    ask_vol = last_record[f'{ask_type}_ask_vol']
    bid_price = last_record[f'{bid_type}_bid_price']
    bid_vol = last_record[f'{bid_type}_bid_vol']
    amount = min(max_order_amount, bid_vol, ask_vol)

    gap = (ask_price - best_ask_price) + (best_bid_price - bid_price)
    if gap > gap_threshold:
        if log2_cooldown.check():
            send_unblock(
                f'Ignore: price gap too large: {gap} = '
                f'{ask_price} - {best_ask_price} ({ask_type}), '
                f'{best_bid_price} - {bid_price} ({bid_type})')
        return

    if not arbitrage_cooldown.check():
        return

    send_unblock(
        f'REQUEST: LONG on {ask_type} at {ask_price} for {amount} '
        f'(available vol {ask_vol})')
    send_unblock(
        f'REQUEST: SHORT on {bid_type} at {bid_price} for {amount} '
        f'(available vol {bid_vol})')

    long_order = functools.partial(
        order.open_long_order, ask_type, amount, ask_price)
    short_order = functools.partial(
        order.open_short_order, bid_type, amount, bid_price)

    asyncio.get_event_loop().run_in_executor(
        open_order_executors[ask_type], long_order)
    asyncio.get_event_loop().run_in_executor(
        open_order_executors[bid_type], short_order)


def check_close_position(ask_type, bid_type):
    global last_position
    if 'long' in last_position[bid_type] and 'short' in last_position[ask_type]:
        long_amount, long_price = last_position[bid_type]['long']
        short_amount, short_price = last_position[ask_type]['short']
        best_ask_price = last_record[f'{ask_type}_ask_price']
        best_bid_price = last_record[f'{bid_type}_bid_price']
        amount = Decimal('1')  # TODO: use availabe order book depth from last_record
        estimated_earning = (best_bid_price - long_price) + \
            (short_price - best_ask_price)
        if estimated_earning > close_position_take_profit_threshold:
            if not close_position_cooldown.check():
                return
            send_unblock(
                f'Close Position: {ask_type}-{ask_type}, '
                f'({best_bid_price} - {long_price}) + '
                f'({short_price} - {best_ask_price}) = {estimated_earning}')

            long_order = functools.partial(
                order.close_long_order, bid_type, amount, best_bid_price)
            short_order = functools.partial(
                order.close_short_order, ask_type, amount, best_ask_price)

            asyncio.get_event_loop().run_in_executor(
                close_order_executors[bid_type], long_order)
            asyncio.get_event_loop().run_in_executor(
                close_order_executors[ask_type], short_order)

            # hot update
            if long_amount - 1 == 0:
                del last_position[bid_type]['long']
            else:
                last_position[bid_type]['long'] = (long_amount - 1, long_price)
            if short_amount - 1 == 0:
                del last_position[ask_type]['short']
            else:
                last_position[ask_type]['short'] = (
                    short_amount - 1, short_price)


def calculate():
    log_cooldown_ready = log_cooldown.check()
    time_window = table.index[-1] - table.index[0]
    if log_cooldown_ready:
        logging.info(f'{currency} {time_window}')
    if time_window < delta(window_length_min):
        return

    for pair in table.columns:
        if '-' in pair:
            history = table[pair].astype('float64')
            spread = history[-1]
            spread_minus_avg = spread - history.mean()
            zscores = stats.zscore(history)  # why zscore didn't work?

            if log_cooldown_ready:
                logging.info('{:<5} {:<50} {:>10.4} {:>10.4} {:>10.4}'.format(
                    currency, pair, spread, spread_minus_avg, zscores[-1]))

            left, right = tuple(pair.split('-'))
            ask_type = rchop(left, '_ask_price')
            bid_type = rchop(right, '_bid_price')

            # if abs(zscores[-1]) < close_position_zscore_threshold:
            check_close_position(ask_type, bid_type)

            # TODO: use a more intuitive trigger condition
            # Below logic works with the following illustration. Assume there's contract
            # A and B. A is around $100. B is around 200$. Then A - B is monitored by
            # A_ask - B_bid and B - A is monitored by B_ask - A_bid. A_ask - B_bid should
            # fluctuate around -100 and B_ask - A_bin should around 100. When the spread
            # suddenly increase, A_ask - B_bid will be smaller(bigger in absolute value), thus
            # triggering arbitrage. When the spread suddenly shrink, B_ask - A_bid will be
            # smaller, also triggering arbitrage.

            # if zscores[-1] < zscore_threshold and diff < spread_minus_avg_threshold:
            if spread_minus_avg < spread_minus_avg_threshold:
                trigger_arbitrage(ask_type, bid_type)


def last_record_to_row():
    data = {}
    for c in columns:
        if c != 'timestamp':
            data[c] = [last_record[c]]  # Are those necessary? Where are they used?
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
    # logging.info(f'{asks_bids}')
    update_last_record(contract, asks_bids)


async def order_book_loop():
    async with websockets.connect(
            'wss://real.okex.com:10440/ws/v1') as websocket:
        for channel, _ in channels.items():
            await websocket.send(json.dumps({'event': 'addChannel',
                                             'channel': channel}))

        while True:
            response_json = inflate(await websocket.recv())
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
            traceback.print_exc()


async def get_position_source():
    while True:
        try:
            await get_position_loop()
        except Exception as e:
            traceback.print_exc()


def main(argv):
    global currency
    currency = flags.FLAGS.symbol
    order.init(currency)
    position.init(currency)
    asyncio.ensure_future(order_book_source())
    asyncio.ensure_future(get_position_source())
    asyncio.get_event_loop().run_forever()
