import asyncio
import functools
from decimal import *

import order
from slack import send_unblock
from util import Cooldown

arbitrage_cooldown = Cooldown(interval_sec=1)


max_order_amount = Decimal('2')


def trigger_arbitrage(ask_type, bid_type, last_record):
    best_ask_price = last_record[f'{ask_type}_ask_price']
    best_bid_price = last_record[f'{bid_type}_bid_price']
    ask_price = last_record[f'{ask_type}_ask_price']
    ask_vol = last_record[f'{ask_type}_ask_vol']
    bid_price = last_record[f'{bid_type}_bid_price']
    bid_vol = last_record[f'{bid_type}_bid_vol']
    amount = max_order_amount
    amount = min(amount, bid_vol, ask_vol)

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
        order_executors[ask_type], long_order)
    asyncio.get_event_loop().run_in_executor(
        order_executors[bid_type], short_order)
