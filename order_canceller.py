import asyncio

import position


async def ws_loop():
    while True:
        for contract_type in ['this_week', 'next_week', 'quarter']:
            position.cancel_all_open_orders(ttl=1, contract_type=contract_type)
            await asyncio.sleep(2)


async def hello():
    while True:
        try:
            await ws_loop()
        except Exception as e:
            print(e)


if __name__ == '__main__':
    position.init('btc')
    asyncio.get_event_loop().run_until_complete(hello())
