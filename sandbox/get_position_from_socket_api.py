import json
import websockets
import asyncio
import zlib
import hashlib

from ..key import api as api_key  # public key
from ..key import secret as api_secret  # private key


def sign(params):
    sign = ''
    for key in sorted(params.keys()):
        sign += key + '=' + str(params[key]) +'&'
    return hashlib.md5((sign+'secret_key=' + api_secret).encode("utf-8")).hexdigest().upper()


def parse_response(binary):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(binary)
    inflated += decompress.flush()
    return json.loads(inflated)


async def main():
    async with websockets.connect('wss://real.okex.com:10440/ws/v1') as ws:
        # according to API doc:
        # 个人信息推送，个人数据有变化时会自动推送，其它旧的个人数据订阅类型可不订阅，
        # 如:ok_sub_futureusd_trades,ok_sub_futureusd_userinfo,ok_sub_futureusd_positions
        # So there's no need to subscribe more channles, there will be update for
        #   * trade happened
        #   * account balance change
        #   * position change
        await ws.send(json.dumps({
            'event': 'login',
            'parameters': {
                "api_key": api_key,
                "sign": sign({"api_key": api_key})
            }
        }))
        while True:
            resp = parse_response(await ws.recv())
            print(resp)


if __name__ == '__main__':
    asyncio.run(main())