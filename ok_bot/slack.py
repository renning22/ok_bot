import pprint
import time
from concurrent.futures import ProcessPoolExecutor

from slackclient import SlackClient

_SLACK_TOKEN = 'xoxa-155576176225-412348771143-411178932211-b5814cce48d4c3726a6daaa7f048fa04'
_METHOD = 'chat.postMessage'
_CHANNEL = 'CC3CCUW65'

_executors = ProcessPoolExecutor(max_workers=1)


def _send_impl(message):
    if not isinstance(message, str):
        message = pprint.pformat(message)
    sc = SlackClient(_SLACK_TOKEN)
    sc.api_call(
        _METHOD,
        channel=_CHANNEL,
        text=message
    )
    time.sleep(1)


def send_unblock(message):
    _executors.submit(_send_impl, message)
