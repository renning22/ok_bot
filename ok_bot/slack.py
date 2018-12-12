import pprint
import time
from concurrent.futures import ProcessPoolExecutor

from slackclient import SlackClient

slack_token = 'xoxa-155576176225-412348771143-411178932211-b5814cce48d4c3726a6daaa7f048fa04'

executors = ProcessPoolExecutor(max_workers=1)


def send_impl(message):
    if not isinstance(message, str):
        message = pprint.pformat(message)
    sc = SlackClient(slack_token)
    sc.api_call(
        "chat.postMessage",
        channel="CC3CCUW65",
        text=message
    )
    time.sleep(1)


def send_unblock(message):
    executors.submit(send_impl, message)
