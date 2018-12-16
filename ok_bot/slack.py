import getpass
import logging
import pprint
import socket
import time
from concurrent.futures import ProcessPoolExecutor

from slackclient import SlackClient

_USERNAME = getpass.getuser()
_HOSTNAME = socket.gethostname()
_SLACK_TOKEN = 'xoxa-155576176225-412348771143-411178932211-b5814cce48d4c3726a6daaa7f048fa04'
_METHOD = 'chat.postMessage'
_CHANNEL = 'CC3CCUW65'

executors = ProcessPoolExecutor(max_workers=1)


def send_impl(message):
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
    executors.submit(send_impl, message)


class SlackLoggingHandler(logging.Handler):

    def format(self, record):
        created_tuple = time.localtime(record.created)
        prefix = '%s@%s [%02d%02d %02d:%02d:%02d %s:%d] ' % (
            _USERNAME,
            _HOSTNAME,
            created_tuple.tm_mon,
            created_tuple.tm_mday,
            created_tuple.tm_hour,
            created_tuple.tm_min,
            created_tuple.tm_sec,
            record.filename,
            record.lineno)
        return prefix + super().format(record)

    def emit(self, record):
        send_unblock(self.format(record))
