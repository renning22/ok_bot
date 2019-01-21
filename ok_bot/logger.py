import getpass
import logging as py_logging
import os
import socket
import time

from absl import app, flags, logging

from . import slack

_USERNAME = getpass.getuser()
_HOSTNAME = socket.gethostname()

os.makedirs('log', exist_ok=True)
os.makedirs('transaction', exist_ok=True)


class TransactionAdapter(py_logging.LoggerAdapter):
    """Add transaction id/relative time."""

    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self._created_time = time.time()

    def process(self, msg, kwargs):
        relative_time = time.time() - self._created_time
        return f'[+{relative_time:8.4f}s] : {msg}', kwargs


class SlackHandler(py_logging.Handler):

    def format(self, record):
        prefix = '{}@{} '.format(_USERNAME, _HOSTNAME)
        return prefix + super().format(record)

    def emit(self, record):
        slack.send_unblock(self.format(record))


def create_transaction_logger(id):
    logger = logging.get_absl_logger().getChild(str(id))
    fh = py_logging.FileHandler(f'transaction/{id}.log')
    logger.addHandler(fh)
    if flags.FLAGS.log_transaction_to_slack:
        logger.addHandler(SlackHandler('INFO'))
    return TransactionAdapter(logger, {})


def init_global_logger():
    if flags.FLAGS.logtofile:
        logging.get_absl_handler().use_absl_log_file('ok_bot', 'log')
    if flags.FLAGS.alsologtoslack:
        logging.get_absl_logger().addHandler(SlackHandler('INFO'))


def _testing(_):
    logger = create_transaction_logger('test_id_0')
    logger.info('1111')
    logger.critical('2222')

    logger = create_transaction_logger('test_id_1')
    logger.info('3333')
    time.sleep(1)
    logger.critical('4444')


if __name__ == '__main__':
    app.run(_testing)
