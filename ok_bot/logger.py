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


class RelativeTimeAdapter(py_logging.LoggerAdapter):
    """Add relative creation time in seconds."""

    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self._created_time = time.time()

    def process(self, msg, kwargs):
        relative_time = time.time() - self._created_time
        return f'[+{relative_time:12.8f}s] {msg}', kwargs


class TransactionAdapter(py_logging.LoggerAdapter):
    """Add transaction id."""

    def process(self, msg, kwargs):
        id = self.extra['id']
        return f'[{id}] {msg}', kwargs


class UserIdentityAdapter(py_logging.LoggerAdapter):
    """Add username@host."""

    def process(self, msg, kwargs):
        return f'{_USERNAME}@{_HOSTNAME} {msg}', kwargs


class SlackHandler(py_logging.Handler):

    def format(self, record):
        created_tuple = time.localtime(record.created)
        prefix = '%s@%s [%02d%02d %02d:%02d:%02d] ' % (
            _USERNAME,
            _HOSTNAME,
            created_tuple.tm_mon,
            created_tuple.tm_mday,
            created_tuple.tm_hour,
            created_tuple.tm_min,
            created_tuple.tm_sec)
        return prefix + super().format(record)

    def emit(self, record):
        slack.send_unblock(self.format(record))


def create_transaction_logger(id):
    logger = py_logging.getLogger(f'absl.{id}')
    fh = py_logging.FileHandler(f'transaction/{id}.log')
    logger.addHandler(fh)
    if flags.FLAGS.log_transaction_to_slack:
        logger.addHandler(SlackHandler('INFO'))
    return RelativeTimeAdapter(TransactionAdapter(logger, {'id': id}), {})


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
    from . import define_cli_flags
    app.run(_testing)
