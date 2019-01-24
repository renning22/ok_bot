import getpass
import logging as py_logging
import os
import socket
import time
import timeit

from absl import app, flags, logging
from absl.logging import DEBUG, ERROR, FATAL, INFO, WARNING

from . import slack

_USERNAME = getpass.getuser()
_HOSTNAME = socket.gethostname()

os.makedirs('log', exist_ok=True)
os.makedirs('transaction', exist_ok=True)

# Keeps track of the last log time of the given token.
# Note: must be a dict since set/get is atomic in CPython.
# Note: entries are never released as their number is expected to be low.
_log_timer_per_token = {}


def _seconds_have_elapsed(token, num_seconds):
    """Tests if 'num_seconds' have passed since 'token' was requested.
    Not strictly thread-safe - may log with the wrong frequency if called
    concurrently from multiple threads. Accuracy depends on resolution of
    'timeit.default_timer()'.
    Always returns True on the first call for a given 'token'.
    Args:
      token: The token for which to look up the count.
      num_seconds: The number of seconds to test for.
    Returns:
      Whether it has been >= 'num_seconds' since 'token' was last requested.
    """
    now = timeit.default_timer()
    then = _log_timer_per_token.get(token, None)
    if then is None or (now - then) >= num_seconds:
        _log_timer_per_token[token] = now
        return True
    else:
        return False


class TransactionAdapter(py_logging.LoggerAdapter):
    """Add transaction id/relative time."""

    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self._created_time = time.time()

    def process(self, msg, kwargs):
        relative_time = time.time() - self._created_time
        return f'[+{relative_time:6.2f}s] {msg}', kwargs

    def log_every_n_seconds(self, level, msg, n_seconds, *args):
        should_log = _seconds_have_elapsed(
            logging.get_absl_logger().findCaller(), n_seconds)
        self.log_if(level, msg, should_log, *args)

    def log_if(self, level, msg, condition, *args):
        if condition:
            self.log(level, msg, *args)

    def log(self, level, msg, *args, **kwargs):
        if level > logging.converter.ABSL_DEBUG:
            standard_level = logging.converter.STANDARD_DEBUG - (level - 1)
        else:
            if level < logging.converter.ABSL_FATAL:
                level = logging.converter.ABSL_FATAL
            standard_level = logging.converter.absl_to_standard(level)

        super().log(standard_level, msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        self.log(FATAL, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.log(ERROR, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(WARNING, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(INFO, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.log(DEBUG, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        raise NotImplemented


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
    logger.warning('2222')

    logger = create_transaction_logger('test_id_1')
    logger.info('3333')
    time.sleep(1)
    logger.warning('4444')

    for i in range(10):
        logger.log_every_n_seconds(INFO, '[A] %s seconds', 2, i)
        logger.log_every_n_seconds(INFO, '[B] %s seconds', 5, i)
        time.sleep(1)


if __name__ == '__main__':
    app.run(_testing)
