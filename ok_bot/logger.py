import getpass
import logging
import os
import socket
import time
import importlib
import timeit

from . import slack

LOG_FORMAT = '%(levelname)-7s %(asctime)s %(filename)s:%(lineno)4d] %(message)s'
_logging_transaction_to_slack = False
_USER_NAME = getpass.getuser()
_HOST_NAME = socket.gethostname()

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


def log_every_n_seconds(level, msg, n_seconds, *args):
    should_log = _seconds_have_elapsed(
        logging.getLogger().findCaller(), n_seconds)
    if should_log:
        logging.log(level, msg, *args)


# Monkey patch to logging
logging.log_every_n_seconds = log_every_n_seconds


class TransactionAdapter(logging.LoggerAdapter):
    """Add transaction id/relative time."""

    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self._created_time = time.time()

    def process(self, msg, kwargs):
        relative_time = time.time() - self._created_time
        return f'[+{relative_time:6.2f}s] {msg}', kwargs

    def log_every_n_seconds(self, level, msg, n_seconds, *args):
        should_log = _seconds_have_elapsed(
            logging.getLogger().findCaller(), n_seconds)
        if should_log:
            self.log(level, msg, *args)


class SlackHandler(logging.Handler):
    def format(self, record):
        prefix = '{}@{} '.format(_USER_NAME, _HOST_NAME)
        return prefix + super().format(record)

    def emit(self, record):
        slack.send_unblock(self.format(record))


def create_transaction_logger(id):
    logger = logging.getLogger().getChild(str(id))
    fh = logging.FileHandler(f'transaction/{id}.log')
    logger.addHandler(fh)
    if _logging_transaction_to_slack:
        logger.addHandler(SlackHandler('INFO'))
    return TransactionAdapter(logger, {})


def init_global_logger(
        log_to_file=False,
        log_to_slack=False,
        log_level=logging.INFO):
    global _logging_transaction_to_slack

    # basicConfig won't work if logging module is imported
    # already, so reload it.
    importlib.reload(logging)
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
    )
    if log_to_file:
        fh = logging.FileHandler('log/ok_bot.log')
        fh.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(fh)
    if log_to_slack:
        logging.getLogger().addHandler(SlackHandler('CRITICAL'))
        _logging_transaction_to_slack = True


def _testing():
    logger_0 = create_transaction_logger('test_id_0')
    logger_0.info('1111')
    logger_0.warning('2222')

    logger_1 = create_transaction_logger('test_id_1')
    logger_1.info('3333')
    time.sleep(1)
    logger_1.warning('4444')

    for i in range(12):
        logger_0.log_every_n_seconds(logging.INFO, '[A] %s seconds', 2, i)
        logger_0.log_every_n_seconds(logging.INFO, '[B] %s seconds', 5, i)
        time.sleep(1)


if __name__ == '__main__':
    init_global_logger(log_to_file=False, log_level=logging.DEBUG)
    logging.debug('Testing transaction logging')
    _testing()
