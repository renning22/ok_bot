import os

from absl import flags, logging

from . import singleton
from .slack import SlackLoggingHandler


def config_logging():
    if flags.FLAGS.logtofile:
        os.makedirs('log', exist_ok=True)
        logging.get_absl_handler().use_absl_log_file('ok_bot', 'log')
    if flags.FLAGS.alsologtoslack:
        logging.get_absl_logger().addHandler(SlackLoggingHandler('INFO'))


def main(_):
    config_logging()

    symbol = flags.FLAGS.symbol
    logging.info('starting program with %s', symbol)

    # initialize components
    singleton.initialize_objects(symbol)
    singleton.start_loop()


