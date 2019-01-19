from absl import flags, logging

from . import singleton
from .logger import init_global_logger


def main(_):
    init_global_logger()
    symbol = flags.FLAGS.symbol
    logging.info('starting program with %s', symbol)

    # initialize components
    singleton.initialize_objects(symbol)
    singleton.start_loop()
