from absl import flags, logging

from . import singleton
from . import define_cli_flags
from .logger import init_global_logger



def main(_):
    define_cli_flags.define_flags()
    init_global_logger()
    symbol = flags.FLAGS.symbol
    logging.info('starting program with %s', symbol)

    # initialize components
    singleton.initialize_objects(symbol)
    singleton.start_loop()
