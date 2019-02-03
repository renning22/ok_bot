import argparse
import logging
import sys

from . import singleton
from .logger import init_global_logger


def main():
    args = argparse.ArgumentParser(description='Automatic arbitrage trading')
    args.add_argument('--symbol',
                      help='Symbol for crypto-currency in under case',
                      default='ETH',
                      required=True)
    args.add_argument('--log-to-file',
                      help='Log to file when set to true',
                      type=bool,
                      default=False)
    args.add_argument('--log-to-slack',
                      help='Also log to slack when set to true',
                      type=bool,
                      default=False)
    args.add_argument('--verbose',
                      help='Log debug level info',
                      action='store_const',
                      dest='log_level',
                      const=logging.DEBUG,
                      default=logging.INFO
                      )

    args = args.parse_args()
    init_global_logger(args.log_to_file, args.log_to_slack, args.log_level)
    symbol = args.symbol
    logging.critical('starting program with %s, args: %s', symbol, sys.argv)

    # initialize components
    singleton.initialize_objects(symbol)
    singleton.start_loop()
