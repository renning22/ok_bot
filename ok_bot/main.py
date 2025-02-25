import argparse
import logging
import sys

import git

from . import singleton
from .logger import init_global_logger


def main():
    args = argparse.ArgumentParser(description='Automatic arbitrage trading')
    args.add_argument('--symbol',
                      help='Symbol for crypto-currency in under case',
                      default='ETH',
                      required=True)
    args.add_argument('--log-to-stderr',
                      help='Log to STDERR when set to true',
                      action='store_true')
    args.add_argument('--log-to-slack',
                      help='Also log to slack when set to true',
                      action='store_true')
    args.add_argument('--verbose',
                      help='Log debug level info',
                      action='store_const',
                      dest='log_level',
                      const=logging.DEBUG,
                      default=logging.INFO)
    args.add_argument('--simple-strategy',
                      help='Enable simple trigger strategy',
                      action='store_true')
    args.add_argument('--max-parallel-transaction-num',
                      type=int,
                      default=int(1e9),
                      help='Max number of concurrent transactions')

    args = args.parse_args()
    init_global_logger(log_to_slack=args.log_to_slack,
                       log_level=args.log_level,
                       log_to_stderr=args.log_to_stderr)
    symbol = args.symbol
    last_ci = git.Repo(search_parent_directories=True).head.commit
    logging.critical('Starting program @%s (%s) with %s, args: %s, ',
                     str(last_ci)[:6], last_ci.summary,
                     symbol, sys.argv)

    # initialize components
    singleton.initialize_objects(
        currency=symbol,
        simple_strategy=args.simple_strategy,
        max_parallel_transaction_num=args.max_parallel_transaction_num
    )
    singleton.start_loop()
    logging.critical('Ended program @%s', str(last_ci)[:6])
