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
    args.add_argument('--log-to-file',
                      help='Log to file when set to true',
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
    init_global_logger(args.log_to_file, args.log_to_slack, args.log_level)
    symbol = args.symbol
    sha = git.Repo(search_parent_directories=True).head.object.hexsha
    logging.critical('starting program with %s, args: %s, GIT sha: %s',
                     symbol, sys.argv, sha)

    # initialize components
    singleton.initialize_objects(
        currency=symbol,
        simple_strategy=args.simple_strategy,
        max_parallel_transaction_num=args.max_parallel_transaction_num
    )
    singleton.start_loop()
