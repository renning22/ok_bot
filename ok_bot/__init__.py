from absl import flags

# Make sure patched_io_modules is being imported before everything.
from . import patched_io_modules

flags.DEFINE_string(
    'symbol', 'ETH', 'symbol for crypto-currency in under case.')
flags.DEFINE_boolean(
    'logtofile', False, 'log to file.')
flags.DEFINE_boolean(
    'alsologtoslack', False, 'also log to slack.')
flags.DEFINE_boolean(
    'log_transaction_to_slack', False, 'also log transaction to slack.')
