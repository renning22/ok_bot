from absl import flags

flags.DEFINE_string(
    'symbol', 'ETH', 'symbol for crypto-currency in under case.')
flags.DEFINE_boolean(
   'logtofile', False, 'log to file.')
flags.DEFINE_boolean(
    'alsologtoslack', False, 'also log to slack.')
flags.DEFINE_boolean(
    'log_transaction_to_slack', False, 'also log transaction to slack.')
