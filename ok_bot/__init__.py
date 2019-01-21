import eventlet
from absl import flags

# Global eventlet monkey patch
# Fixes bug: https://github.com/renning22/ok_bot/issues/45
websocket = eventlet.import_patched('websocket')
requests = eventlet.import_patched('requests')

flags.DEFINE_string(
    'symbol', 'ETH', 'symbol for crypto-currency in under case.')
flags.DEFINE_boolean(
    'logtofile', False, 'log to file.')
flags.DEFINE_boolean(
    'alsologtoslack', False, 'also log to slack.')
flags.DEFINE_boolean(
    'log_transaction_to_slack', False, 'also log transaction to slack.')
