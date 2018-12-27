from absl import app, flags

from . import refacted_main

flags.DEFINE_string(
    'symbol', 'BTC', 'symbol for crypto-currency in under case.')
flags.DEFINE_boolean(
    'logtofile', False, 'log to file.')
flags.DEFINE_boolean(
    'alsologtoslack', False, 'also log to slack.')


if __name__ == '__main__':
    app.run(refacted_main.main)
