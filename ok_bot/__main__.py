from absl import app, flags

from . import refacted_main

flags.DEFINE_string(
    'symbol', 'btc', 'symbol for crypto-currency in under case.')


if __name__ == '__main__':
    app.run(refacted_main.main)
