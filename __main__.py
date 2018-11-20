from absl import app, flags, logging

from . import main

flags.DEFINE_string(
    'symbol', 'btc', 'symbol for crypto-currency in under case')


if __name__ == '__main__':
    app.run(main.main)
