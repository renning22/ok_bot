from absl import app, flags, logging

from . import main as ok_bot
from . import order_canceller

flags.DEFINE_boolean(
    'run_order_canceller', False, 'whether to run order_canceller.')
flags.DEFINE_string(
    'symbol', 'btc', 'symbol for crypto-currency in under case.')


def main(argv):
    if flags.FLAGS.run_order_canceller:
        order_canceller.main(argv)
    else:
        ok_bot.main(argv)


if __name__ == '__main__':
    app.run(main)
