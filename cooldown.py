from util import Cooldown

_trigger_arbitrage_cooldown = Cooldown(interval_sec=1)


def trigger_arbitrage_cooldown():
    return trigger_arbitrage_cooldown.check()
