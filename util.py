import zlib
from datetime import datetime

import numpy as np


def inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


def delta(sec):
    return np.timedelta64(sec, 's')


def to_time(s):
    return np.datetime64(s)


def current_time():
    return np.datetime64(datetime.utcnow())


class Cooldown:
    def __init__(self, interval_sec=5):
        self._interval = delta(interval_sec)
        self._check_point = current_time()

    def check(self):
        now = current_time()
        if now - self._check_point > self._interval:
            self._check_point = now
            return True
        else:
            return False


def every_five(l):
    if not len(l):
        return
    yield l[:5]
    yield from every_five(l[5:])
