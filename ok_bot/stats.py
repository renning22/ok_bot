import numpy as np
import pandas as pd


class Stats:
    def __init__(self, time_window_sec=5):
        self.time_window = np.timedelta64(time_window_sec, 's')
        self.data = pd.Series()

    def truncate(self):
        now = pd.Timestamp.now()
        self.data = self.data.loc[self.data.index >= now - self.time_window]

    def add(self, x):
        now = pd.Timestamp.now()
        self.data = self.data.append(pd.Series(x, index=[now]))

    def var(self):
        self.truncate()
        return self.data.var()

    def mean(self):
        self.truncate()
        return self.data.mean()

    def __str__(self):
        self.truncate()
        return str(self.data)

    def histogram(self):
        self.truncate()
        if self.data.empty:
            return ''

        values = sorted(self.data.values.astype(np.float64))
        min_v = min(values)
        max_v = max(values)

        quantiles = min(self.data.size, 10)
        intervals, step = np.linspace(
            min_v, max_v, num=quantiles, retstep=True)

        bin = 0
        dist = [0] * quantiles
        for i in values:
            if i >= intervals[bin] + step:
                bin += 1
            dist[bin] += 1
        max_dist = max(dist)

        result = ''
        for bin, count in enumerate(dist):
            star_count = int((count / max_dist) * 10)
            result += '{:8.3f} [{:5d}]: {}\n'.format(
                intervals[bin] + step / 2, count, '∎' * star_count)
        return result


if __name__ == '__main__':
    import random
    import time
    from .quant import Quant
    from .slack import send_unblock

    s = Stats()
    for i in range(1):
        for _ in range(100):
            s.add(Quant(random.gauss(50, 20)))
        print(s.histogram())
        send_unblock(s.histogram())
        time.sleep(1)
