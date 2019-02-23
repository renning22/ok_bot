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

    def histogram(self, mark_last=True):
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

        result_lines = []
        for bin, count in enumerate(dist):
            bin_left = intervals[bin]
            bin_right = intervals[bin] + step
            mark = (' <--' if mark_last and self.data[-1] >=
                    bin_left and self.data[-1] < bin_right else '')
            star_count = int((count / max_dist) * 10)
            result_lines.append('{:8.3f} [{:5d}]: {:10s}{}'.format(
                (bin_left + bin_right) / 2, count, '∎' * star_count, mark))
        return '\n'.join(reversed(result_lines))


if __name__ == '__main__':
    import random
    import time
    from .quant import Quant
    from .slack import send_unblock

    s = Stats()
    for i in range(1):
        for _ in range(1000):
            s.add(Quant(random.gauss(50, 20)))
        print(s.histogram())
        send_unblock(s.histogram())
        time.sleep(1)
