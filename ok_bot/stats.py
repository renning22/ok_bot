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

        values = sorted(self.data.values)
        min_v = values[0]
        max_v = values[-1]

        quantiles = min(self.data.size, 10)
        step = (max_v - min_v) / quantiles

        bin = 0
        dist = [0] * quantiles
        for i in values:
            while (bin + 1 < quantiles) and (i >= bin * step + step):
                bin += 1
            dist[bin] += 1
        max_dist = max(dist)
        last_sample = self.data[-1]

        result_lines = []
        for bin, count in enumerate(dist):
            bin_left = bin * step
            bin_right = bin_left + step
            mark = (' <--' if mark_last and last_sample
                    >= bin_left and last_sample < bin_right else '')
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
        s.add(100)
        print(s.histogram())
        # send_unblock(s.histogram())
        time.sleep(1)
