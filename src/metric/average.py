"""
Moving average metrics
"""

import math
import statistics

from metric import Metric


class SimpleMovingAverage(Metric):
    def __init__(self, market_data_manager, data, length):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(data)
        self.length = length

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        data_slice = self.ts[bars_back:0]
        return statistics.mean(data_slice)


class ExponentialWeightedMA(Metric):
    def __init__(self, market_data_manager, data, half_life):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(data)
        self._lambda = math.pow(0.5, 1 / half_life)
        self.first = True

    def _calculate(self, datetime):
        if not self.first:
            return (1 - self._lambda) * self.ts[0] + self._lambda * self._value(-1)
        self.first = False
        return self.ts[0]
