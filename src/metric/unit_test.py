import statistics

import metric as metric
from metric import Metric


class UnitTest00(Metric):
    def _calculate(self, datetime):
        pass


class UnitTest01(Metric):
    def __init__(self, market_data_manager, time_series):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(time_series)
        self._empty = True

    def _calculate(self, datetime):
        if self._empty:
            value = self.ts[0]
            self._empty = False
        else:
            value = self._value(-1) + self.ts[0]
        return value


class UnitTest02(Metric):
    def __init__(self, market_data_manager, time_series, yesterday):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(time_series)
        self.yesterday = yesterday
        self._empty = True

    def _calculate(self, datetime):
        if self._empty:
            value = self.ts[0]
            self._empty = False
        else:
            value = self.ts[0] - self._value(self.yesterday)
        return value


class UnitTest03(Metric):
    def __init__(self, market_data_manager, time_series):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(time_series)
        self.first_bar = True

    def _calculate(self, datetime):
        bars_back = max(-2, -len(self.ts))
        ma1 = statistics.mean(self.ts[bars_back:0])
        if self.first_bar:
            value = ma1
            self.first_bar = False
        else:
            bars_back = max(-3, -len(self.data))
            my_mean = statistics.mean(self._value((bars_back, -1)))
            value = ma1 - my_mean
        return value


class UnitTest04(Metric):
    """
    Difference from the 1D close on second time series
    """

    def __init__(self, market_data_manager, time_series_1d, time_series_1min):
        super().__init__(market_data_manager)
        self.ts_1d = self.series_wrap(time_series_1d)
        self.ts_1min = self.series_wrap(time_series_1min)

    def _calculate(self, datetime):
        return self.ts_1min[0] - self.ts_1d[0]


class UnitTest05(Metric):
    """
    Instantiate a metric inside the metric
    """

    def __init__(self, market_data_manager, data, length_fast, length_slow):
        super().__init__(market_data_manager)
        self.ts = data
        ma_fast = metric.SimpleMovingAverage(market_data_manager, data, length_fast)
        ma_slow = metric.SimpleMovingAverage(market_data_manager, data, length_slow)
        self.diff = metric.Subtraction(market_data_manager, ma_fast, ma_slow)

    def _calculate(self, datetime):
        return self.diff[0]
