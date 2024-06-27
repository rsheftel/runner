"""
Simple metrics
"""

from metric import Metric


class Duplicate(Metric):
    """
    A simple duplicate (copy) of the data into the metric. No math, no transformations.
    """

    def __init__(self, market_data_manager, data):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(data)

    def _calculate(self, datetime):
        return self.ts[0]


class Accumulate(Metric):
    """
    Running total (accumulation) of the time_series input
    """

    def __init__(self, market_data_manager, data):
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(data)

    def _calculate(self, datetime):
        if len(self.data) == 0:
            return self.ts[0]
        else:
            return self.ts[0] + self._value(-1)


class Subtraction(Metric):
    """
    Difference (subtraction) between two inputs
    """

    def __init__(self, market_data_manager, left_data, right_data):
        super().__init__(market_data_manager)
        self.left_data = self.series_wrap(left_data)
        self.right_data = self.series_wrap(right_data)

    def _calculate(self, datetime):
        return self.left_data[0] - self.right_data[0]


class Difference(Metric):
    """
    Lagged difference (change) in one data stream between the most recent value and a value lag_bars ago
    """

    def __init__(self, market_data_manager, data, lag_bars):
        """

        :param market_data_manager: MarketDataManager object
        :param data: input data
        :param lag_bars: change will be calculated between the current bar and the bar lag_bars ago
        """
        if lag_bars <= 0:
            raise AttributeError('lag_bars must be a positive number greater than zero')
        super().__init__(market_data_manager)
        self.ts = self.series_wrap(data)
        self.lag_bars = lag_bars

    def _calculate(self, datetime):
        if len(self.ts) <= self.lag_bars:  # If these are not enough bars yet return None
            return None

        last = self.ts[0]
        prior = self.ts[-self.lag_bars]
        if not last or not prior:  # If either value is None, return None
            return None
        return last - prior
