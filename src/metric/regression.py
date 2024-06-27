"""
Regression metrics
"""

from collections import namedtuple

import numpy as np
import statsmodels.api as sm

import math.statistics as statutils
from metric import Metric
from utils.collections import strip_leading_none

RegressionCoefficient = namedtuple('RegressionCoefficient', ['intercept', 'beta'])


class OrdinaryLeastSquares(Metric):
    """
    OLS regression. The data is a list of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # if the length of the non None data is 1, then special case where sm.WLS does not work
        if len(x) == 1:
            return RegressionCoefficient(intercept=0, beta=(y[0] / x[0]))

        # all other instances, calculate
        x = sm.add_constant(x)
        model = sm.OLS(y, x, missing='drop')
        res = model.fit().params
        return RegressionCoefficient(intercept=res[0], beta=res[1])


class ExpWeightedOrdinaryLeastSquares(Metric):
    """
    Weighted OLS regression. The data is a list of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length, half_life):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        :param half_life: the half life for the exponential weighting
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length
        # pre-calculate the weights list. Must be reversed to match order of the observations
        self.weights = list(reversed(statutils.exponential_weights(window_length, half_life)))

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # if the length of the non None data is 1, then special case where sm.WLS does not work
        if len(x) == 1:
            return RegressionCoefficient(intercept=0, beta=(y[0] / x[0]))

        # all other instances, calculate
        w = self.weights[-len(y):]
        x = sm.add_constant(x)
        model = sm.WLS(y, x, w, missing='drop')
        res = model.fit().params
        return RegressionCoefficient(intercept=res[0], beta=res[1])


class EqualDollarWeighted(Metric):
    """
    Calculates the ratio of the dollar weighting. This is useful for stocks where the assets have very low or volatile
    correlation and just want to keep the investment amount on both side constant. This will expect that the data, both
    dependant and independent, are level of prices and not changes. The use of changes will lead to unstable results.
    The data is a list of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # all other instances, calculate
        ratio = (sum(y) / len(y)) / (sum(x) / len(x))
        return RegressionCoefficient(intercept=0, beta=ratio)


class ExpEqualDollarWeighted(Metric):
    """
    Calculates the ratio of the dollar weighting. This is useful for stocks where the assets have very low of volatile
    correlation and just want to keep the investment amount on both side constant. In this metric an exponential
    weighting is applied to the observations. The data is a list of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length, half_life):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        :param half_life: the half life for the exponential weighting
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length
        # pre-calculate the weights list. Must be reversed to match order of the observations
        self.weights = list(reversed(statutils.exponential_weights(window_length, half_life)))

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # all other instances, calculate
        w = self.weights[-len(y):]
        ratio = np.average(y, weights=w) / np.average(x, weights=w)
        return RegressionCoefficient(intercept=0, beta=ratio)


class TotalLeastSquares(Metric):
    """
    Total Least Squares regression (aka: Orthogonal Distance Regression). The data is a list of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # if the length of the non None data is 1, then special case where sm.WLS does not work
        if len(x) == 1:
            return RegressionCoefficient(intercept=0, beta=(y[0] / x[0]))

        # all other instances, calculate
        res = statutils.total_least_squares(x, y)
        return RegressionCoefficient(intercept=res[1], beta=res[0])  # Note the return order different from OLS


class ExpWeightedTotalLeastSquares(Metric):
    """
    Exponentially weighted Total Least Squares regression (aka: Orthogonal Distance Regression). The data is a list
    of RegressionCoefficient
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, window_length, half_life):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param window_length: number of observations to use
        :param half_life: the half life for the exponential weighting
        """
        super().__init__(market_data_manager)
        self.y = self.series_wrap(dependant_data)
        self.x = self.series_wrap(independent_data)
        self.length = window_length
        # pre-calculate the weights list. Must be reversed to match order of the observations
        self.weights = list(reversed(statutils.exponential_weights(window_length, half_life)))

    def _calculate(self, datetime):
        bars_back = max(-self.length + 1, -len(self.data))
        y = self.y[bars_back:0]
        x = self.x[bars_back:0]
        x, y = strip_leading_none(x, y)

        # if the length of the non None data is 0, return None
        if len(x) == 0:
            return RegressionCoefficient(intercept=None, beta=None)

        # if the length of the non None data is 1, then special case where sm.WLS does not work
        if len(x) == 1:
            return RegressionCoefficient(intercept=0, beta=(y[0] / x[0]))

        # all other instances, calculate
        w = self.weights[-len(y):]
        res = statutils.total_least_squares(x, y, w)
        return RegressionCoefficient(intercept=res[1], beta=res[0])  # Note the return order different from OLS
