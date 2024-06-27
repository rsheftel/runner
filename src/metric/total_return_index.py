"""
Total Return Index metric
"""

import numpy as np

from metric import Metric
from metric.simple import Difference, Duplicate


class TotalReturnIndex(Metric):
    """
    Total Return Index. Takes in as input a regression Metric that will produce output in the form of [beta, intercept],
    a dependant and independent time series and kwargs. This metric will calculate the TRI by:
    - Transforming the input time series to change-on-change times series if lag_bars is > 0,
      otherwise it is level-on-level if lag_bars = 0
    - Apply the regression metric on these changes to get a time series of beta
    - Start with TRI of 100 for first bar
    - On each bar calculate the bar return = chg(dependant) - beta*chg(independent)
    - Add this value to the prior TRI time series to create the cumulative TRI
    """

    def __init__(self, market_data_manager, dependant_data, independent_data, regression_metric, lag_bars, **kwargs):
        """
        :param market_data_manager: MarketDataManager object
        :param dependant_data: the dependant (Y) data
        :param independent_data: the independent (X) data
        :param regression_metric: regression Metric class to use for the beta
        :param lag_bars: change for regression will be calculated between the current bar and the bar lag_bars ago.
                         if 0 then levels will be used for the regression
        :param kwargs: passed to the regression Metric object
        """
        super().__init__(market_data_manager)
        self.lag_bars = lag_bars
        if lag_bars == 0:  # then make the duplicate metrics for calculating the beta, will be level-on-level regress
            self.regress_y = Duplicate(market_data_manager, dependant_data)
            self.regress_x = Duplicate(market_data_manager, independent_data)
        else:  # create the difference metrics for calculating the beta
            self.regress_y = Difference(market_data_manager, dependant_data, lag_bars)
            self.regress_x = Difference(market_data_manager, independent_data, lag_bars)

        # calculate 1d change series to calculate the TRI
        self.chg_y_1d = Difference(market_data_manager, dependant_data, 1)
        self.chg_x_1d = Difference(market_data_manager, independent_data, 1)
        # setup the regression metric
        self.regressor = regression_metric(market_data_manager, self.regress_y, self.regress_x, **kwargs)
        self.started = False

    def _calculate(self, datetime):
        # if the TRI has already started, then just calculate each bar
        if self.started:
            tri_chg = self.chg_y_1d[0] - self.regressor[-1].beta * self.chg_x_1d[0]
            return self._value(-1) + tri_chg

        # since it has not started, first calculate the regression metric and the 1d changes for consistency
        self.regressor.calculate(datetime)
        self.chg_x_1d.calculate(datetime)
        self.chg_y_1d.calculate(datetime)

        # calculate the number of non-None/nan beta entries in the regression metric
        betas = [x.beta for x in self.regressor.data.data]
        non_nones = len(betas) - betas.count(None) - betas.count(np.nan)

        # if there are no non-Nones in the regression beta, return None as it is not ready to start yet
        if non_nones == 0:
            return None

        # if there is one non-None then it is the first good bar and set started to True
        if non_nones == 1:
            self.started = True
            return 100.0
