"""
Unit test of regression functions. Note that the mock input data is arbitrary and in practice some regression metrics
can accept changes or levels, like OLS, and some like fixed dollar only make sense with levels.
"""

import math

import pandas as pd
import raccoon as rc
from numpy.testing import assert_almost_equal

from import data
from metric.regression import *


def assert_metric_result(mdm, met, all_data, y_data, x_data, expected):
    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)
    assert_almost_equal(met.data.data, expected)


def test_ols():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [10 + 10 * x for x in range(12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = OrdinaryLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 6)

    expected = [[0, 0.1], [0, 0.1], [0, 0.1], [0, 0.1], [-1.2, 0.16], [-1.4, 0.16857143], [-1.97142857, 0.17714286],
                [-1.77142857, 0.16857143], [-2.57142857, 0.18571429], [-1.14285714, 0.16857143],
                [-2.05714286, 0.17714286], [-1.51428571, 0.16857143]]

    expected = [RegressionCoefficient(*x) for x in expected]
    assert_metric_result(mdm, met, all_data, y_data, x_data, expected)


def test_ols_with_none():
    mdm = data.MarketDataManager(None, None)
    # noinspection PyTypeChecker
    all_data = rc.DataFrame({'close': [None, None, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [None, None] + [10 + 10 * x for x in range(2, 12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = OrdinaryLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 6)

    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    expected = [[None, None], [None, None], [0, 0.1], [0, 0.1], [-5, 0.25], [-3.9, 0.22], [-2.7, 0.19],
                [-1.77142857, 0.16857143], [-2.57142857, 0.18571429], [-1.14285714, 0.16857143],
                [-2.05714286, 0.17714286], [-1.51428571, 0.16857143]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert met.data.data[:2] == expected[:2]  # check the None
    assert_almost_equal(met.data.data[2:], expected[2:])  # check the data


def test_exp_weighted_ols():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [10 + 10 * x for x in range(12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = ExpWeightedOrdinaryLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 10, 5)

    expected = [0.034657359, 0.039810851, 0.045730659, 0.052530733, 0.060341967, 0.069314718, 0.079621703, 0.091461319,
                0.105061466, 0.120683934]
    assert_almost_equal(met.weights, expected)

    expected = [[0, 0.1], [0, 0.1], [0, 0.1], [0, 0.1], [-1.4723777, 0.1686745], [-1.6263593, 0.1746917],
                [-1.3848615, 0.1666187], [-1.0303976, 0.1563188], [-1.7296936, 0.174207], [-1.8356035, 0.1766167],
                [-1.9416535, 0.1760605], [-1.5059835, 0.1684116]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert_metric_result(mdm, met, all_data, y_data, x_data, expected)


def test_exp_weighted_ols_none():
    mdm = data.MarketDataManager(None, None)
    # noinspection PyTypeChecker
    all_data = rc.DataFrame({'close': [np.nan, math.nan, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [None, None] + [10 + 10 * x for x in range(2, 12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = ExpWeightedOrdinaryLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 10, 5)

    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    expected = [[None, None], [None, None], [0, 0.1], [0, 0.1], [-5.2804562, 0.2569315], [-3.88403, 0.219654],
                [-2.4689242, 0.1855056], [-1.3997641, 0.1620541], [-2.6144673, 0.1863899], [-2.5999164, 0.1861225],
                [-2.1292007, 0.1781568], [-1.5059835, 0.1684116]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert met.data.data[:2] == expected[:2]  # check the None
    assert_almost_equal(met.data.data[2:], expected[2:])  # check the data


def test_jagged_data():
    mdm = data.MarketDataManager(None, None)

    # extra None in the volume column
    # noinspection PyTypeChecker
    all_data = rc.DataFrame({'close': [None, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [None, None] + [10 + 10 * x for x in range(2, 12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = ExpWeightedOrdinaryLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 10, 5)

    for count, row in enumerate(all_data.iterrows()):
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    expected = [[None, None], [None, None], [0, 0.1], [0, 0.1], [-5.2804562, 0.2569315], [-3.88403, 0.219654],
                [-2.4689242, 0.1855056], [-1.3997641, 0.1620541], [-2.6144673, 0.1863899], [-2.5999164, 0.1861225],
                [-2.1292007, 0.1781568], [-1.5059835, 0.1684116]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert met.data.data[:2] == expected[:2]  # check the None
    assert_almost_equal(met.data.data[2:], expected[2:])  # check the data


def test_equal_weighted():
    mdm = data.MarketDataManager(None, None)
    # noinspection PyTypeChecker
    all_data = rc.DataFrame({'close': [None, None, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [None, None] + [10 + 10 * x for x in range(2, 12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = EqualDollarWeighted(mdm, (y_data, 'close'), (x_data, 'volume'), window_length=3)

    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    expected = [[None, None], [None, None], [0, 0.1], [0, 0.1], [0, 0.125], [0, 0.14], [0, 0.15], [0, 0.1428571],
                [0, 0.15], [0, 0.1555555], [0, 0.16], [0, 0.1545455]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert met.data.data[:2] == expected[:2]  # check the None
    assert_almost_equal(met.data.data[2:], expected[2:])  # check the data


def test_exp_equal_weighted():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [10 + 10 * x for x in range(12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = ExpEqualDollarWeighted(mdm, (y_data, 'close'), (x_data, 'volume'), window_length=10, half_life=9)

    expected = [[0, 0.10], [0, 0.10], [0, 0.10], [0, 0.10], [0, 0.12206288], [0, 0.13107991], [0, 0.13448491],
                [0, 0.13527791], [0, 0.14287968], [0, 0.14673495], [0, 0.14881713], [0, 0.14982553]]
    expected = [RegressionCoefficient(*x) for x in expected]

    assert_metric_result(mdm, met, all_data, y_data, x_data, expected)


def test_total_least_squares():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [10 + 10 * x for x in range(12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = TotalLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), 7)

    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    # test the last value
    assert_almost_equal(met[0], RegressionCoefficient(intercept=-1.091287301, beta=0.164506367))


def test_exp_weighted_tls():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18],
                             'volume': [10 + 10 * x for x in range(12)]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=12).tolist())
    y_data = rc.DataFrame(columns=['close'])
    x_data = rc.DataFrame(columns=['volume'])

    met = ExpWeightedTotalLeastSquares(mdm, (y_data, 'close'), (x_data, 'volume'), window_length=8, half_life=3)

    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'close': row['close']})
        x_data.append_row(datetime, {'volume': row['volume']})
        met.value(0)

    # test the last value
    assert_almost_equal(met[0], RegressionCoefficient(intercept=-0.169927984, beta=0.155072688))
