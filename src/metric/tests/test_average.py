import pandas as pd
import raccoon as rc
from numpy.testing import assert_almost_equal

import metric as metric
import data
from metric.average import ExponentialWeightedMA, SimpleMovingAverage


def assert_metric_result(mdm, met, all_data, in_data, expected):
    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        in_data.append_row(datetime, row)
        met.value(0)
    assert_almost_equal(met.data.data, expected)


def test_simple_ma():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 5, 6]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=6).tolist())
    in_data = rc.DataFrame(columns=['close'])

    met = SimpleMovingAverage(mdm, (in_data, 'close'), 3)
    assert_metric_result(mdm, met, all_data, in_data, [1, 1.5, 2, 3, 4, 5])


def test_simple_ma_compound():
    # compound as SMA(SMA(data))
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 5, 6]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=6).tolist())
    in_data = rc.DataFrame(columns=['close'])

    ma1 = SimpleMovingAverage(mdm, (in_data, 'close'), 3)
    ma2 = SimpleMovingAverage(mdm, ma1, 4)
    assert_metric_result(mdm, ma2, all_data, in_data, [1, 1.25, 1.5, 1.875, 2.625, 3.5])


def test_simple_ma_difference():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 2, 0]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=6).tolist())
    in_data = rc.DataFrame(columns=['close'])

    ma1 = SimpleMovingAverage(mdm, (in_data, 'close'), 3)
    ma2 = SimpleMovingAverage(mdm, (in_data, 'close'), 4)
    diff = metric.Subtraction(mdm, ma1, ma2)
    assert_metric_result(mdm, diff, all_data, in_data, [0, 0, 0, 0.50, 0.25, -0.25])


def test_ewma():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [1, 2, 3, 4, 5, 6]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=6).tolist())
    in_data = rc.DataFrame(columns=['close'])

    met = ExponentialWeightedMA(mdm, (in_data, 'close'), 5)
    assert_metric_result(mdm, met, all_data, in_data, [1.000000, 1.1294494, 1.3715912, 1.7118372, 2.137488, 2.637488])
