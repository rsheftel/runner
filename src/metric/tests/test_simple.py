import os

import pandas as pd
import pytest
import raccoon as rc


import data
from data.structures import SymbolTuple
from metric.simple import Accumulate, Difference, Duplicate, Subtraction

inst_dir = None



def setup_module():
    global inst_dir
    inst_dir = os.path.normpath("./data/tests/inst/")  # the directory of the csv files in test dir
    


def assert_metric_result(mdm, metric, all_data, in_data, expected):
    for row in all_data.iterrows():
        datetime = row.pop('index')
        mdm.bartime = datetime
        in_data.append_row(datetime, row)
        metric.value(0)
    assert metric.data.data == expected


def test_duplicate():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [10, 9, 8]}, index=pd.date_range('2010-01-01', freq='1D', periods=3).tolist())
    in_data = rc.DataFrame(columns=['close'])

    met = Duplicate(mdm, (in_data, 'close'))
    assert_metric_result(mdm, met, all_data, in_data, [10, 9, 8])


def test_accumulate():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [10, 9, 8]}, index=pd.date_range('2010-01-01', freq='1D', periods=3).tolist())
    in_data = rc.DataFrame(columns=['close'])

    met = Accumulate(mdm, (in_data, 'close'))
    assert_metric_result(mdm, met, all_data, in_data, [10, 19, 27])


def test_accumulate_market_data():
    csvdf = data.CsvDataFeed(inst_dir + '/csv_data_feed')
    ldm = data.LiveDataManager(csvdf)
    mdm = data.MarketDataManager(None, ldm)
    mdm.add_symbols('stock', ['AAPL', 'MSFT'], '1min')

    met = Accumulate(mdm, (SymbolTuple('stock', 'MSFT', '1min'), 'close'))

    # add some data
    mdm.bartime = '2010-01-01 09:40:00'
    mdm.update('stock', '1min')
    assert met[0] == 44.23

    # add some data
    mdm.bartime = '2010-01-01 09:41:00'
    mdm.update('stock', '1min')
    assert met[0] == 44.23 + 44.38
    assert met[-1] == 44.23

    # add some data
    mdm.bartime = '2010-01-01 09:42:00'
    mdm.update('stock', '1min')
    assert met[0] == 44.23 + 44.38 + 44.39
    assert met[-1] == 44.23 + 44.38
    assert met[-2] == 44.23

    # calculate again with no new data pull the last to repeat
    mdm.bartime = '2010-01-01 09:43:00'
    assert met[0] == 44.23 + 44.38 + 44.39 + 44.39


def test_compound():
    # Test a metric of a metric
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [10, 9, 8]}, index=pd.date_range('2010-01-01', freq='1D', periods=3).tolist())
    in_data = rc.DataFrame(columns=['close'])

    dup = Duplicate(mdm, (in_data, 'close'))
    met0 = Accumulate(mdm, dup)
    met1 = Accumulate(mdm, met0)
    assert_metric_result(mdm, met1, all_data, in_data, [10, 29, 56])


def test_subtract():
    mdm = data.MarketDataManager(None, None)
    all_data = rc.DataFrame({'close': [10, 9, 8]}, index=pd.date_range('2010-01-01', freq='1D', periods=3).tolist())
    in_data = rc.DataFrame(columns=['close'])

    dup = Duplicate(mdm, (in_data, 'close'))
    met0 = Accumulate(mdm, dup)
    met1 = Accumulate(mdm, met0)
    diff = Subtraction(mdm, met1, met0)
    assert_metric_result(mdm, diff, all_data, in_data, [0, 10, 29])


def test_difference():
    all_data = rc.DataFrame({'close': [1, 2, 4, 7, 10, 7, 5, 1]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=8).tolist())

    mdm = data.MarketDataManager(None, None)
    in_data = rc.DataFrame(columns=['close'])
    diff = Difference(mdm, (in_data, 'close'), 1)
    assert_metric_result(mdm, diff, all_data, in_data, [None, 1, 2, 3, 3, -3, -2, -4])

    mdm = data.MarketDataManager(None, None)
    in_data = rc.DataFrame(columns=['close'])
    diff = Difference(mdm, (in_data, 'close'), 2)
    assert_metric_result(mdm, diff, all_data, in_data, [None, None, 3, 5, 6, 0, -5, -6])

    mdm = data.MarketDataManager(None, None)
    in_data = rc.DataFrame(columns=['close'])
    diff = Difference(mdm, (in_data, 'close'), 5)
    assert_metric_result(mdm, diff, all_data, in_data, [None, None, None, None, None, 6, 3, -3])

    # lag_bars cannot be negative
    mdm = data.MarketDataManager(None, None)
    with pytest.raises(AttributeError):
        Difference(mdm, (in_data, 'close'), lag_bars=-1)

    # with None in the input series
    all_data = rc.DataFrame({'close': [1, 2, None, 7, 10, 7, 5, 1]},
                            index=pd.date_range('2010-01-01', freq='1D', periods=8).tolist())

    mdm = data.MarketDataManager(None, None)
    in_data = rc.DataFrame(columns=['close'])
    diff = Difference(mdm, (in_data, 'close'), 1)
    assert_metric_result(mdm, diff, all_data, in_data, [None, 1, None, None, 3, -3, -2, -4])
