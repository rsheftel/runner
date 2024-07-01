"""
Unit tests for the base Metric ABC
"""
from pathlib import Path

import pandas as pd
import pytest
import raccoon as rc
from numpy.testing import assert_almost_equal as np_assert_almost_equal
from pytest import approx
from raccoon.utils import assert_series_equal

import metric.unit_test as unit_tests
import data
import metric
from data.structures import SymbolTuple


def test_initialize():
    mdm = data.MarketDataManager(None, None)
    in_data = rc.DataFrame(columns=['close'])
    met = unit_tests.UnitTest01(mdm, (in_data, 'close'))

    assert isinstance(met, metric.Metric)
    assert isinstance(met.data, rc.Series)
    assert isinstance(met.uuid, str)


def test_series_wrap():
    in_data = rc.DataFrame(data={'close': [1, 2, 3]}, index=[4, 5, 6])
    srs_data = rc.Series(data=[1, 2, 3], index=[4, 5, 6])
    met = unit_tests.UnitTest01(None, (in_data, 'close'))

    # metric in... metric out
    res = unit_tests.UnitTest00(None).series_wrap(met)
    assert isinstance(res, metric.Metric)

    # Series in... ViewSeries out
    res = unit_tests.UnitTest00(None).series_wrap(srs_data)
    assert isinstance(res, rc.ViewSeries)
    assert res.data == [1, 2, 3]
    assert res.index == [4, 5, 6]

    # dataframe in... ViewSeries out
    res = unit_tests.UnitTest00(None).series_wrap((in_data, 'close'))
    assert isinstance(res, rc.ViewSeries)
    assert res.data == [1, 2, 3]
    assert res.index == [4, 5, 6]

    # ViewSeries in... ViewSeries out
    vs = rc.ViewSeries([1, 2], [3, 4])
    res = unit_tests.UnitTest00(None).series_wrap(vs)
    assert isinstance(res, rc.ViewSeries)

    # just a dataframe fails
    with pytest.raises(ValueError):
        unit_tests.UnitTest00(None).series_wrap(in_data)


def test_wrap_market_data():
    csvdf = data.CsvDataFeed(Path(__file__).parent.parent.parent / "data/tests/inst/csv_data_feed")
    ldm = data.LiveDataManager(csvdf, host="temp")
    mdm = data.MarketDataManager(None, ldm)
    mdm.add_symbols('stock', ['test.sym.3', 'AAPL', 'MSFT'], '1min')

    actual = unit_tests.UnitTest00(mdm).series_wrap((SymbolTuple('stock', 'MSFT', '1min'), 'close'))
    assert isinstance(actual, rc.ViewSeries)

    src_index = []
    src_data = []
    expected = rc.ViewSeries(src_data, src_index, index_name='datetime', data_name='close', offset=1, sort=True)
    assert_series_equal(actual, expected)

    # add some data
    mdm.bartime = '2010-01-01 09:40:00'
    mdm.update('stock', '1min')
    src_index.append(pd.Timestamp('2010-01-01 09:40:00', tz='America/New_York'))
    src_data.append(44.23)
    assert_series_equal(actual, expected)

    # add some data
    mdm.bartime = '2010-01-01 09:41:00'
    mdm.update('stock', '1min')
    src_index.append(pd.Timestamp('2010-01-01 09:41:00', tz='America/New_York'))
    src_data.append(44.38)
    assert_series_equal(actual, expected)


def test_calculate():
    # setup
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest01(mdm, (in_data, 'close'))
    expected = rc.Series(data_name='value', index_name='datetime', sort=True)
    assert_series_equal(met.data, expected)

    # no input data, bartime before start of data
    mdm.bartime = pd.Timestamp('2010-05-16', tz='America/New_York')
    with pytest.raises(IndexError):
        met.calculate(mdm.bartime)

    # no data, first bar
    mdm.bartime = pd.Timestamp('2010-05-17', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 10})

    met.calculate(mdm.bartime)
    expected.append_row(mdm.bartime, 10)
    assert_series_equal(met.data, expected)

    # new bar
    mdm.bartime = pd.Timestamp('2010-05-18', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 11})

    met.calculate(mdm.bartime)
    expected.append_row(mdm.bartime, 21)
    assert_series_equal(met.data, expected)

    # update input data, repeat bartime, no recalc
    in_data[mdm.bartime, 'close'] = 13

    met.calculate(mdm.bartime)
    assert_series_equal(met.data, expected)

    # force recalc
    met.calculate(mdm.bartime, force_recalc=True)
    expected[mdm.bartime] = 23
    assert_series_equal(met.data, expected)


def test_value_int():
    # setup
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest01(mdm, (in_data, 'close'))

    # no data, first bar
    mdm.bartime = pd.Timestamp('2010-05-17', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 10})

    assert met.value(0) == 10
    with pytest.raises(IndexError):
        met.value(-1)

    # new bar
    mdm.bartime = pd.Timestamp('2010-05-18', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 11})

    assert met[0] == 21
    assert met[-1] == 10

    # update input data, repeat bartime, no recalc
    in_data[mdm.bartime, 'close'] = 13

    assert met.value(0) == 21
    assert met.value(-1) == 10

    # force recalc
    met.calculate(mdm.bartime, force_recalc=True)
    assert met[0] == 23
    assert met[-1] == 10

    # cannot have positive index
    with pytest.raises(IndexError):
        met.value(1)

    # must be valid type
    with pytest.raises(ValueError):
        met.value((1, 2))


def test_value_timestamp():
    # setup
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest01(mdm, (in_data, 'close'))

    # no data, first bar
    mdm.bartime = pd.Timestamp('2010-05-17', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 10})

    assert met.value(mdm.bartime) == 10
    with pytest.raises(ValueError):
        met.value(pd.Timestamp('2010-01-01', tz='America/New_York'))

    # new bar
    mdm.bartime = pd.Timestamp('2010-05-18', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 11})

    assert met[mdm.bartime] == 21
    assert met[pd.Timestamp('2010-05-17', tz='America/New_York')] == 10

    # update input data, repeat bartime, no recalc
    in_data[mdm.bartime, 'close'] = 13

    assert met.value(mdm.bartime) == 21
    assert met.value(-1) == 10

    # force recalc
    met.calculate(mdm.bartime, force_recalc=True)
    assert met.value(mdm.bartime) == 23
    assert met.value(pd.Timestamp('2010-05-17', tz='America/New_York')) == 10


def test_under_value_datetime():
    # setup
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest02(mdm, (in_data, 'close'), pd.Timestamp('2010-05-17', tz='America/New_York'))

    # no data, first bar
    mdm.bartime = pd.Timestamp('2010-05-17', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 10})

    assert met.value(0) == 10

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-18', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 11})

    assert met[0] == 1
    assert met[-1] == 10

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-19', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 12})

    assert met[0] == 2
    assert met[pd.Timestamp('2010-05-18', tz='America/New_York')] == 1
    assert met[-2] == 10


def test_index_slice():
    # setup
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest03(mdm, (in_data, 'close'))

    # no data, first bar
    mdm.bartime = pd.Timestamp('2010-05-17', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 1})

    assert met[0] == 1

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-18', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 3})

    assert met[0] == 1
    assert met[-1] == 1
    assert met[-1:0] == [1, 1]

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-19', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 5})

    assert met[0] == 2
    assert met[-2:0] == [1, 1, 2]

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-20', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 7})

    assert met[0] == approx(3.66666667, 0.001)
    np_assert_almost_equal(met[-3:0], [1, 1, 2, 3.66666667])

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-21', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 9})

    assert met[0] == approx(4.7777777, 0.001)
    np_assert_almost_equal(met[-4:-1], [1, 1, 2, 3.66666667])

    # next bar
    mdm.bartime = pd.Timestamp('2010-05-22', tz='America/New_York')
    in_data.append_row(mdm.bartime, {'close': 8})

    assert met[0] == approx(4.518518519)
    np_assert_almost_equal(met[-5:-2], [1, 1, 2, 3.66666667])
    np_assert_almost_equal(met[-4:-3], [1, 2])


def test_multi_frequency():
    # setup
    data_1d = rc.DataFrame(columns=['close'])
    data_1min = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest04(mdm, (data_1d, 'close'), (data_1min, 'close'))

    # update 1D and 1min data
    mdm.bartime = pd.Timestamp('2010-05-15 16:00', tz='America/New_York')
    data_1d.append_row(mdm.bartime, {'close': 5})

    mdm.bartime = pd.Timestamp('2010-05-16 09:00', tz='America/New_York')
    data_1min.append_row(mdm.bartime, {'close': 8})
    assert met[0] == 3

    # new 1min
    mdm.bartime = pd.Timestamp('2010-05-16 10:00', tz='America/New_York')
    data_1min.append_row(mdm.bartime, {'close': 10})
    assert met[-1:0] == [3, 5]

    # update 1D and 1min data
    mdm.bartime = pd.Timestamp('2010-05-16 16:00', tz='America/New_York')
    data_1d.append_row(mdm.bartime, {'close': 10})

    mdm.bartime = pd.Timestamp('2010-05-17 09:00', tz='America/New_York')
    data_1min.append_row(mdm.bartime, {'close': 11})
    assert met[-2:0] == [3, 5, 1]

    # new 1min
    mdm.bartime = pd.Timestamp('2010-05-17 10:00', tz='America/New_York')
    data_1min.append_row(mdm.bartime, {'close': 12})
    assert met[-1:0] == [1, 2]


def test_embedded_metric():
    in_data = rc.DataFrame(columns=['close'])
    mdm = data.MarketDataManager(None, None)
    met = unit_tests.UnitTest05(mdm, (in_data, 'close'), 2, 4)

    mdm.bartime = '2017-05-01'
    in_data.append_row(mdm.bartime, {'close': 1})
    assert met[0] == 0

    mdm.bartime = '2017-05-02'
    in_data.append_row(mdm.bartime, {'close': 2})
    assert met[0] == 0

    mdm.bartime = '2017-05-03'
    in_data.append_row(mdm.bartime, {'close': 3})
    assert met[0] == 0.5

    mdm.bartime = '2017-05-04'
    in_data.append_row(mdm.bartime, {'close': 7})
    assert met[0] == 1.75

    mdm.bartime = '2017-05-05'
    in_data.append_row(mdm.bartime, {'close': 9})
    assert met[0] == 2.75
