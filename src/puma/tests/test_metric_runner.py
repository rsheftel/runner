"""
Test of the Metric Runner class
"""

import os

import puma.data as datalib
import database.symbol as symboldb
import puma.metric.metric_container as mc
import puma.metric.unit_test as test_metrics
import puma.runner as runner
import pandas as pd
import pytest
from config.database import credentials
from config.datetime import NYC, default_time_zone
from puma.data.structures import SymbolTuple
from pandas.testing import assert_index_equal
from pytest import approx

# Global variables
data_dir = ''
db_credentials = {}
seng = None


def setup_module():
    global seng, db_credentials, data_dir
    data_dir = os.path.normpath("./puma/data/tests/inst/csv_data_feed")
    test_login = credentials('test')
    seng = symboldb.symbol_engine('stock', **test_login, db_host='localhost')
    db_credentials = credentials('test', 'localhost', prefix='db_')


def teardown_module():
    seng.dispose()


def test_construction():
    metrun = runner.MetricRunner(**db_credentials, runner_id='test_runner_99')
    assert metrun.id == 'test_runner_99'


def test_setup_market_data():
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)

    assert isinstance(metrun.market_data_manager, datalib.MarketDataManager)
    assert metrun.time_zone == default_time_zone

    metrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir, time_zone='EST')
    assert metrun.time_zone == 'EST'


def test_add_metrics():
    metrun = runner.MetricRunner(**db_credentials)

    # test that adding metrics before defining market data raises error
    with pytest.raises(RuntimeError):
        metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest01,
                           [(SymbolTuple('stock', 'test.sym.9', '1min'), 'close')])

    metrun.setup_market_data(data_feed='SymbolDBDataFeed', engines={'stock': seng})
    metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest01,
                       [(SymbolTuple('stock', 'test.sym.9', '1min'), 'close')])
    assert isinstance(metrun.containers[0], mc.MetricContainer)


def test_bartimes_daily():
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest01,
                       [(SymbolTuple('stock', 'test.sym.9', '1D'), 'close')])

    # Test daily, note that the 1st is missing for the holiday, as are the 5th and 6th for weekend
    expected = pd.DatetimeIndex([pd.Timestamp(x + ' 16:00', tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02', '1991-01-03', '1991-01-04', '1991-01-07']], freq='B')
    actual = metrun.bartimes(pd.Timestamp('1991-01-01'), pd.Timestamp('1991-01-07'), include_open=False)
    assert_index_equal(actual, expected)


def test_bartimes_minute():
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest01,
                       [(SymbolTuple('stock', 'test.sym.9', '1min'), 'close')])

    # Test minute inside one day including the open bar
    expected = pd.DatetimeIndex([pd.Timestamp(x, tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02 09:30:00', '1991-01-02 09:31:00', '1991-01-02 09:32:00']],
                                freq='T')
    actual = metrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC),
                             pd.Timestamp('1991-01-02 09:32:00', tz=NYC))
    assert_index_equal(actual, expected)

    # Test minute inside one day not including the open bar
    expected = pd.DatetimeIndex([pd.Timestamp(x, tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02 09:31:00', '1991-01-02 09:32:00']],
                                freq='T')
    actual = metrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC),
                             pd.Timestamp('1991-01-02 09:32:00', tz=NYC), include_open=False)
    assert_index_equal(actual, expected)

    # Error out if no time zone on the start_datetime or end_datetime
    with pytest.raises(ValueError):
        metrun.bartimes(pd.Timestamp('1991-01-02 09:30:00'), pd.Timestamp('1991-01-02 09:32:00', tz=NYC))

    with pytest.raises(ValueError):
        metrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC), pd.Timestamp('1991-01-02 09:32:00'))


def test_run():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test.log')

    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(engines={'stock': seng}, source='test_source_02', )

    # Use the UnitTest01 metric which is a basic accumulator
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.11']]
    metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest01, [(x, 'close') for x in symbols])

    # Use the UnitTest05 of dual moving averages
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.10', 'test.sym.11']]
    metrun.add_metrics(mc.SignalContainer, test_metrics.UnitTest05, [(x, 'close') for x in symbols],
                       length_fast=2, length_slow=3)

    datetimes = pd.date_range('2010-01-04 09:31:00', tz='America/New_York', freq='1min', periods=5)
    metrun.run(datetimes)

    assert metrun.containers[0].metrics[('stock', 'test.sym.9', '1min'), 'metric'][0] == 258.74
    assert metrun.containers[0].metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 493.22

    assert metrun.containers[1].metrics[('stock', 'test.sym.10', '1min'), 'metric'][0] == approx(0.15)
    assert metrun.containers[1].metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == approx(-0.34)
