"""
Unit tests for TRI container
"""

import os
import tempfile

import pandas as pd
from pandas.testing import assert_frame_equal
from pytest import approx

from database import symboldb
import data.datetime as mdatetime
import utils.file as futils
from utils.pandas import read_csv_time_series, datetime_parser

from import data
from data.structures import *
from metric.metric_container import TRIContainer
from metric.regression import *
from metric.total_return_index import *

ldm = None
seng = None
inst_dir = ''


def setup_module():
    global inst_dir, ldm, seng
    inst_dir = os.path.normpath("./metric/tests/inst/")
    
    
    seng = symboldb.symbol_engine('stock', host='localhost')
    symboldf = data.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    ldm = data.LiveDataManager(symboldf)


def teardown_module():
    seng.dispose()


def test_initialize():
    mdm = data.MarketDataManager(None, ldm)
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    mc = TRIContainer(mdm, ExpWeightedOrdinaryLeastSquares, symbol_list, lag_bars=2, window_length=10, half_life=5)

    assert isinstance(mc, TRIContainer)
    assert isinstance(mc.uuid, str)
    assert len(mc.metrics) == 2
    assert isinstance(mc.metrics[(('stock', 'test.sym.9', '1min'), ('stock', 'test.sym.10', '1min')), 'metric'], Metric)

    # check that the kwargs made it all the way to the regressor metric
    tri_metric = mc.metrics[(('stock', 'test.sym.9', '1min'), ('stock', 'test.sym.10', '1min')), 'metric']
    regressor = tri_metric.regressor
    assert regressor.length == 10
    assert regressor.weights[0] == approx(0.034657359)
    assert regressor.weights[9] == approx(0.120683934)


def test_calculate():
    mdm = data.MarketDataManager(None, ldm)
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    mc = TRIContainer(mdm, ExpWeightedOrdinaryLeastSquares, symbol_list, lag_bars=3, window_length=10, half_life=5)

    datetimes = mdatetime.bartimes('NYSE', '1min', pd.Timestamp('2010-01-04 09:31', tz='America/New_York'),
                                   pd.Timestamp('2010-01-04 09:55', tz='America/New_York'))

    # initial calculation is None
    mdm.bartime = datetimes[0]
    mdm.update('stock', '1min')
    mc.calculate()

    assert mc.metrics.get_location(0, as_dict=True)['metric'][0] is None
    assert mc.metrics.get_location(1, as_dict=True)['metric'][0] is None

    # Next 5 bars and check the last bar
    for x in range(1, 6):
        mdm.bartime = datetimes[x]
        mdm.update('stock', '1min')
        mc.calculate()

    assert mc.metrics.get_location(0, as_dict=True)['metric'][0] == approx(100.0667009)
    assert mc.metrics.get_location(1, as_dict=True)['metric'][0] == approx(99.5946380)

    # run to the end of 25 bars
    for x in range(6, 25):
        mdm.bartime = datetimes[x]
        mdm.update('stock', '1min')
        mc.calculate()

    assert mc.metrics.get_location(0, as_dict=True)['metric'][0] == approx(102.8045681)
    assert mc.metrics.get_location(1, as_dict=True)['metric'][0] == approx(96.9426120)


def test_calculate_output():
    # delete existing test output files if any
    file1 = os.path.join(tempfile.gettempdir(), 'test.sym.9_test.sym.10_TRI_index_1min.csv')
    file2 = os.path.join(tempfile.gettempdir(), 'test.sym.10_test.sym.11_TRI_index_1min.csv')
    file3 = os.path.join(tempfile.gettempdir(), 'test.sym.9_test.sym.10_TRI_index_1min_regression.csv')
    file4 = os.path.join(tempfile.gettempdir(), 'test.sym.10_test.sym.11_TRI_index_1min_regression.csv')
    futils.delete(file1)
    futils.delete(file2)
    futils.delete(file3)
    futils.delete(file4)

    # setup objects
    mdm = data.MarketDataManager(None, ldm)
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    mc = TRIContainer(mdm, ExpWeightedOrdinaryLeastSquares, symbol_list, lag_bars=2, output_dir=tempfile.gettempdir(),
                      window_length=10, half_life=3)

    datetimes = mdatetime.bartimes('NYSE', '1min', pd.Timestamp('2010-01-04 09:31', tz='America/New_York'),
                                   pd.Timestamp('2010-01-04 09:55', tz='America/New_York'))

    # Run all bars
    for x in range(25):
        mdm.bartime = datetimes[x]
        mdm.update('stock', '1min')
        mc.calculate()

    # run stop to output data
    mc.stop()

    # compare results - SymbolDB TRI
    file1_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.9_test.sym.10_TRI_index_1min.csv'),
                                 datetime_col='datetime', parser=datetime_parser)
    file2_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.10_test.sym.11_TRI_index_1min.csv'),
                                 datetime_col='datetime', parser=datetime_parser)

    file1_actual = read_csv_time_series(file1, datetime_col='datetime', parser=datetime_parser)
    file2_actual = read_csv_time_series(file2, datetime_col='datetime', parser=datetime_parser)

    assert_frame_equal(file1_actual, file1_expected)
    assert_frame_equal(file2_actual, file2_expected)

    # compare results - TSDB regressor
    file3_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.9_test.sym.10_TRI_index_1min_regression.csv'),
                                 datetime_col='datetime', parser=datetime_parser)
    file4_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.10_test.sym.11_TRI_index_1min_regression.csv'),
                                 datetime_col='datetime', parser=datetime_parser)

    file3_actual = read_csv_time_series(file3, datetime_col='datetime', parser=datetime_parser)
    file4_actual = read_csv_time_series(file4, datetime_col='datetime', parser=datetime_parser)

    assert_frame_equal(file3_actual, file3_expected)
    assert_frame_equal(file4_actual, file4_expected)


def test_slurp_output():
    # setup objects
    mdm = data.MarketDataManager(None, ldm)
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    mc = TRIContainer(mdm, ExpWeightedOrdinaryLeastSquares, symbol_list, lag_bars=2, output_dir=tempfile.gettempdir(),
                      slurp_style=True, window_length=10, half_life=3)

    datetimes = mdatetime.bartimes('NYSE', '1min', pd.Timestamp('2010-01-04 09:31', tz='America/New_York'),
                                   pd.Timestamp('2010-01-04 09:55', tz='America/New_York'))

    # Run all bars
    for x in range(25):
        mdm.bartime = datetimes[x]
        mdm.update('stock', '1min')
        mc.calculate()

    # run stop to output data
    mc.stop()

    # compare SymbolDB TRI results
    file1_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.9_test.sym.10_TRI_index_1min.csv'),
                                 datetime_col='datetime', parser=datetime_parser)
    file2_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.10_test.sym.11_TRI_index_1min.csv'),
                                 datetime_col='datetime', parser=datetime_parser)

    file1_expected.index.name = 'test.sym.9_test.sym.10_TRI:index:1min:test_source_02'
    file2_expected.index.name = 'test.sym.10_test.sym.11_TRI:index:1min:test_source_02'

    file1 = mc.metrics.get_location(0, 'filename_symboldb')
    file2 = mc.metrics.get_location(1, 'filename_symboldb')

    file1_actual = read_csv_time_series(file1, datetime_col=0, parser=datetime_parser)
    file2_actual = read_csv_time_series(file2, datetime_col=0, parser=datetime_parser)

    assert_frame_equal(file1_actual, file1_expected)
    assert_frame_equal(file2_actual, file2_expected)

    # compare TSDB TRI results
    file3_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.9_test.sym.10_TRI_index_1min_regression.csv'),
                                 datetime_col='datetime', parser=datetime_parser)
    file4_expected = read_csv_time_series(os.path.join(inst_dir, 'test.sym.10_test.sym.11_TRI_index_1min_regression.csv'),
                                 datetime_col='datetime', parser=datetime_parser)

    file3_expected.columns = ['test.sym.9_test.sym.10_TRI_index_1min_intercept:test_source_02',
                              'test.sym.9_test.sym.10_TRI_index_1min_beta:test_source_02']
    file4_expected.columns = ['test.sym.10_test.sym.11_TRI_index_1min_intercept:test_source_02',
                              'test.sym.10_test.sym.11_TRI_index_1min_beta:test_source_02']

    file3 = mc.metrics.get_location(0, 'filename_tsdb')
    file4 = mc.metrics.get_location(1, 'filename_tsdb')

    file3_actual = read_csv_time_series(file3, datetime_col=0, parser=datetime_parser)
    file4_actual = read_csv_time_series(file4, datetime_col=0, parser=datetime_parser)

    assert_frame_equal(file3_actual, file3_expected)
    assert_frame_equal(file4_actual, file4_expected)
