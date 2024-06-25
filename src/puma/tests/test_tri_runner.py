"""
Test of the Metric Runner class as applied to TRI metrics
"""

import os
import tempfile

import pandas as pd
from pandas.testing import assert_frame_equal
from pytest import approx

import database.symbol as symboldb
import puma.metric.metric_container as mc
import puma.runner as runner
import utils.file as futils
import utils.pandas as pdutils
from config.database import credentials
from data.structures import *
from puma.metric.regression import *

# Global variables
db_credentials = {}
seng = None
inst_dir = ''


def setup_module():
    global seng, db_credentials, inst_dir
    inst_dir = os.path.normpath("./puma/metric/tests/inst/")
    test_login = credentials('test')
    seng = symboldb.symbol_engine('stock', **test_login, db_host='localhost')
    db_credentials = credentials('test', 'localhost', prefix='db_')


def teardown_module():
    seng.dispose()


def test_add_metrics():
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(engines={'stock': seng}, source='test_source_02', )

    # Use the UnitTest01 metric which is a basic accumulator
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    metrun.add_metrics(mc.TRIContainer, ExpWeightedOrdinaryLeastSquares, symbol_list,
                       lag_bars=3, window_length=10, half_life=5)

    assert isinstance(metrun.containers[0], mc.TRIContainer)


def test_run():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test.log')

    # delete existing test output files if any
    file1 = os.path.join(tempfile.gettempdir(), 'test.sym.9_test.sym.10_TRI_index_1min.csv')
    file2 = os.path.join(tempfile.gettempdir(), 'test.sym.10_test.sym.11_TRI_index_1min.csv')
    futils.delete(file1)
    futils.delete(file2)

    # setup runner
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(engines={'stock': seng}, source='test_source_02', )

    # Use the ExpWeightedOLS regression metric
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    symbol_list = [PairsTuple(SymbolComponent(symbols[0], 'close'), SymbolComponent(symbols[1], 'close')),
                   PairsTuple(SymbolComponent(symbols[1], 'close'), SymbolComponent(symbols[2], 'close'))]
    metrun.add_metrics(mc.TRIContainer, ExpWeightedOrdinaryLeastSquares, symbol_list,
                       lag_bars=2, output_dir=tempfile.gettempdir(), window_length=10, half_life=3)

    datetimes = pd.date_range('2010-01-04 09:31:00', tz='America/New_York', freq='1min', periods=25)
    metrun.run(datetimes)

    # compare last value
    assert metrun.containers[0].metrics[(symbol_list[0].dependent.symbol_tuple,
                                         symbol_list[0].independent.symbol_tuple), 'metric'][0] == approx(103.1548397)
    assert metrun.containers[0].metrics[(symbol_list[1].dependent.symbol_tuple,
                                         symbol_list[1].independent.symbol_tuple), 'metric'][0] == approx(96.0527252)

    # compare output files
    file1_expected = pdutils.read_csv_time_series(os.path.join(inst_dir, 'test.sym.9_test.sym.10_TRI_index_1min.csv'),
                                                  datetime_col='datetime', parser=pdutils.datetime_parser)
    file2_expected = pdutils.read_csv_time_series(os.path.join(inst_dir, 'test.sym.10_test.sym.11_TRI_index_1min.csv'),
                                                  datetime_col='datetime', parser=pdutils.datetime_parser)

    file1_actual = pdutils.read_csv_time_series(file1, 'datetime', pdutils.datetime_parser)
    file2_actual = pdutils.read_csv_time_series(file2, 'datetime', pdutils.datetime_parser)

    assert_frame_equal(file1_actual, file1_expected)
    assert_frame_equal(file2_actual, file2_expected)
