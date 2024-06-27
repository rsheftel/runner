"""
Test of TRI using different regression types
"""

import os

import pandas as pd
from pytest import approx

import montauk.metric.metric_container as mc
import montauk.tomahawk.runner as runner
import montauk.utils.data as datautils
from config.database import credentials
from montauk.metric.regression import *

# import montauk.utils.file as futils


# Global variables
db_credentials = {}
inst_dir = ''


def setup_module():
    global db_credentials, inst_dir
    inst_dir = os.path.normpath("./examples/tests/inst/")
    db_credentials = credentials('test', 'localhost', prefix='db_')


def test_equal_dollar():
    # setup logging
    # futils.setup_logging(filename='c:/temp/tri_example_equal.log')

    # setup runner
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(data_feed='CsvDataFeed', directory=os.path.join(inst_dir, 'symbol_data'))

    # setup symbols
    symbol_pairs = pd.DataFrame({'dependent_product_type': ['stock'] * 3,
                                 'dependent_symbol': ['XLF.1C', 'XLE.1C', 'XLRE.1C'],
                                 'dependent_component': ['close'] * 3,
                                 'independent_product_type': ['stock'] * 3,
                                 'independent_symbol': ['SPY.1C'] * 3,
                                 'independent_component': ['close'] * 3,
                                 'frequency': ['1D'] * 3})
    pairs = datautils.df_to_pairs_list(symbol_pairs)

    # EqualDollarWeight regression metric
    metrun.add_metrics(mc.TRIContainer, EqualDollarWeighted, pairs, lag_bars=0, window_length=10)

    # setup the datetimes. Force the close time to be 16:00 even on early closes until the database is updated
    datetimes = metrun.bartimes(pd.Timestamp('2017-01-03', tz='America/New_York'),
                                pd.Timestamp('2017-11-30', tz='America/New_York'),
                                include_open=False, default_close=True)

    # run it all
    metrun.run(datetimes)

    # compare aggregate values
    tri_xlf = metrun.containers[0].metrics[(pairs[0].dependent.symbol_tuple, pairs[0].independent.symbol_tuple),
                                           'metric'].data

    assert len(tri_xlf) == 231
    assert sum(tri_xlf.data) == approx(22940.08232253)
    assert min(tri_xlf.data) == approx(98.03345553)
    assert max(tri_xlf.data) == approx(100.22638415)
    assert min(tri_xlf.index) == pd.Timestamp('2017-01-03 16:00:00-05:00')
    assert max(tri_xlf.index) == pd.Timestamp('2017-11-30 16:00:00-05:00')

    tri_xle = metrun.containers[0].metrics[(pairs[1].dependent.symbol_tuple, pairs[1].independent.symbol_tuple),
                                           'metric'].data

    assert len(tri_xle) == 231
    assert sum(tri_xle.data) == approx(20153.92613036)
    assert min(tri_xle.data) == approx(80.74255089)
    assert max(tri_xle.data) == approx(100.000)
    assert min(tri_xle.index) == pd.Timestamp('2017-01-03 16:00:00-05:00')
    assert max(tri_xle.index) == pd.Timestamp('2017-11-30 16:00:00-05:00')

    tri_xlre = metrun.containers[0].metrics[(pairs[2].dependent.symbol_tuple, pairs[2].independent.symbol_tuple),
                                            'metric'].data

    assert len(tri_xlre) == 231
    assert tri_xlre.data.count(None) == 20
    assert sum(filter(None, tri_xlre.data)) == approx(21044.23885120)
    assert min(filter(None, tri_xlre.data)) == approx(98.08653288)
    assert max(filter(None, tri_xlre.data)) == approx(101.33052745)
    assert min(tri_xlre.index) == pd.Timestamp('2017-01-03 16:00:00-05:00')
    assert max(tri_xlre.index) == pd.Timestamp('2017-11-30 16:00:00-05:00')


def test_weighted_ols():
    # setup logging
    # futils.setup_logging(filename='c:/temp/tri_example_equal.log')

    # setup runner
    metrun = runner.MetricRunner(**db_credentials)
    metrun.setup_market_data(data_feed='CsvDataFeed', directory=os.path.join(inst_dir, 'symbol_data'))

    # setup symbols
    symbol_pairs = pd.DataFrame({'dependent_product_type': ['stock', 'stock'],
                                 'dependent_symbol': ['XLF.1C', 'XLE.1C'],
                                 'dependent_component': ['close', 'close'],
                                 'independent_product_type': ['stock', 'stock'],
                                 'independent_symbol': ['SPY.1C', 'SPY.1C'],
                                 'independent_component': ['close', 'close'],
                                 'frequency': ['1D', '1D']})
    pairs = datautils.df_to_pairs_list(symbol_pairs)

    # EqualDollarWeight regression metric
    metrun.add_metrics(mc.TRIContainer, ExpWeightedOrdinaryLeastSquares, pairs,
                       lag_bars=3, window_length=10, half_life=5)

    # setup the datetimes. Force the close time to be 16:00 even on early closes until the database is updated
    datetimes = metrun.bartimes(pd.Timestamp('2017-01-03', tz='America/New_York'),
                                pd.Timestamp('2017-11-30', tz='America/New_York'),
                                include_open=False, default_close=True)

    # run it all
    metrun.run(datetimes)

    # compare aggregate values
    tri_xlf = metrun.containers[0].metrics[(pairs[0].dependent.symbol_tuple, pairs[0].independent.symbol_tuple),
                                           'metric'].data

    assert len(tri_xlf) == 231
    assert sum(tri_xlf.data[3:]) == approx(22394.82943222)
    assert min(tri_xlf.data[3:]) == approx(95.92073346)
    assert max(tri_xlf.data[3:]) == approx(100.00000000)
    assert min(tri_xlf.index) == pd.Timestamp('2017-01-03 16:00:00-05:00')
    assert max(tri_xlf.index) == pd.Timestamp('2017-11-30 16:00:00-05:00')

    tri_xle = metrun.containers[0].metrics[(pairs[1].dependent.symbol_tuple, pairs[1].independent.symbol_tuple),
                                           'metric'].data

    assert len(tri_xle) == 231
    assert sum(tri_xle.data[3:]) == approx(19940.60069486)
    assert min(tri_xle.data[3:]) == approx(78.23426176)
    assert max(tri_xle.data[3:]) == approx(100.00000000)
    assert min(tri_xle.index) == pd.Timestamp('2017-01-03 16:00:00-05:00')
    assert max(tri_xle.index) == pd.Timestamp('2017-11-30 16:00:00-05:00')
