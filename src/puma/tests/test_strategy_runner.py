"""
Test of the Runner class
"""

import collections
import os
from pathlib import Path

import pandas as pd
import pytest
import raccoon as rc
from pandas.testing import assert_index_equal
from pytest import approx
from raccoon.utils import assert_frame_equal

import data as datalib
import database.utils as dbutils
import metric as metric
import puma as tw
import puma.runner as runner

from utils.datetime import NYC, default_time_zone
from database import strategydb, tapdb, metadb
from puma.strategy import Strategy
from puma.utils import assert_persisted_dfs

# Global variables
data_dir = ''
inst_dir = ''

test_login = {}
temp_strategydb = None


def setup_module():
    global data_dir, inst_dir, test_login, temp_strategydb
    data_dir = Path(__file__).parent.parent.parent / "data/tests/inst/csv_data_feed"
    inst_dir = Path(__file__) / "inst"

    # setup temp DBs
    tapdb.delete_db("temp")
    strategydb.delete_db("temp")
    strategydb.create_db("temp")
    tapdb.create_db("temp")
    temp_tapdb = tapdb.engine(host="temp")
    temp_strategydb = strategydb.engine(host="temp")
    temp_strategydb = strategydb.engine(host="temp")

    # attach the stock symbolDB
    metadb.delete_db("temp", "stock")
    metadb.create_db("temp", "stock")
    seng = metadb.engine("temp", "stock")
    dbutils.attach_schema(temp_tapdb, "stock", "temp")

    # setup default data
    dbutils.upload_name(seng, "symbol", "test.sym.1")
    dbutils.upload_name(seng, "symbol", "test.sym.2")
    strategydb.insert_strategy(temp_strategydb, "test_04", "examples.strategy_examples", "UnitTest_04")
    tapdb.insert_source(temp_tapdb, "test_unit")

    # dispose of unneeded engines
    seng.dispose()
    temp_tapdb.dispose()


def teardown_module():
    temp_strategydb.dispose()


def test_construction():
    simrun = runner.SimRunner(host="temp")

    assert isinstance(simrun.risk, tw.Risk)
    assert isinstance(simrun.order_manager, tw.OrderManager)
    assert isinstance(simrun.position_manager, tw.PositionManager)

    # confirm the tapdb is there and name inserted, first as the default simulation
    assert dbutils.name_exists(simrun.tapdb_engine, 'source', 'simulation')

    simrun = runner.SimRunner(host="temp", runner_id='test_runner_99')
    assert simrun.id == 'test_runner_99'
    assert dbutils.name_exists(simrun.tapdb_engine, 'source', 'test_runner_99')


def test_setup_market_data():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)

    assert isinstance(simrun.market_data_manager, datalib.MarketDataManager)
    assert simrun.time_zone == default_time_zone

    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir, time_zone='EST')
    assert simrun.time_zone == 'EST'


def test_add_strategies():
    simrun = runner.SimRunner(host="temp")
    strat_df = rc.DataFrame({'module_name': 'puma.puma.strategy', 'class_name': 'ExampleStrategy',
                             'strategy_id': 'strat_01', 'portfolio_id': 'port_01'})

    # test that adding strategies before defining market data raises error
    with pytest.raises(RuntimeError):
        simrun.add_strategies(strat_df)

    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(strat_df)
    assert isinstance(simrun.strategies.pop('strat_01'), Strategy)
    assert isinstance(simrun.portfolios.pop('port_01'), tw.Portfolio)


def test_add_symbols():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'puma.puma.strategy', 'class_name': 'ExampleStrategy',
                                        'strategy_id': 'test_01', 'portfolio_id': 'port_01'}))

    symbols = rc.DataFrame({'strategy_id': ['test_01', 'test_01'], 'product_type': ['stock', 'stock'],
                            'symbol_name': ['test.sym.1', 'test.sym.2'], 'frequency': ['1min', '1D']})
    simrun.add_symbols(symbols)

    assert sorted(simrun.frequencies()) == ['1D', '1min']
    assert simrun.min_frequency() == '1min'
    assert simrun.product_types() == ['stock']

    # test bad DataFrame, missing column
    symbols = rc.DataFrame({'strategy_id': ['test_01', 'test_01'],
                            'symbol_name': ['test.sym.1', 'test.sym.2'], 'frequency': ['1min', '1D']})
    with pytest.raises(ValueError):
        simrun.add_symbols(symbols)

    # test trying to add symbols for strategies that do not exist
    symbols = rc.DataFrame({'strategy_id': ['BAD_ID', 'test_01'], 'product_type': ['stock', 'stock'],
                            'symbol_name': ['test.sym.1', 'test.sym.2'], 'frequency': ['1min', '1D']})
    with pytest.raises(KeyError):
        simrun.add_symbols(symbols)

    # try adding bad symbol
    symbols = rc.DataFrame({'strategy_id': ['test_01'], 'product_type': ['stock'],
                            'symbol_name': ['NotInDB'], 'frequency': ['1D']})
    with pytest.raises(AttributeError):
        simrun.add_symbols(symbols)


def test_set_parameters():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': ['puma.puma.strategy', 'puma.puma.strategy'],
                                        'class_name': ['ExampleStrategy', 'ExampleStrategy'],
                                        'strategy_id': ['test_01', 'test_02'], 'portfolio_id': ['port_01', 'port_02']}))

    params = {'test_01': {'param1': 1, 'param2': 99}, 'test_02': {'param3': 33, 'param4': 'BUY'}}
    simrun.set_parameters(params)

    assert simrun.strategies['test_01'].parameters == params['test_01']
    assert simrun.strategies['test_02'].parameters == params['test_02']


def test_bartimes_daily():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'puma.puma.strategy', 'class_name': 'ExampleStrategy',
                                        'strategy_id': 'test_01', 'portfolio_id': 'port_01'}))
    symbols = pd.DataFrame({'strategy_id': ['test_01'], 'product_type': ['stock'], 'symbol_name': ['test.sym.1'],
                            'frequency': ['1D']})
    simrun.add_symbols(symbols)

    # Test daily, note that the 1st is missing for the holiday, as are the 5th and 6th for weekend
    expected = pd.DatetimeIndex([pd.Timestamp(x + ' 16:00', tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02', '1991-01-03', '1991-01-04', '1991-01-07']], freq='B')
    actual = simrun.bartimes(pd.Timestamp('1991-01-01'), pd.Timestamp('1991-01-07'), include_open=False)
    assert_index_equal(actual, expected)


def test_bartimes_minute():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'puma.puma.strategy', 'class_name': 'ExampleStrategy',
                                        'strategy_id': 'test_01', 'portfolio_id': 'port_01'}))
    symbols = pd.DataFrame({'strategy_id': ['test_01'], 'product_type': ['stock'], 'symbol_name': ['test.sym.1'],
                            'frequency': ['1min']})
    simrun.add_symbols(symbols)

    # Test minute inside one day including the open bar
    expected = pd.DatetimeIndex([pd.Timestamp(x, tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02 09:30:00', '1991-01-02 09:31:00', '1991-01-02 09:32:00']],
                                freq='T')
    actual = simrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC),
                             pd.Timestamp('1991-01-02 09:32:00', tz=NYC))
    assert_index_equal(actual, expected)

    # Test minute inside one day not including the open bar
    expected = pd.DatetimeIndex([pd.Timestamp(x, tz=default_time_zone).tz_convert('UTC')
                                 for x in ['1991-01-02 09:31:00', '1991-01-02 09:32:00']],
                                freq='T')
    actual = simrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC),
                             pd.Timestamp('1991-01-02 09:32:00', tz=NYC), include_open=False)
    assert_index_equal(actual, expected)

    # Error out if no time zone on the start_datetime or end_datetime
    with pytest.raises(ValueError):
        simrun.bartimes(pd.Timestamp('1991-01-02 09:30:00'), pd.Timestamp('1991-01-02 09:32:00', tz=NYC))

    with pytest.raises(ValueError):
        simrun.bartimes(pd.Timestamp('1991-01-02 09:30:00', tz=NYC), pd.Timestamp('1991-01-02 09:32:00'))


def test_run():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test.log')

    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'puma.puma.strategy', 'class_name': 'ExampleStrategy',
                                        'strategy_id': 'test.example', 'portfolio_id': 'port_01'}))

    symbols = rc.DataFrame({'strategy_id': ['test.example'] * 3, 'product_type': ['stock'] * 3,
                            'symbol_name': ['AAPL', 'MSFT', 'test.sym.3'], 'frequency': ['1min'] * 3})
    simrun.add_symbols(symbols)
    datetimes = pd.date_range('2010-01-01 09:30:00', '2010-01-01 09:35:00', freq='1min', tz=NYC)
    simrun.run(datetimes)

    pos_df = simrun.position_manager.positions_df

    expected = rc.DataFrame({'buy_avg_price': 52.52, 'buy_quantity': 25.0, 'current_position': 25.0,
                             'sell_avg_price': 0.0, 'sell_quantity': 0.0}, index=[('test.example', 'stock', 'AAPL')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(indexes=('test.example', 'stock', 'AAPL'), columns=expected.columns), expected)

    expected = rc.DataFrame({'buy_avg_price': 43.5, 'buy_quantity': 50.0, 'current_position': 0.0,
                             'sell_avg_price': 44.5, 'sell_quantity': 50.0}, index=[('test.example', 'stock', 'MSFT')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(indexes=('test.example', 'stock', 'MSFT'), columns=expected.columns), expected)

    # test that the strategy was stopped with on_stop
    assert simrun.strategies['test.example'].stopped == pd.Timestamp('2010-01-01 09:35:00', tz=NYC)


def test_run_with_cancels_partials():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'examples.strategy_examples', 'class_name': 'UnitTest_01',
                                        'strategy_id': 'test_01', 'portfolio_id': 'port_01'}))

    symbols = rc.DataFrame({'strategy_id': ['test_01'], 'product_type': ['stock'],
                            'symbol_name': ['test.sym.9'], 'frequency': ['1min']})
    simrun.add_symbols(symbols)

    pnl = metric.PositionManagerMetric(simrun.market_data_manager, simrun.position_manager, 'net_pnl', sum)
    equity = metric.Accumulate(simrun.market_data_manager, pnl)
    simrun.add_eod_metrics(collections.OrderedDict([('equity', equity)]))

    bartimes = simrun.bartimes(pd.Timestamp('2010-01-04 09:30:00', tz=NYC),
                               pd.Timestamp('2010-01-04 09:41:00', tz=NYC), include_open=True)
    simrun.run(bartimes)

    pos_df = simrun.position_manager.positions_df

    # check positions
    expected = rc.DataFrame({'buy_avg_price': 51.59285714285714, 'buy_quantity': 175.0, 'current_position': 42.0,
                             'sell_avg_price': 52.26533834586466, 'sell_quantity': 133.0},
                            index=[('test_01', 'stock', 'test.sym.9')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_01', 'stock', 'test.sym.9'), expected.columns), expected)

    # test the number of fills
    assert len(simrun.position_manager.new_trades) == 7

    # test PnL
    expected_pnl = (25 * (52.25 - 51.75) + 50 * (52.23 - 51.5)) + 100 * (52.23 - 51.6) + 52 * (52.02 - 52.23) + \
                   56 * (52.5 - 52.23) - 308 * 0.01
    assert simrun.position_manager.get_value('test_01', 'stock', 'test.sym.9', 'net_pnl') == approx(expected_pnl)

    # check open orders
    expected_open = rc.DataFrame({'state': ['PARTIALLY_FILLED'], 'buy_sell': ['sell'], 'quantity': [85],
                                  'fill_quantity': [56], 'details': [{'price': 52.5}], 'closed': [False],
                                  'booked': [True]})
    actual_open = simrun.order_manager.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # check closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED',
                                              'CANCELED', 'FILLED', 'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy', 'sell', 'buy', 'sell'],
                                    'fill_quantity': [None, 25, None, 25, None, 50, None, 100, 52],
                                    'fill_price': [None, 51.75, None, 52.25, None, 51.5, None, 51.6, 52.02],
                                    'closed': [True, True, True, True, True, True, True, True, True],
                                    'booked': [None, True, None, True, None, True, None, True, True]})
    actual_closed = simrun.order_manager.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # check metrics get called on .stop()
    assert simrun.position_manager.eod_metrics['equity'][0] == approx(expected_pnl)


def test_run_1d_eod_bod():
    # Setup strategies
    strategies = pd.DataFrame({'strategy_id': ['test_04'], 'portfolio_id': 'port_01'})

    # use the StrategyDB to get the strategy details required
    details = strategydb.get_strategies(temp_strategydb)
    strategies = strategies.merge(details, left_on='strategy_id', right_on='strategy_name').drop('strategy_name',
                                                                                                 axis=1)
    # Setup SimRunner
    simrun = runner.SimRunner(host="temp", runner_id='test_1d')
    simrun.setup_market_data(data_feed='CsvDataFeed', directory=data_dir, live_frequency='1D')
    simrun.add_strategies(strategies)

    symbols = rc.DataFrame({'strategy_id': ['test_04'] * 2, 'product_type': ['stock'] * 2,
                            'symbol_name': ['test.sym.9', 'test.sym.10'], 'frequency': ['1D'] * 2})
    simrun.add_symbols(symbols)

    pnl = metric.PositionManagerMetric(simrun.market_data_manager, simrun.position_manager, 'gross_pnl', sum)
    equity = metric.Accumulate(simrun.market_data_manager, pnl)
    simrun.add_eod_metrics(collections.OrderedDict([('equity', equity)]))

    bartimes = simrun.bartimes(pd.Timestamp('2009-12-31 09:30:00', tz=NYC),
                               pd.Timestamp('2010-01-05 16:00:00', tz=NYC))

    # Run the simulation
    simrun.run(bartimes)

    # check the results
    assert_persisted_dfs(simrun.tapdb_engine, os.path.join(inst_dir, 'UnitTest_04'), simrun.id,
                         bartimes[3].tz_convert('America/New_York'))
    assert_persisted_dfs(simrun.tapdb_engine, os.path.join(inst_dir, 'UnitTest_04'), simrun.id, '2010-01-05 16:00:00')

    # check the metrics
    assert simrun.position_manager.eod_metrics['equity'][0] == approx(639.75)
    assert simrun.position_manager.eod_metrics['equity'][-1] == approx(924)
