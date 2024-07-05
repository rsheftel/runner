"""
Unit / end-to-end testing using examples strategies
"""

import collections
from collections import namedtuple
from pathlib import Path

import numpy as np
import pandas as pd
import raccoon as rc
from pytest import approx
from raccoon.utils import assert_frame_equal

from database import tapdb, strategydb, metadb
from database import utils as dbutils
import examples.strategy_examples
import data as datalib
import metric
import puma as tw
import puma.runner as runner

from utils.datetime import NYC, default_time_zone
from puma.utils import assert_positions_df
from utils.collections import aggregate_rc

# Global variables
inst_dir = Path()
csv_data_dir = Path()


def setup_module():
    global inst_dir, csv_data_dir
    inst_dir = Path(__file__).parent / "inst"
    csv_data_dir = Path(__file__).parent.parent.parent / "data/tests/inst/csv_data_feed"

    # setup temp DBs
    tapdb.delete_db("temp")
    strategydb.delete_db("temp")
    strategydb.create_db("temp")
    tapdb.create_db("temp")
    temp_tapdb = tapdb.engine(host="temp")
    temp_strategydb = strategydb.engine(host="temp")

    # attach the stock symbolDB
    metadb.delete_db("temp", "stock")
    metadb.create_db("temp", "stock")
    seng = metadb.engine("temp", "stock")
    dbutils.attach_schema(temp_tapdb, "stock", "temp")

    # setup default data
    dbutils.upload_name(seng, "symbol", "test.sym.9")
    dbutils.upload_name(seng, "symbol", "test.sym.10")
    dbutils.upload_name(seng, "symbol", "test.sym.11")
    strategydb.insert_strategy(temp_strategydb, "test_01", "examples.strategy_examples", "UnitTest_01")
    strategydb.insert_strategy(temp_strategydb, "test_02", "examples.strategy_examples", "UnitTest_02")
    strategydb.insert_strategy(temp_strategydb, "test_03", "examples.strategy_examples", "UnitTest_03")
    strategydb.insert_strategy(temp_strategydb, "test_05", "examples.strategy_examples", "UnitTest_05")
    tapdb.insert_source(temp_tapdb, "test_unit")

    # dispose of unneeded engines
    seng.dispose()
    temp_tapdb.dispose()
    temp_strategydb.dispose()


def test_event_loop_w_replaces():
    # setup logging
    # futils.setup_logging()

    # setup all the environment objects
    oms = tw.OrderManager('unit_test', None)
    tap = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, tap)
    risk = tw.Risk(oms)

    # Paper broker and paper exchange
    exchange = tw.PaperExchange()
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # Setup market data
    datafeed = datalib.CsvDataFeed(csv_data_dir)
    hdm = datalib.HistoricalDataManager(datafeed, host="temp")
    ldm = datalib.LiveDataManager(datafeed, host="temp")
    mdm = datalib.MarketDataManager(hdm, ldm)

    # Now attach and link the objects to each other
    tap.setup_market_data(mdm)
    port.setup_market_data(mdm)

    # set up the strategy
    objects = namedtuple('OB', 'order_manager, market_data_manager, position_manager')(oms, mdm, tap)
    strat = examples.strategy_examples.UnitTest_02('test_02', objects)

    # Attached the strategy to the Portfolio
    port.add_strategy(strat)

    # Add the symbols to the strategy
    strat.add_symbols([('stock', 'test.sym.9', '1min')])
    strat.add_symbols([('stock', 'test.sym.10', '1min')])

    # Set the strategy parameters
    strat.set_parameters({'start_bar': 1})

    # Initialize the event loop
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # start the strategies
    strat.start()

    # put the market in open state
    oms.market_state('stock', True)

    # test that processing fills with nothing to fill just passes
    event_loop.process_fills()

    # test that processing cancels with nothing just passes
    event_loop.process_cancels()

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.10'], 'state': ['SENT'], 'buy_sell': ['sell'],
                                  'quantity': [50],
                                  'details': [{'price': 44.8}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    assert len(oms.closed_orders_df()) == 0

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.10'], 'state': ['LIVE'], 'buy_sell': ['sell'],
                                  'quantity': [50],
                                  'details': [{'price': 44.8}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    assert len(oms.closed_orders_df()) == 0

    # process the next bar which will create some orders
    mdm.bartime = pd.Timestamp('2010-01-04 09:32:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.10'], 'state': ['SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell'], 'quantity': [100, 100],
                                  'details': [{'price': 50.6}, {'price': 45.5}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10'], 'state': ['FILLED'],
                                    'buy_sell': ['sell'],
                                    'fill_quantity': [50],
                                    'fill_price': [44.8],
                                    'closed': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert len(tap.new_trades) == 1

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-50)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == approx(50 * (44.8 - 43.93) - 50 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:33:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.10', 'test.sym.9'],
                                  'state': ['REPLACE_SENT', 'REPLACE_SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell', 'buy'], 'quantity': [100, 75, 100],
                                  'details': [{'price': 50.8}, {'price': 45.3}, {'price': 51.0}],
                                  'closed': [False, False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    expected_closed = rc.DataFrame({'symbol': ['test.sym.10'], 'state': ['FILLED'],
                                    'buy_sell': ['sell'],
                                    'fill_quantity': [50],
                                    'fill_price': [44.8],
                                    'closed': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert len(tap.new_trades) == 1

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-50)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == approx(50 * (44.8 - 44.43) - 50 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:34:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'],
                                  'state': ['REPLACE_SENT', 'REPLACE_SENT'],
                                  'buy_sell': ['buy', 'buy'], 'quantity': [100, 50],
                                  'details': [{'price': 51.7}, {'price': 51.0}], 'closed': [False, False],
                                  'booked': [None, True], 'fill_quantity': [None, 54], 'fill_price': [None, 51.0]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.10'], 'state': ['FILLED', 'FILLED'],
                                    'buy_sell': ['sell', 'sell'], 'fill_quantity': [50, 75],
                                    'fill_price': [44.8, 45.3], 'closed': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert len(tap.new_trades) == 3

    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(54)
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == approx(54 * (51.92 - 51.0) - 54 * 0.01)

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-125)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.8 - 44.82) + 75 * (45.3 - 44.82) - 125 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:35:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [100], 'details': [{'price': 51.75}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.10', 'test.sym.9'],
                                    'state': ['FILLED', 'FILLED', 'FILLED'],
                                    'buy_sell': ['sell', 'sell', 'buy'], 'fill_quantity': [50, 75, 54],
                                    'fill_price': [44.8, 45.3, 51.0], 'closed': [True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert len(tap.new_trades) == 3

    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(54)
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == approx(54 * (51.84 - 51.0) - 54 * 0.01)

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-125)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.8 - 44.94) + 75 * (45.3 - 44.94) - 125 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:36:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.10'], 'state': ['REPLACE_SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell'], 'quantity': [80, 50],
                                  'details': [{'price': 51.5}, {'price': 44.5}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.10', 'test.sym.9'],
                                    'state': ['FILLED', 'FILLED', 'FILLED'],
                                    'buy_sell': ['sell', 'sell', 'buy'], 'fill_quantity': [50, 75, 54],
                                    'fill_price': [44.8, 45.3, 51.0], 'closed': [True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test booked orders
    assert len(tap.new_trades) == 4

    # test PnL
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(114)
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == \
           approx(60 * (51.83 - 51.75) + 54 * (51.83 - 51.0) - 114 * 0.01)

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-125)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.8 - 44.25) + 75 * (45.3 - 44.25) - 125 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:37:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    assert len(oms.open_orders_df()) == 0

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.9', 'test.sym.10', 'test.sym.9', 'test.sym.10'],
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'FILLED', 'FILLED'],
                                    'buy_sell': ['sell', 'buy', 'sell', 'buy', 'sell'],
                                    'fill_quantity': [50, 80, 75, 54, 50],
                                    'fill_price': [44.8, (60 * 51.75 + 20 * 51.5) / 80, 45.3, 51.0, 44.5],
                                    'closed': [True, True, True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test booked orders
    assert len(tap.new_trades) == 6

    # test PnL
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(134)
    assert tap.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == \
           approx(80 * (51.16 - (60 * 51.75 + 20 * 51.5) / 80) + 54 * (51.16 - 51.0) - 134 * 0.01)

    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-175)
    assert tap.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.50 - 44.05) + + 75 * (45.3 - 44.05) + 50 * (44.80 - 44.05) - 175 * 0.01)


def test_runner_w_replaces():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed="CsvDataFeed", directory=csv_data_dir)
    simrun.add_strategies(rc.DataFrame({'module_name': 'examples.strategy_examples', 'class_name': 'UnitTest_02',
                                        'strategy_id': 'test_02', 'portfolio_id': 'port_02'}))

    simrun.set_parameters({'test_02': {'start_bar': 1}})

    symbols = rc.DataFrame({'strategy_id': ['test_02', 'test_02'], 'product_type': ['stock', 'stock'],
                            'symbol_name': ['test.sym.9', 'test.sym.10'], 'frequency': ['1min', '1min']})
    simrun.add_symbols(symbols)

    bartimes = simrun.bartimes(pd.Timestamp('2010-01-04 09:30:00', tz=NYC),
                               pd.Timestamp('2010-01-04 09:37:00', tz=NYC), include_open=True)
    simrun.run(bartimes)

    pos_df = simrun.position_manager.positions_df

    # check positions
    expected = rc.DataFrame({'buy_avg_price': 51.41044776119403, 'buy_quantity': 134, 'current_position': 134,
                             'sell_avg_price': 0.0, 'sell_quantity': 0},
                            index=[('test_02', 'stock', 'test.sym.9')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_02', 'stock', 'test.sym.9'), expected.columns), expected)

    expected = rc.DataFrame({'buy_avg_price': 0.0, 'buy_quantity': 0, 'current_position': -175,
                             'sell_avg_price': 44.92857142857143, 'sell_quantity': 175},
                            index=[('test_02', 'stock', 'test.sym.10')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_02', 'stock', 'test.sym.10'), expected.columns), expected)

    # test the number of fills
    assert len(simrun.position_manager.new_trades) == 6

    # test PnL
    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(134)
    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == \
           approx(80 * (51.16 - (60 * 51.75 + 20 * 51.5) / 80) + 54 * (51.16 - 51.0) - 134 * 0.01)

    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-175)
    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.50 - 44.05) + 75 * (45.3 - 44.05) + 50 * (44.80 - 44.05) - 175 * 0.01)

    # check open orders
    assert len(simrun.order_manager.open_orders_df()) == 0

    # check closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.9', 'test.sym.10', 'test.sym.9', 'test.sym.10'],
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'FILLED', 'FILLED'],
                                    'buy_sell': ['sell', 'buy', 'sell', 'buy', 'sell'],
                                    'fill_quantity': [50, 80, 75, 54, 50],
                                    'fill_price': [44.8, (60 * 51.75 + 20 * 51.5) / 80, 45.3, 51.0, 44.5],
                                    'closed': [True, True, True, True, True]})
    actual_closed = simrun.order_manager.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)
    simrun.exit()


def test_runner_multi_strats():
    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed="CsvDataFeed", directory=csv_data_dir)

    simrun.add_strategies(rc.DataFrame({'module_name': ['examples.strategy_examples', 'examples.strategy_examples'],
                                        'class_name': ['UnitTest_01', 'UnitTest_02'],
                                        'strategy_id': ['test_01', 'test_02'],
                                        'portfolio_id': ['port_SimRunner', 'port_SimRunner']}))

    simrun.set_parameters({'test_02': {'start_bar': 1}})

    symbols = rc.DataFrame({'strategy_id': ['test_01', 'test_02', 'test_02'],
                            'product_type': ['stock', 'stock', 'stock'],
                            'symbol_name': ['test.sym.9', 'test.sym.9', 'test.sym.10'],
                            'frequency': ['1min', '1min', '1min']})
    simrun.add_symbols(symbols)

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

    expected = rc.DataFrame({'buy_avg_price': 51.41044776119403, 'buy_quantity': 134.0, 'current_position': 134.0,
                             'sell_avg_price': 0.0, 'sell_quantity': 0.0},
                            index=[('test_02', 'stock', 'test.sym.9')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_02', 'stock', 'test.sym.9'), expected.columns), expected)

    expected = rc.DataFrame({'buy_avg_price': 0.0, 'buy_quantity': 0.0, 'current_position': -175.0,
                             'sell_avg_price': 44.92857142857143, 'sell_quantity': 175.0},
                            index=[('test_02', 'stock', 'test.sym.10')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_02', 'stock', 'test.sym.10'), expected.columns), expected)

    # aggregate across strategies
    actual_positions = aggregate_rc(pos_df[['current_position']], 'symbol', sum)
    expected_positions = rc.DataFrame({'current_position': [-175, 176]}, index=['test.sym.10', 'test.sym.9'],
                                      index_name='symbol', sort=True)
    assert_frame_equal(actual_positions, expected_positions)

    # test the number of fills
    assert len(simrun.position_manager.new_trades) == 13

    # test PnL
    assert simrun.position_manager.get_value('test_01', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (52.23 - 51.5)) + 100 * (52.23 - 51.6) + 52 * (52.02 - 52.23)
                  + 56 * (52.5 - 52.23) - 308 * 0.01)

    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.9', 'current_position') == approx(134)
    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.9', 'net_pnl') == \
           approx(80 * (52.23 - (60 * 51.75 + 20 * 51.5) / 80) + 54 * (52.23 - 51.0) - 134 * 0.01)

    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.10', 'current_position') == approx(-175)
    assert simrun.position_manager.get_value('test_02', 'stock', 'test.sym.10', 'net_pnl') == \
           approx(50 * (44.50 - 44.38) + 75 * (45.3 - 44.38) + 50 * (44.80 - 44.38) - 175 * 0.01)

    # check open orders
    expected_open = rc.DataFrame({'state': ['PARTIALLY_FILLED'], 'buy_sell': ['sell'], 'quantity': [85],
                                  'fill_quantity': [56], 'details': [{'price': 52.5}], 'closed': [False],
                                  'booked': [True]})
    actual_open = simrun.order_manager.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # check closed orders
    file_data = pd.read_csv(inst_dir / 'runner_multi_strat_closed_orders.csv', index_col=[0])
    file_data = file_data.replace(np.nan, None)
    expected_closed = rc.DataFrame(file_data.to_dict('list'))
    actual_closed = simrun.order_manager.closed_orders_df()[expected_closed.columns]
    actual_closed['details'] = [str(x) for x in actual_closed.get_entire_column('details', as_list=True)]
    assert_frame_equal(actual_closed, expected_closed)
    simrun.exit()


def test_event_loop_intents():
    # setup logging
    # futils.setup_logging()

    # setup all the environment objects
    oms = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_01', oms, pm)
    risk = tw.Risk(oms)

    # Paper broker and paper exchange
    exchange = tw.PaperExchange(live_frequency='5min')
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # Setup market data
    csvdf = datalib.CsvDataFeed(csv_data_dir)
    hdm = datalib.HistoricalDataManager(csvdf, host="temp")
    ldm = datalib.LiveDataManager(csvdf, host="temp")
    mdm = datalib.MarketDataManager(hdm, ldm)

    # Now attach and link the objects to each other
    pm.setup_market_data(mdm, live_frequency='5min')
    port.setup_market_data(mdm, live_frequency='5min')

    # setup the strategy
    objects = namedtuple('OB', 'order_manager, market_data_manager, position_manager')(oms, mdm, pm)
    strat = examples.strategy_examples.UnitTest_03('test_03', objects)

    # Attached the strategy to the Portfolio
    port.add_strategy(strat)

    # Add the symbols to the strategy
    strat.add_symbols([('stock', 'test.sym.9', '5min')])

    # Set the strategy parameters
    strat.set_parameters({'start_bar': 1})

    # Initialize the event loop
    event_loop = tw.EventProcessor([strat], [port], risk, oms, pm, broker, mdm, exchange)

    # put the market in open state
    oms.market_state('stock', True)

    # start the strategies
    strat.start()

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:35:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['SENT'], 'buy_sell': ['buy'],
                                  'quantity': [25], 'details': [{'price': 51}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    assert len(oms.closed_orders_df()) == 0

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [25], 'details': [{'price': 52.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    assert len(oms.closed_orders_df()) == 0

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:45:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [25], 'details': [{'price': 53.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    assert len(oms.closed_orders_df()) == 0

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:50:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    assert len(oms.open_orders_df()) == 0

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['FILLED'], 'buy_sell': ['buy'],
                                    'quantity': [25], 'details': [{'price': 53.0}], 'closed': [True], 'booked': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 25

    # confirm on_filled was not called with any orders
    assert strat.filled_orders is None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 09:55:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['SENT'], 'buy_sell': ['buy'],
                                  'quantity': [10.0], 'details': [{'price': 55.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['FILLED'], 'buy_sell': ['buy'],
                                    'quantity': [25], 'details': [{'price': 53.0}], 'closed': [True], 'booked': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 25

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [20.0], 'details': [{'price': 56.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['FILLED'], 'buy_sell': ['buy'],
                                    'quantity': [25], 'details': [{'price': 53.0}], 'closed': [True], 'booked': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 25

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:05:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [30.0], 'details': [{'price': 57.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['FILLED'], 'buy_sell': ['buy'],
                                    'quantity': [25], 'details': [{'price': 53.0}], 'closed': [True], 'booked': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 25

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:10:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['SENT'], 'buy_sell': ['sell'],
                                  'quantity': [50.0], 'details': [{'price': 52.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'], 'state': ['FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy'], 'quantity': [25.0, 30.0],
                                    'fill_price': [53, 57.0], 'fill_quantity': [25, 30],
                                    'closed': [True, True], 'booked': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 55

    # confirm on_filled was not called with any orders
    assert strat.filled_orders is None

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:15:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['sell'],
                                  'quantity': [40.0], 'details': [{'price': 51.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'], 'state': ['FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy'], 'quantity': [25.0, 30.0],
                                    'fill_price': [53, 57.0], 'fill_quantity': [25, 30],
                                    'closed': [True, True], 'booked': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 55

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:20:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['sell'],
                                  'quantity': [30.0], 'details': [{'price': 50.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'], 'state': ['FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy'], 'quantity': [25.0, 30.0],
                                    'fill_price': [53, 57.0], 'fill_quantity': [25, 30],
                                    'closed': [True, True], 'booked': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 55

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:25:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['SENT'], 'buy_sell': ['buy'],
                                  'quantity': [10.0], 'details': [{'price': 49.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 3, 'state': ['FILLED'] * 3,
                                    'buy_sell': ['buy', 'buy', 'sell'], 'quantity': [25.0, 30.0, 30.0],
                                    'fill_price': [53, 57.0, 50.0], 'fill_quantity': [25, 30, 30],
                                    'closed': [True] * 3, 'booked': [True] * 3})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # confirm no fills from intents invoke the on_fill() method
    assert strat.filled_orders is None

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 25
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'gross_pnl') == approx(30 * (50 - 57) + 25 * (49 - 53))
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'net_pnl') == approx(30 * -7 + 25 * -4 + 85 * -0.01)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'] * 2, 'state': ['CANCEL_SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell'], 'quantity': [10.0, 10.0],
                                  'details': [{'price': 49.0}, {'price': 50.0}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:35:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['sell'],
                                  'quantity': [10.0], 'details': [{'price': 50.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 4, 'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy'], 'quantity': [25.0, 30.0, 30.0, 10.0],
                                    'fill_price': [53, 57.0, 50.0, None], 'fill_quantity': [25, 30, 30, None],
                                    'closed': [True] * 4, 'booked': [True, True, True, None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'] * 2, 'state': ['CANCEL_SENT', 'SENT'],
                                  'buy_sell': ['sell', 'buy'], 'quantity': [10.0, 5.0],
                                  'details': [{'price': 50.0}, {'price': 50.0}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # check that orders not in the orders_list exposed to strategy
    assert strat.orders_list() == []

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:45:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['SENT'], 'buy_sell': ['buy'],
                                  'quantity': [70.0], 'details': [{'price': 50.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 6,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0],
                                    'closed': [True] * 6, 'booked': [True, True, True, None, None, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # confirm no fills from intents invoke the on_fill() method
    assert strat.filled_orders is None

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 30
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'gross_pnl') == approx(30 * (50 - 57) + 25 * (50 - 53))

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:50:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [70.0], 'details': [{'price': 50.5}],
                                  'closed': [False], 'booked': [True]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 6,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0],
                                    'closed': [True] * 6, 'booked': [True, True, True, None, None, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # confirm no fills from intents invoke the on_fill() method
    assert strat.filled_orders is None

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 40

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 10:55:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['REPLACE_SENT'], 'buy_sell': ['buy'],
                                  'quantity': [30.0], 'details': [{'price': 52.0}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 6,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0],
                                    'closed': [True] * 6, 'booked': [True, True, True, None, None, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 55

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 11:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'] * 2, 'state': ['SENT', 'SENT'], 'buy_sell': ['sell', 'buy'],
                                  'quantity': [50.0, 100.0], 'details': [{'price': 54.5}, {'price': 53.0}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 7,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0, 30.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0, 50.583333333333336],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0, 30.0],
                                    'closed': [True] * 7, 'booked': [True, True, True, None, None, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test that the regular order is visible to strategy
    assert len(strat.orders_list()) == 1
    assert strat.orders_list()[0].uuid == strat.order_1

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 60

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 11:05:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'] * 2, 'state': ['SENT', 'SENT'], 'buy_sell': ['buy', 'sell'],
                                  'quantity': [50.0, 10.0], 'details': [{'price': 51.5}, {'price': 54.5}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 9,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED', 'FILLED',
                                              'FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy', 'buy', 'sell', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0, 30.0, 50.0, 100.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0, 50.583333333333336, 54.5, 53.0],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0, 30.0, 50.0, 100.0],
                                    'closed': [True] * 9,
                                    'booked': [True, True, True, None, None, True, True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test that the regular order is called in on_fills
    # noinspection PyTypeChecker
    assert len(strat.filled_orders) == 1
    assert strat.filled_orders[0].uuid == strat.order_1

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 110

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 11:10:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    expected_open = rc.DataFrame({'symbol': ['test.sym.9'] * 2, 'state': ['CANCEL_SENT', 'CANCEL_SENT'],
                                  'buy_sell': ['buy', 'sell'],
                                  'quantity': [50.0, 10.0], 'details': [{'price': 51.5}, {'price': 54.5}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 10,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED', 'FILLED',
                                              'FILLED', 'FILLED', 'RISK_REJECTED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy', 'buy', 'sell',
                                                 'buy', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0, 30.0, 50.0, 100.0, 490.0],
                                    'fill_price': [53, 57.0, 50.0, None, None,
                                                   50.0, 50.583333333333336, 54.5, 53.0, None],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0, 30.0, 50.0, 100.0, None],
                                    'closed': [True] * 10,
                                    'booked': [True, True, True, None, None, True, True, True, True, None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 110

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    mdm.bartime = pd.Timestamp('2010-01-04 11:15:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')

    # test open orders
    assert len(oms.open_orders_df()) == 0

    # test closed orders
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'] * 12,
                                    'state': ['FILLED', 'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'FILLED', 'FILLED',
                                              'FILLED', 'FILLED', 'CANCELED', 'CANCELED', 'RISK_REJECTED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'buy', 'sell', 'buy', 'buy', 'sell', 'buy',
                                                 'buy', 'sell', 'buy'],
                                    'quantity': [25.0, 30.0, 30.0, 10.0, 10.0, 5.0, 30.0, 50.0, 100.0, 50.0, 10.0,
                                                 490.0],
                                    'fill_price': [53, 57.0, 50.0, None, None, 50.0, 50.583333333333336, 54.5, 53.0,
                                                   None,
                                                   None, None],
                                    'fill_quantity': [25, 30, 30, None, None, 5.0, 30.0, 50.0, 100.0, None, None, None],
                                    'closed': [True] * 12,
                                    'booked': [True, True, True, None, None, True, True, True, True, None, None, None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test that the regular order is called in on_cancel
    assert len(strat.canceled_orders) == 1
    assert strat.canceled_orders[0].uuid == strat.order_2

    # test positions & PnL
    assert pm.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == 110


def test_runner_intents():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test_runner_intents.log')

    simrun = runner.SimRunner(host="temp")
    simrun.setup_market_data(data_feed="CsvDataFeed", directory=csv_data_dir, live_frequency='5min')
    simrun.add_strategies(rc.DataFrame({'module_name': 'examples.strategy_examples', 'class_name': 'UnitTest_03',
                                        'strategy_id': 'test_03', 'portfolio_id': 'port_SimRunner'}))

    symbols = rc.DataFrame({'strategy_id': ['test_03'],
                            'product_type': ['stock'],
                            'symbol_name': ['test.sym.9'],
                            'frequency': ['5min']})
    simrun.add_symbols(symbols)

    bartimes = simrun.bartimes(pd.Timestamp('2010-01-04 09:35:00', tz=NYC),
                               pd.Timestamp('2010-01-04 11:15:00', tz=NYC), include_open=True)
    simrun.run(bartimes)

    pos_df = simrun.position_manager.positions_df

    # check positions
    expected = rc.DataFrame({'buy_avg_price': 53.171052631578945, 'buy_quantity': 190.0, 'current_position': 110.0,
                             'sell_avg_price': 52.8125, 'sell_quantity': 80.0},
                            index=[('test_03', 'stock', 'test.sym.9')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)
    assert_frame_equal(pos_df.get(('test_03', 'stock', 'test.sym.9'), expected.columns), expected)

    # test the number of fills
    assert len(simrun.position_manager.new_trades) == 9

    # test PnL
    assert simrun.position_manager.get_value('test_03', 'stock', 'test.sym.9', 'net_pnl') == approx(224.80)
    assert simrun.position_manager.get_value('test_03', 'stock', 'test.sym.9', 'current_position') == approx(110)

    # test open orders
    assert len(simrun.order_manager.open_orders_df()) == 0

    # check closed orders
    file_data = pd.read_csv(inst_dir / 'runner_intent_closed_orders.csv', index_col=[0],
                            float_precision='high')
    file_data = file_data.replace(np.nan, None)
    expected_closed = rc.DataFrame(file_data.to_dict('list'))
    actual_closed = simrun.order_manager.closed_orders_df()[expected_closed.columns]
    actual_closed['details'] = [str(x) for x in actual_closed.get_entire_column('details', as_list=True)]
    assert_frame_equal(actual_closed, expected_closed)
    simrun.exit()


def test_metric_strategy():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test_metric_strategy.log')

    simrun = runner.SimRunner(host="temp", runner_id='simulation')
    simrun.setup_market_data(data_feed="CsvDataFeed", directory=csv_data_dir)

    simrun.add_strategies(rc.DataFrame({'module_name': 'examples.strategy_examples', 'class_name': 'UnitTest_05',
                                        'strategy_id': 'test_05', 'portfolio_id': 'port_SimRunner'}))

    simrun.set_parameters({'test_05': {'half_life': 15}})

    symbols = rc.DataFrame({'strategy_id': ['test_05'] * 3,
                            'product_type': ['stock'] * 3,
                            'symbol_name': ['test.sym.9', 'test.sym.10', 'test.sym.11'],
                            'frequency': ['1min'] * 3})
    simrun.add_symbols(symbols)

    pnl = {}
    equity = {}
    for symbol in symbols['symbol_name'].to_list():
        pnl[symbol] = metric.PositionManagerMetric(simrun.market_data_manager, simrun.position_manager, 'gross_pnl',
                                                   sum, symbol=symbol)
        equity[symbol] = metric.Accumulate(simrun.market_data_manager, pnl[symbol])

    simrun.add_eod_metrics(collections.OrderedDict(list(zip(equity.keys(), equity.values()))))

    bartimes = simrun.bartimes(pd.Timestamp('2010-01-04 09:31:00', tz=NYC),
                               pd.Timestamp('2010-01-08 16:00:00', tz=NYC), include_open=False)
    simrun.run(bartimes)

    # test equity metric from each calculated from PositionManager
    assert equity['test.sym.9'][0] == approx(467.20)
    assert equity['test.sym.10'][0] == approx(241.00)
    assert equity['test.sym.11'][0] == approx(-589.00)

    # test positions DF that are persisted to TAPDB (temp)
    assert_positions_df(simrun.tapdb_engine, inst_dir / 'UnitTest_05', simrun.id,
                        pd.Timestamp('2010-01-04 16:00', tz='America/New_York'))

    assert_positions_df(simrun.tapdb_engine, inst_dir / 'UnitTest_05', simrun.id,
                        pd.Timestamp('2010-01-06 16:00', tz='America/New_York'))

    assert_positions_df(simrun.tapdb_engine, inst_dir / 'UnitTest_05', simrun.id,
                        pd.Timestamp('2010-01-08 16:00', tz='America/New_York'))
    simrun.exit()
