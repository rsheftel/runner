"""
Strategy Event Processor test suite
"""

import os
from collections import namedtuple

import pandas as pd
import pytest
import raccoon as rc
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from pytest import approx
from raccoon.utils import assert_frame_equal

import examples.strategy_examples
import puma.data as datalib
import database.symbol as symboldb
import database.utils as dbutils
import puma.metric as metric
import puma as tw
import puma.strategy as strategy
import utils.collections as cutils
import utils.data as dutils
from config.database import credentials
from config.datetime import NYC, default_time_zone
from puma.data.data_manager import HistoricalDataManager, LiveDataManager
from puma.data.market_data_manager import MarketDataManager
from database import tapdb

# Global variables
inst_dir = None
test_login = {}
db_credentials = {}
seng = None
temp_tapdb = None
prod_tapdb = None


def setup_module():
    global inst_dir, seng, test_login, db_credentials, temp_tapdb, prod_tapdb
    test_login = credentials('test')
    inst_dir = os.path.normpath("./puma/data/tests/inst/")  # the directory of the csv files in test dir
    seng = symboldb.symbol_engine('stock', **test_login, db_host='localhost')
    db_credentials = credentials('test', 'localhost', prefix='db_')

    prod_tapdb = tapdb.tapdb_engine(**test_login, db_host='localhost')
    temp_tapdb = dbutils.make_engine('temp_tapdb', host='localhost')
    dbutils.copy_table_schema(prod_tapdb, temp_tapdb)


def teardown_module():
    seng.dispose()
    temp_tapdb.dispose()
    prod_tapdb.dispose()


def setup_objects_symboldb():
    # setup market data
    mdm = dutils.market_data_manager('SymbolDBDataFeed', engines={'stock': seng}, source='test_source_02',
                                     **db_credentials)

    # setup objects
    oms = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    port.setup_market_data(mdm)
    risk = tw.risk.Risk(oms)
    exchange = tw.exchange.PaperExchange()
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # setup strategies
    objs = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat1 = strategy.ExampleStrategy('strat-01', objs)
    strat1.add_symbol('stock', 'test.sym.10', '1min')
    strat1.add_symbol('stock', 'test.sym.11', '1min')
    strat2 = strategy.ExampleStrategy('strat-02', objs)
    strat2.add_symbol('stock', 'test.sym.10', '1min')
    port.add_strategy(strat1)
    port.add_strategy(strat2)
    return mdm, oms, port, pm, strat1, strat2, risk, exchange, broker


def setup_objects_csv(live_frequency='1min'):
    # Setup market data
    datafeed = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    hmds = HistoricalDataManager(datafeed, **db_credentials)
    lmds = LiveDataManager(datafeed, **db_credentials)
    mdm = MarketDataManager(hmds, lmds)

    # setup all the environment objects
    oms = tw.OrderManager('test_unit', temp_tapdb)
    tap = tw.PositionManager('test_unit', oms, temp_tapdb)
    tap.setup_market_data(mdm, live_frequency)
    port = tw.Portfolio('port_test', oms, tap)
    port.setup_market_data(mdm, live_frequency)
    risk = tw.Risk(oms)

    # Paper broker and paper exchange
    exchange = tw.PaperExchange()
    exchange.live_frequency = live_frequency
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # setup the strategy
    objects = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = tw.strategy.ExampleStrategy('test.example', objects)

    # Attached the strategy to the Portfolio
    port.add_strategy(strat)

    # Add the symbols to the strategy
    strat.add_symbols([('stock', 'test.sym.3', '1min'), ('stock', 'AAPL', '1min'), ('stock', 'MSFT', '1min')])

    return broker, exchange, oms, tap, mdm, strat, port, risk


def test_process_cancels():
    mdm, oms, port, pm, strat1, strat2, risk, exchange, broker = setup_objects_symboldb()
    event_loop = tw.EventProcessor([strat1, strat2], [port], risk, oms, pm, broker, mdm, exchange)

    # setup some orders and intents
    strategy_order = strat1.order('stock', 'test.sym.10', 'buy', 100, 'LIMIT', price=55.5)
    strat1.intent('stock', 'test.sym.11', -50)

    # process orders to turn intent into order
    mdm.bartime = pd.Timestamp('2010-01-04 10:00:00')
    mdm.update('stock', '1min')
    port.process_orders()
    orders = oms.orders_list()
    assert len(orders) == 2

    # now make both canceled but not closed
    for o in orders:
        oms.change_state(o, 'CANCELED')

    assert all(o.state == 'CANCELED' for o in orders)
    assert all(o.closed is False for o in orders)

    # invoke the process_cancels and confirm that only the order made it to the cancel, not the intent
    event_loop.process_cancels()

    # confirm that all orders put into closed state
    assert all(o.closed is True for o in orders)

    # confirm that only the order and not the intent made it to the on_cancel
    cancels = strat1.canceled_orders
    assert len(cancels) == 1
    assert cancels[0] == oms.order(strategy_order)
    assert all(
        c.originator_id == 'strategy.' + strat1.strategy_id for c in cancels
    )

    assert all(c.originator_uuid == strat1.uuid for c in cancels)
    assert all(c.strategy_id == strat1.strategy_id for c in cancels)
    assert all(c.strategy_uuid == strat1.uuid for c in cancels)


def test_process_fills():
    mdm, oms, port, pm, strat1, strat2, risk, exchange, broker = setup_objects_symboldb()
    event_loop = tw.EventProcessor([strat1, strat2], [port], risk, oms, pm, broker, mdm, exchange)

    # setup some orders and intents
    filled_order_id = strat1.order('stock', 'test.sym.10', 'buy', 100, 'LIMIT', price=55.5)
    partial_order_id = strat1.order('stock', 'test.sym.10', 'sell', 75, 'LIMIT', price=40.0)
    strat1.intent('stock', 'test.sym.11', -50)
    strat1.intent('stock', 'test.sym.10', 100)

    # process orders to turn intent into order
    mdm.bartime = pd.Timestamp('2010-01-04 10:00:00')
    mdm.update('stock', '1min')
    port.process_orders()
    orders = oms.orders_list()
    assert len(orders) == 4

    # get the orders
    filled_order = oms.order(filled_order_id)
    partial_order = oms.order(partial_order_id)
    filled_intent = port.get_intent('strat-01', 'stock', 'test.sym.11')['order']
    partial_intent = port.get_intent('strat-01', 'stock', 'test.sym.10')['order']

    # now make FILLED and PARTIALLY_FILLED
    for o in [filled_order, filled_intent]:
        o.add_fill('001', pd.Timestamp('2010-01-04 10:00:00', tz=NYC), pd.Timestamp('2010-01-04 10:00:00', tz=NYC),
                   o.quantity, o.details['price'], 0.0)
        oms.change_state(o, 'FILLED')
        oms.set_booked(o, False)

    for o in [partial_order, partial_intent]:
        o.add_fill('001', pd.Timestamp('2010-01-04 10:00:00', tz=NYC), pd.Timestamp('2010-01-04 10:00:00', tz=NYC),
                   10, o.details['price'], 0.0)
        oms.change_state(o, 'PARTIALLY_FILLED')
        oms.set_booked(o, False)

    assert all(o.state in ['FILLED', 'PARTIALLY_FILLED'] for o in orders)
    assert all(o.booked is False for o in orders)
    assert all(o.closed is False for o in orders)

    # invoke the process_fills and confirm that only the order made it to the fills, not the intent
    event_loop.process_fills()

    assert all(o.booked is True for o in orders)
    assert all(o.closed is True for o in [filled_order, filled_intent])
    assert all(o.closed is False for o in [partial_order, partial_intent])

    # confirm that only the order and not the intent made it to the on_fills
    fills = strat1.filled_orders
    assert len(fills) == 2
    assert all(f.originator_id == 'strategy.' + strat1.strategy_id for f in fills)
    assert all(f.originator_uuid == strat1.uuid for f in fills)
    assert all(f.strategy_id == strat1.strategy_id for f in fills)
    assert all(f.strategy_uuid == strat1.uuid for f in fills)
    assert all(f in [partial_order, filled_order] for f in fills)


def test_stuck_orders():
    # Initialize the event loop
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    oms.market_state('stock', True)
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # stick an order into the strategy, move it along the path and confirm stuck raises errors
    strat.order('stock', 'test.sym.3', 'b', 100, 'LIMIT', 50.50)
    assert len(oms.orders_list({'state': 'CREATED'})) == 1
    with pytest.raises(RuntimeError):
        event_loop.check_stuck_orders()

    port.process_orders()
    assert len(oms.orders_list({'state': 'STAGED'})) == 1
    with pytest.raises(RuntimeError):
        event_loop.check_stuck_orders()

    risk.process_portfolio_orders(port)
    assert len(oms.orders_list({'state': 'RISK_ACCEPTED'})) == 1
    with pytest.raises(RuntimeError):
        event_loop.check_stuck_orders()

    # fully process and confirm no issues
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    event_loop.check_stuck_orders()


def test_market_open():
    # Initialize the event loop
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # open the market
    event_loop.market_open(['stock'])
    assert oms.market_state('stock') is True

    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.orders_list({'state': 'SENT'})) == 2


def test_market_close_orders():
    # Initialize the event loop
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    oms.market_state('stock', True)
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # add three orders that will make it to SENT
    ord1 = strat.order('stock', 'test.sym.3', 'b', 100, 'LIMIT', 50.50)
    ord2 = strat.order('stock', 'test.sym.3', 'b', 50, 'LIMIT', 45.40)
    strat.order('stock', 'test.sym.3', 'b', 25, 'LIMIT', 25.25)

    # fully process
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.orders_list({'state': 'LIVE'})) == 1
    assert len(oms.orders_list({'state': 'SENT'})) == 3

    # add a replace and a cancel
    strat.cancel_order(strat.get_order(ord1))
    strat.replace_order(strat.get_order(ord2), 55, price=54.54)
    mdm.bartime = pd.Timestamp('2010-01-01 09:32:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # confirm the open order states prior to running market close
    assert len(oms.orders_list({'state': 'LIVE'})) == 1
    assert len(oms.orders_list({'state': 'SENT'})) == 1
    assert len(oms.orders_list({'state': 'CANCEL_SENT'})) == 1
    assert len(oms.orders_list({'state': 'REPLACE_SENT'})) == 1

    # run the market close process and confirm that everything is in a closed state
    event_loop.market_close(['stock'])
    assert len(oms.orders_list({'state': 'CANCELED'})) == 4
    assert len(oms.open_orders_df()) == 0
    assert oms.orders_df()['closed'].data == [[True] * 7]


def test_market_close_intents():
    # Intent SENT
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv('5min')
    strat.add_symbol('stock', 'test.sym.9', '5min')
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)
    oms.market_state('stock', True)

    # add intent that will make it to SENT
    strat.intent('stock', 'test.sym.9', 50)
    mdm.bartime = pd.Timestamp('2010-01-04 09:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')
    assert oms.orders_df()['state'].data == [['SENT']]

    # confirm market_close process worked
    event_loop.market_close(['stock'])
    assert oms.orders_df()['state'].data == [['CANCELED']]
    assert oms.orders_df()['closed'].data == [[True]]
    assert len(oms.open_orders_df()) == 0
    assert strat.get_intent('stock', 'test.sym.9') is None

    ##########################################################################################################
    # Intent REPLACED
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv('5min')
    strat.add_symbol('stock', 'test.sym.9', '5min')
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # put the market in open state
    oms.market_state('stock', True)

    strat.intent('stock', 'test.sym.9', 50)
    mdm.bartime = pd.Timestamp('2010-01-04 09:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')
    assert oms.orders_df()['state'].data == [['SENT']]

    strat.intent('stock', 'test.sym.9', 50)
    mdm.bartime = pd.Timestamp('2010-01-04 09:45:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')
    assert oms.orders_df()['state'].data == [['REPLACE_SENT']]

    # confirm market_close process worked
    event_loop.market_close(['stock'])
    assert oms.orders_df()['state'].data == [['CANCELED']]
    assert oms.orders_df()['closed'].data == [[True]]
    assert len(oms.open_orders_df()) == 0
    assert strat.get_intent('stock', 'test.sym.9') is None

    ##########################################################################################################
    # Intent CANCELED
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv('5min')
    strat.add_symbol('stock', 'test.sym.9', '5min')
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # put the market in open state
    oms.market_state('stock', True)

    strat.intent('stock', 'test.sym.9', 50)
    mdm.bartime = pd.Timestamp('2010-01-04 09:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')
    assert oms.orders_df()['state'].data == [['SENT']]

    mdm.bartime = pd.Timestamp('2010-01-04 09:45:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '5min')
    assert oms.orders_df()['state'].data == [['CANCEL_SENT']]

    # confirm market_close process worked
    event_loop.market_close(['stock'])
    assert oms.orders_df()['state'].data == [['CANCELED']]
    assert oms.orders_df()['closed'].data == [[True]]
    assert len(oms.open_orders_df()) == 0
    assert strat.get_intent('stock', 'test.sym.9') is None


def test_market_close_errors():
    # Initialize the event loop
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)
    oms.market_state('stock', True)

    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # stick an order into the strategy that has not made it to the exchange raises error
    strat.order('stock', 'test.sym.3', 'b', 100, 'LIMIT', 50.50)
    assert len(oms.orders_list({'state': 'CREATED'})) == 1
    with pytest.raises(RuntimeError):
        event_loop.market_close(['stock'])


def test_stop():
    # Initialize the event loop
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # Put the market state to open
    oms.market_state('stock', True)

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.open_orders_df()) == 2

    # process the next bar which will fill some orders
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # execute stop and check persistence
    event_loop.stop()

    # confirm that orders are persisted but not flushed
    assert len(oms.orders_list()) == 3
    order_cols = ['originator_id', 'strategy_id', 'strategy_uuid', 'originator_id', 'quantity', 'event_type',
                  'product_type', 'symbol', 'buy_sell', 'type', 'details', 'state', 'closed', 'uuid']
    actual = tapdb.get_orders_df(temp_tapdb, 'test_unit', pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone))
    assert_frame_equal(actual[order_cols], oms.orders_df()[order_cols])

    # confirm postions_df persisted
    assert len(tap.positions_df) == 1
    actual = tapdb.get_positions_df(temp_tapdb, 'test_unit', pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone))
    assert_frame_equal(actual, tap.positions_df)

    # confirm the on_stop called on the strategy
    assert strat.start_stop == 0
    assert strat.stopped == pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)


def test_bod_eod():
    # Initialize the event loop
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # Put the market state to open
    oms.market_state('stock', True)

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.open_orders_df()) == 2
    # since one order was risk rejected it is here
    closed_df = oms.closed_orders_df()
    assert len(closed_df) == 1
    assert closed_df.get(0, 'state') == 'RISK_REJECTED'
    assert closed_df.get(0, 'symbol') == 'test.sym.3'

    # process the next bar which will fill some orders
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    closed_df = oms.closed_orders_df()
    assert closed_df.get(1, 'state') == 'FILLED'
    assert closed_df.get(1, 'symbol') == 'AAPL'
    assert closed_df.get(1, 'fill_price') == 52.52
    assert closed_df.get(1, 'fill_quantity') == 25

    open_df = oms.open_orders_df()
    assert open_df.get(0, 'state') == 'LIVE'
    assert open_df.get(0, 'symbol') == 'MSFT'
    assert open_df.get(0, 'details') == {'price': 44.5}
    assert open_df.get(0, 'quantity') == 50
    assert open_df.get(0, 'buy_sell') == 'sell'

    # call market_close()
    event_loop.market_close(['stock'])
    assert oms.market_state('stock') is False
    assert len(oms.open_orders_df()) == 0
    assert len(oms.closed_orders_df()) == 3
    msft_order = oms.orders_list({'symbol': 'MSFT'})[0]
    assert msft_order.state == 'CANCELED'

    # Call end_of_day()
    mdm.bartime = pd.Timestamp('2010-01-01 16:00:00', tz='America/New_York')
    event_loop.end_of_day(['stock'])

    # check PnL
    assert tap.get_value('test.example', 'stock', 'AAPL', 'current_price') == 67.98
    assert tap.get_value('test.example', 'stock', 'AAPL', 'gross_pnl') == approx(25 * (67.98 - 52.52))

    # check TAPDB persistence
    expected = pd.DataFrame({'source': ['test_unit'], 'strategy': ['test.example'], 'product_type': ['stock'],
                             'symbol': ['AAPL'], 'datetime': [pd.Timestamp('2010-01-01 21:00:00', tz='UTC')],
                             'position': [25.0]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(temp_tapdb)
    pd_assert_frame_equal(actual, expected)

    # Check that the OrderManager has cleared out orders
    assert len(oms.orders_df()) == 0
    assert oms.orders_list() == []

    # Roll to the next day and call begin of day
    mdm.bartime = '2010-01-02 09:30:00'
    event_loop.begin_of_day()

    # Check position loading from TAPDB
    # Note that the prior close price is from 2009-12-31 because 2010-01-01 is a stock holiday
    expected = rc.DataFrame({'current_position': [25.0], 'start_position': [25.0],
                             'prior_close_price': [52.0], 'current_price': [None]},
                            index=[('test.example', 'stock', 'AAPL')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    actual = tap.positions_df
    actual = actual[expected.columns]
    assert_frame_equal(actual, expected)

    # Open the market
    event_loop.market_open(['stock'])
    assert oms.market_state('stock') is True


def test_process_bar():
    # Initialize the event loop
    broker, exchange, oms, tap, mdm, strat, port, risk = setup_objects_csv()
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)
    oms.market_state('stock', True)

    # test that processing fills with nothing to fill just passes
    event_loop.process_fills()

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.open_orders_df()) == 2
    # since one order was risk rejected it is here
    closed_df = oms.closed_orders_df()
    assert len(closed_df) == 1
    assert closed_df.get(0, 'state') == 'RISK_REJECTED'
    assert closed_df.get(0, 'symbol') == 'test.sym.3'

    # process the next bar which will fill some orders
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    closed_df = oms.closed_orders_df()
    assert closed_df.get(1, 'state') == 'FILLED'
    assert closed_df.get(1, 'symbol') == 'AAPL'
    assert closed_df.get(1, 'fill_price') == 52.52
    assert closed_df.get(1, 'fill_quantity') == 25

    open_df = oms.open_orders_df()
    assert open_df.get(0, 'state') == 'LIVE'
    assert open_df.get(0, 'symbol') == 'MSFT'
    assert open_df.get(0, 'details') == {'price': 44.5}
    assert open_df.get(0, 'quantity') == 50
    assert open_df.get(0, 'buy_sell') == 'sell'

    # process the next bar which fills the outstanding live orders, causes a new MSFT order to be created on fill
    mdm.bartime = pd.Timestamp('2010-01-01 09:32:00', tz=default_time_zone)
    event_loop.process_bar(['stock', 'future'], '1min')
    assert len(oms.open_orders_df()) == 1
    assert len(oms.closed_orders_df()) == 3

    # process next bar which fills the outstanding MSFT order
    mdm.bartime = pd.Timestamp('2010-01-01 09:33:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert tap.get_value('test.example', 'stock', 'MSFT', 'buy_quantity') == 50
    assert tap.get_value('test.example', 'stock', 'MSFT', 'sell_quantity') == 50
    assert tap.get_value('test.example', 'stock', 'MSFT', 'net_quantity') == 0


def test_multi_portfolios():
    # setup all the environment objects
    oms = tw.OrderManager('unit_test', None)
    tap = tw.PositionManager('pm_test', oms, None)
    risk = tw.Risk(oms)

    # Paper broker and paper exchange
    exchange = tw.PaperExchange()
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # Setup market data
    datafeed = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    hmds = HistoricalDataManager(datafeed, **db_credentials)
    lmds = LiveDataManager(datafeed, **db_credentials)
    mdm = MarketDataManager(hmds, lmds)

    # Now attach and link the objects to each other
    tap.setup_market_data(mdm)

    # setup the strategies
    objects = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat_01 = tw.strategy.ExampleStrategy('test.example', objects)
    strat_02 = tw.strategy.ExampleStrategy('test_02', objects)

    # setup the portfolios
    port_01 = tw.Portfolio('port_01', oms, tap)
    port_02 = tw.Portfolio('port_02', oms, tap)

    # Attached the strategies to the Portfolios
    port_01.add_strategy(strat_01)
    port_02.add_strategy(strat_02)

    # Add the symbols to the strategies
    strat_01.add_symbols([('stock', 'test.sym.3', '1min'), ('stock', 'AAPL', '1min'), ('stock', 'MSFT', '1min')])
    strat_02.add_symbols([('stock', 'test.sym.3', '1min'), ('stock', 'AAPL', '1min'), ('stock', 'MSFT', '1min')])

    # Initialize the event loop
    event_loop = tw.EventProcessor([strat_01, strat_02], [port_01, port_02], risk, oms, tap, broker, mdm, exchange)

    # put the market in open state
    oms.market_state('stock', True)

    # test that processing fills with nothing to fill just passes
    event_loop.process_fills()

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-01 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert len(oms.open_orders_df()) == 4

    assert len(oms.orders_list({'state': tw.order.states()['open'], 'portfolio_id': 'port_01'})) == 2
    assert len(oms.orders_list({'state': tw.order.states()['open'], 'portfolio_id': 'port_02'})) == 2

    assert oms.open_orders_df({'portfolio_id': 'port_01'}).get_entire_column('strategy_id', as_list=True) == \
           ['test.example', 'test.example']
    assert oms.open_orders_df({'portfolio_id': 'port_02'}).get_entire_column('strategy_id', as_list=True) == \
           ['test_02', 'test_02']

    # since one order was risk rejected it is here
    closed_df = oms.closed_orders_df()
    assert len(closed_df) == 2
    assert closed_df.get_entire_column('state', as_list=True) == ['RISK_REJECTED', 'RISK_REJECTED']
    assert closed_df.get_entire_column('symbol', as_list=True) == ['test.sym.3', 'test.sym.3']

    # process the next bar which will fill some orders
    mdm.bartime = pd.Timestamp('2010-01-01 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    closed_df = oms.closed_orders_df({'state': 'FILLED'})
    assert closed_df.get_entire_column('portfolio_id', as_list=True) == ['port_01', 'port_02']

    open_df = oms.open_orders_df({'state': 'LIVE'})
    assert open_df.get_entire_column('portfolio_id', as_list=True) == ['port_01', 'port_02']

    # process the next bar which fills the outstanding live orders, causes a new MSFT order to be created on fill
    mdm.bartime = pd.Timestamp('2010-01-01 09:32:00', tz=default_time_zone)
    event_loop.process_bar(['stock', 'future'], '1min')
    assert len(oms.open_orders_df()) == 2
    assert len(oms.closed_orders_df()) == 6

    # process next bar which fills the outstanding MSFT order
    mdm.bartime = pd.Timestamp('2010-01-01 09:33:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')
    assert tap.get_value('test.example', 'stock', 'MSFT', 'buy_quantity') == 50
    assert tap.get_value('test.example', 'stock', 'MSFT', 'sell_quantity') == 50
    assert tap.get_value('test.example', 'stock', 'MSFT', 'net_quantity') == 0

    assert tap.get_value('test_02', 'stock', 'MSFT', 'buy_quantity') == 50
    assert tap.get_value('test_02', 'stock', 'MSFT', 'sell_quantity') == 50
    assert tap.get_value('test_02', 'stock', 'MSFT', 'net_quantity') == 0

    # get aggregate values
    positions_df = tap.positions_df[['buy_quantity', 'sell_quantity']]
    agg_df = cutils.aggregate_rc(positions_df, 'symbol', sum)
    expected = rc.DataFrame({'buy_quantity': [50, 100], 'sell_quantity': [0, 100]}, index=['AAPL', 'MSFT'],
                            index_name='symbol', sort=True, columns=agg_df.columns)
    assert_frame_equal(agg_df, expected)


def test_process_bar_w_cancel_partials():
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
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # Now attach and link the objects to each other
    tap.setup_market_data(mdm)

    # setup the strategy
    objects = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = examples.strategy_examples.UnitTest_01('test.example', objects)

    # Attached the strategy to the Portfolio
    port.add_strategy(strat)

    # Add the symbols to the strategy
    strat.add_symbols([('stock', 'test.sym.9', '1min')])

    # Initialize the event loop
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # put the market in open state
    oms.market_state('stock', True)

    # start the strategies
    strat.start()

    # test that processing fills with nothing to fill just passes
    event_loop.process_fills()

    # test that processing cancels with nothing just passes
    event_loop.process_cancels()

    # this bar will enter the orders from the strategy
    mdm.bartime = pd.Timestamp('2010-01-04 09:30:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'state': ['SENT', 'SENT'], 'buy_sell': ['buy', 'sell'], 'quantity': [25, 25],
                                  'details': [{'price': 51.75}, {'price': 52.1}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED'], 'buy_sell': ['buy'], 'quantity': [1000],
                                    'details': [{'price': 55.5}], 'closed': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # process the next bar which will fill some orders
    mdm.bartime = pd.Timestamp('2010-01-04 09:31:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'state': ['CANCEL_SENT'], 'buy_sell': ['sell'], 'quantity': [25],
                                  'details': [{'price': 52.1}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy'], 'fill_quantity': [None, 25],
                                    'fill_price': [None, 51.75], 'closed': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == 0.0
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'commission') == -0.25
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == -0.25
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'buy_avg_price') == 51.75

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:32:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'state': ['SENT', 'SENT'], 'buy_sell': ['sell', 'buy'], 'quantity': [25, 100],
                                  'details': [{'price': 52.25}, {'price': 50.5}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell'], 'fill_quantity': [None, 25, None],
                                    'fill_price': [None, 51.75, None], 'closed': [True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == 25 * (51.62 - 51.75)
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'commission') == 25 * -0.01
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == 25 * (51.62 - 51.75 - 0.01)
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'buy_avg_price') == 51.75

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:33:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    expected_open = rc.DataFrame({'state': ['CANCEL_SENT', 'SENT'], 'buy_sell': ['buy', 'buy'], 'quantity': [100, 50],
                                  'details': [{'price': 50.5}, {'price': 51.5}], 'closed': [False, False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell'], 'fill_quantity': [None, 25, None, 25],
                                    'fill_price': [None, 51.75, None, 52.25], 'closed': [True, True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == 25 * (52.25 - 51.75)
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'commission') == 50 * -0.01
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == 25 * (52.25 - 51.75) - 50 * 0.01
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'buy_avg_price') == 51.75
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'sell_avg_price') == 52.25

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:34:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders.
    assert len(oms.open_orders_df()) == 0

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy'],
                                    'fill_quantity': [None, 25, None, 25, None, 50],
                                    'fill_price': [None, 51.75, None, 52.25, None, 51.5],
                                    'closed': [True, True, True, True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == \
           approx(25 * (52.25 - 51.75) + 50 * (51.92 - 51.5))
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx(25 * (52.25 - 51.75) + 50 * (51.92 - 51.5) - 100 * 0.01)
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'buy_quantity') == 75
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'buy_avg_price') == (51.75 * 25 + 51.5 * 50) / 75
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'sell_avg_price') == 52.25

    # process the next bar
    # This bar creates and order and then cancels it before it can get to the exchange
    mdm.bartime = pd.Timestamp('2010-01-04 09:35:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # There are none because the order was canceled before it was sent
    assert len(oms.open_orders_df()) == 0

    # test closed orders
    # The key here is that the CANCEL has happened but it is not closed yet because the calling of the on_cancel
    # occurs at the beginning of the bar process, before the on_bar which is where the order sent/cancel occurred
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED',
                                              'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy', 'sell'],
                                    'fill_quantity': [None, 25, None, 25, None, 50, None],
                                    'fill_price': [None, 51.75, None, 52.25, None, 51.5, None],
                                    'closed': [True, True, True, True, True, True, False]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == \
           approx(25 * (52.25 - 51.75) + 50 * (51.84 - 51.5))
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.84 - 51.5)) - 100 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:36:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # This bar get the on_cancels from the prior bar and creates the new order
    expected_open = rc.DataFrame({'state': ['SENT'], 'buy_sell': ['sell'], 'quantity': [85],
                                  'details': [{'price': 52.5}], 'closed': [False]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test closed orders
    # Now that CANCEL orders enters a True for closed state because the on_cancel has been called
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED',
                                              'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy', 'sell'],
                                    'fill_quantity': [None, 25, None, 25, None, 50, None],
                                    'fill_price': [None, 51.75, None, 52.25, None, 51.5, None],
                                    'closed': [True, True, True, True, True, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.83 - 51.5)) - 100 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:37:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # This bar get the on_cancels from the prior bar and creates the new order
    expected_open = rc.DataFrame({'state': ['LIVE', 'SENT', 'SENT'], 'buy_sell': ['sell', 'buy', 'sell'],
                                  'quantity': [85, 100, 100],
                                  'details': [{'price': 52.5}, {'price': 51.6}, {'price': 52.02}],
                                  'closed': [False, False, False], 'booked': [None, None, None]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test that there are no new closed orders
    assert len(oms.closed_orders_df()) == 7
    # test the number of fills
    assert len(tap.new_trades) == 3

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.16 - 51.5)) - 100 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:38:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # Order #9 and 10 have partial fills
    expected_open = rc.DataFrame({'state': ['LIVE', 'PARTIALLY_FILLED', 'CANCEL_SENT'],
                                  'buy_sell': ['sell', 'buy', 'sell'],
                                  'quantity': [85, 100, 100], 'fill_quantity': [None, 52, 52],
                                  'details': [{'price': 52.5}, {'price': 51.6}, {'price': 52.02}],
                                  'closed': [False, False, False], 'booked': [None, True, True]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test trade entries
    assert len(oms.closed_orders_df()) == 7
    # test the number of fills
    assert len(tap.new_trades) == 5

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.59 - 51.5)) + 52 * (51.59 - 51.6) + 52 * (52.02 - 51.59)
                  - 204 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:39:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # Only the ID8 order still alive
    expected_open = rc.DataFrame({'state': ['LIVE'], 'buy_sell': ['sell'], 'quantity': [85], 'fill_quantity': [None],
                                  'details': [{'price': 52.5}], 'closed': [False], 'booked': [None]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test the number of fills
    assert len(tap.new_trades) == 6

    # test closed orders
    expected_closed = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED',
                                              'CANCELED', 'FILLED', 'CANCELED'],
                                    'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy', 'sell', 'buy', 'sell'],
                                    'fill_quantity': [None, 25, None, 25, None, 50, None, 100, 52],
                                    'fill_price': [None, 51.75, None, 52.25, None, 51.5, None, 51.6, 52.02],
                                    'closed': [True, True, True, True, True, True, True, True, True],
                                    'booked': [None, True, None, True, None, True, None, True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.86 - 51.5)) + 100 * (51.86 - 51.6) + 52 * (52.02 - 51.86)
                  - 252 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:40:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # Only the ID8 order still alive
    expected_open = rc.DataFrame({'state': ['LIVE'], 'buy_sell': ['sell'], 'quantity': [85], 'fill_quantity': [None],
                                  'details': [{'price': 52.5}], 'closed': [False], 'booked': [None]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test the number of fills
    assert len(tap.new_trades) == 6

    # test closed orders
    assert len(oms.closed_orders_df()) == 9

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (51.91 - 51.5)) + 100 * (51.91 - 51.6) + 52 * (52.02 - 51.91)
                  - 252 * 0.01)

    # process the next bar
    mdm.bartime = pd.Timestamp('2010-01-04 09:41:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1min')

    # test open orders
    # ID8 partially filled
    expected_open = rc.DataFrame({'state': ['PARTIALLY_FILLED'], 'buy_sell': ['sell'], 'quantity': [85],
                                  'fill_quantity': [56], 'details': [{'price': 52.5}], 'closed': [False],
                                  'booked': [True]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # test the number of fills
    assert len(tap.new_trades) == 7

    # test closed orders
    assert len(oms.closed_orders_df()) == 9

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'net_pnl') == \
           approx((25 * (52.25 - 51.75) + 50 * (52.23 - 51.5)) + 100 * (52.23 - 51.6) + 52 * (52.02 - 52.23)
                  + 56 * (52.5 - 52.23) - 308 * 0.01)


def test_1d_strategy():
    # setup logging
    # futils.setup_logging(filename='c:/temp/test.log')

    # columns to use for testing orders_df persistence
    order_cols = ['originator_id', 'strategy_id', 'strategy_uuid', 'originator_id', 'quantity', 'event_type',
                  'product_type', 'symbol', 'buy_sell', 'type', 'details', 'state', 'closed', 'uuid']

    # clear out temp_tapdb and refresh tables
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])

    # setup all the environment objects
    oms = tw.OrderManager('test_unit', temp_tapdb)
    tap = tw.PositionManager('test_unit', oms, temp_tapdb)
    port = tw.Portfolio('port_test', oms, tap)
    risk = tw.Risk(oms)

    # Paper broker and paper exchange
    exchange = tw.PaperExchange(live_frequency='1D')  # Since this is daily only strategy, set live frequency to 1D
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # Setup market data
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # Now attach and link the objects to each other
    tap.setup_market_data(mdm, live_frequency='1D')  # Keep the strategy 1D frequency

    # add metrics to position manager
    pnl = metric.PositionManagerMetric(mdm, tap, 'gross_pnl', sum)
    equity = metric.Accumulate(mdm, pnl)
    tap.add_eod_metric(pnl, 'gross_pnl')
    tap.add_eod_metric(equity, 'equity')

    # setup the strategy
    objects = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = examples.strategy_examples.UnitTest_04('test.example', objects)

    # Attached the strategy to the Portfolio
    port.add_strategy(strat)

    # Add the symbols to the strategy
    strat.add_symbols([('stock', 'test.sym.9', '1D'), ('stock', 'test.sym.10', '1D')])

    # Initialize the event loop
    event_loop = tw.EventProcessor([strat], [port], risk, oms, tap, broker, mdm, exchange)

    # start the strategies
    strat.start()

    # first bar open
    mdm.bartime = pd.Timestamp('2009-12-31 09:30:00', tz=default_time_zone)
    event_loop.begin_of_day()
    event_loop.market_open(['stock'])

    assert oms.market_state('stock') is True
    assert oms.orders_list() == []

    event_loop.process_bar(['stock'], '1D')
    assert len(tap.positions_df) == 0

    # first bar process
    mdm.bartime = pd.Timestamp('2009-12-31 16:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1D')
    assert len(tap.positions_df) == 0
    assert oms.orders_list() == []

    # first bar close
    event_loop.market_close(['stock'])
    assert oms.market_state('stock') is False

    # first bar end of day
    orders_snapshot = oms.orders_df()  # capture the orders_df before running EOD that clears the DataFrame
    event_loop.end_of_day(['stock'])

    # orders should be persisted and cleared out
    assert oms.orders_list() == []
    actual = tapdb.get_orders_df(temp_tapdb, oms.id, mdm.bartime)
    assert_frame_equal(actual, orders_snapshot)

    # positions_df persisted
    actual = tapdb.get_positions_df(temp_tapdb, tap.id, mdm.bartime)
    assert_frame_equal(actual, tap.positions_df)

    # second bar open
    mdm.bartime = pd.Timestamp('2010-01-04 09:30:00', tz=default_time_zone)
    event_loop.begin_of_day()
    event_loop.market_open(['stock'])
    event_loop.process_bar(['stock'], '1D')

    expected_open = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'], 'state': ['SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell'], 'quantity': [50, 50], 'fill_quantity': [None, None],
                                  'details': [{'price': 49.5}, {'price': 70.25}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # second bar process
    mdm.bartime = pd.Timestamp('2010-01-04 16:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1D')

    expected_open = rc.DataFrame({'symbol': ['test.sym.9'], 'state': ['LIVE'],
                                  'buy_sell': ['sell'], 'quantity': [50], 'fill_quantity': [None],
                                  'details': [{'price': 70.25}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'],
                                    'state': ['FILLED'],
                                    'buy_sell': ['buy'],
                                    'fill_quantity': [50],
                                    'fill_price': [49.5],
                                    'closed': [True],
                                    'booked': [True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # test PnL
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == approx(924.0)

    # the market_close should cancel outstanding orders
    event_loop.market_close(['stock'])

    assert len(oms.open_orders_df()) == 0
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9', 'test.sym.9'],
                                    'state': ['FILLED', 'CANCELED'],
                                    'buy_sell': ['buy', 'sell'],
                                    'fill_quantity': [50, None],
                                    'fill_price': [49.5, None],
                                    'closed': [True, True],
                                    'booked': [True, None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # Second bar EOD
    orders_snapshot = oms.orders_df()  # capture the orders_df before running EOD that clears the DataFrame
    event_loop.end_of_day(['stock'])

    # orders should be persisted and cleared out
    assert oms.orders_list() == []
    actual = tapdb.get_orders_df(temp_tapdb, oms.id, mdm.bartime)
    assert_frame_equal(actual[order_cols], orders_snapshot[order_cols])

    # positions_df persisted
    saved = tapdb.get_positions_df(temp_tapdb, tap.id, mdm.bartime)
    assert_frame_equal(saved, tap.positions_df)

    # metric calculated
    assert tap.eod_metrics['gross_pnl'][0] == approx(924.0)
    assert tap.eod_metrics['equity'][0] == approx(924.0)

    # open of bar 3
    mdm.bartime = pd.Timestamp('2010-01-05 09:30:00', tz=default_time_zone)
    event_loop.begin_of_day()

    # confirm the positions and pnl
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'start_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'current_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'start_position') is None
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == 0.0

    # open the market and process the bar
    event_loop.market_open(['stock'])
    event_loop.process_bar(['stock'], '1D')

    expected_open = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.10'], 'state': ['SENT', 'SENT'],
                                  'buy_sell': ['buy', 'sell'], 'quantity': [25, 25], 'fill_quantity': [None, None],
                                  'details': [{'price': 46.6}, {'price': 65.25}]})
    actual_open = oms.open_orders_df()[expected_open.columns]
    assert_frame_equal(actual_open, expected_open)

    # bar 3 process
    mdm.bartime = pd.Timestamp('2010-01-05 16:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1D')

    assert len(oms.open_orders_df()) == 0

    expected_closed = rc.DataFrame({'symbol': ['test.sym.10', 'test.sym.10'],
                                    'state': ['FILLED', 'FILLED'],
                                    'buy_sell': ['buy', 'sell'],
                                    'fill_quantity': [25, 25],
                                    'fill_price': [46.6, 65.25],
                                    'closed': [True, True],
                                    'booked': [True, True]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # pnl check
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'start_position') == 0
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'current_position') == 0
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'gross_pnl') == approx(25 * (65.25 - 46.6))

    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'start_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'current_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == approx(50 * (52.97 - 67.98))

    # the market_close should cancel outstanding orders
    event_loop.market_close(['stock'])

    assert len(oms.open_orders_df()) == 0
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # Bar 3 EOD
    orders_snapshot = oms.orders_df()  # capture the orders_df before running EOD that clears the DataFrame
    event_loop.end_of_day(['stock'])

    # orders should be persisted and cleared out
    assert oms.orders_list() == []
    actual = tapdb.get_orders_df(temp_tapdb, oms.id, mdm.bartime)
    assert_frame_equal(actual[order_cols], orders_snapshot[order_cols])

    # confirm the PnL is done for the position from last night
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == approx(50 * (52.97 - 67.98))

    # orders should be cleared out
    assert oms.orders_list() == []

    # positions_df persisted
    saved = tapdb.get_positions_df(temp_tapdb, tap.id, mdm.bartime)
    assert_frame_equal(saved, tap.positions_df)

    # metric calculated
    assert tap.eod_metrics['gross_pnl'][0] == approx(25 * (65.25 - 46.6) + 50 * (52.97 - 67.98))
    assert tap.eod_metrics['equity'][0] == approx(924.0 + 25 * (65.25 - 46.6) + 50 * (52.97 - 67.98))
    assert tap.eod_metrics['equity'][-1] == approx(924.0)

    # open of bar 4
    mdm.bartime = pd.Timestamp('2010-01-06 09:30:00', tz=default_time_zone)
    event_loop.begin_of_day()

    # confirm the positions and pnl
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'start_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'start_position') is None
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == 0.0

    # open the market and process the bar
    event_loop.market_open(['stock'])
    event_loop.process_bar(['stock'], '1D')

    assert len(oms.open_orders_df()) == 0

    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'],
                                    'state': ['CANCELED'],
                                    'buy_sell': ['buy'],
                                    'quantity': [10],
                                    'details': [{'price': 70.5}],
                                    'fill_quantity': [None],
                                    'fill_price': [None],
                                    'closed': [False],
                                    'booked': [None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # bar 4 process
    mdm.bartime = pd.Timestamp('2010-01-06 16:00:00', tz=default_time_zone)
    event_loop.process_bar(['stock'], '1D')

    # no new orders and the old canceled is still there
    assert len(oms.open_orders_df()) == 0
    expected_closed = rc.DataFrame({'symbol': ['test.sym.9'],
                                    'state': ['CANCELED'],
                                    'buy_sell': ['buy'],
                                    'quantity': [10],
                                    'details': [{'price': 70.5}],
                                    'fill_quantity': [None],
                                    'fill_price': [None],
                                    'closed': [True],
                                    'booked': [None]})
    actual_closed = oms.closed_orders_df()[expected_closed.columns]
    assert_frame_equal(actual_closed, expected_closed)

    # pnl check
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'start_position') is None
    assert tap.get_value('test.example', 'stock', 'test.sym.10', 'current_position') is None

    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'start_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'current_position') == 50
    assert tap.get_value('test.example', 'stock', 'test.sym.9', 'gross_pnl') == approx(50 * (44.49 - 52.97))

    # attempt to make an order on the market_close, RuntimeError because of attempt to make new order
    with pytest.raises(RuntimeError):
        event_loop.market_close(['stock'])
