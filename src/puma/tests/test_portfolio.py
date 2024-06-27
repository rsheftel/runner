"""
unit tests for Portfolio class and supporting module items
"""

import os
from collections import namedtuple

import data as datalib
from database import symboldb
import pandas as pd
import pytest
import raccoon as rc
from data.structures import Bar
from puma import Order, OrderManager, portfolio
from puma.position_manager import PositionManager
from puma.strategy import ExampleStrategy
from puma.utils import assert_orders_equal
from raccoon.utils import assert_frame_equal

# Global variables
seng = None


def setup_module():
    global seng, db_credentials
    seng = symboldb.symbol_engine('stock', **test_login)
    db_credentials = credentials('test', 'localhost', prefix='db_')


def teardown_module():
    seng.dispose()


def setup_for_intents():
    # setup market data
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup objects
    oms = OrderManager('unit_test', None)
    pm = PositionManager('pm_test', oms, None)
    port = portfolio.Portfolio('port_test', oms, pm)
    port.setup_market_data(mdm)

    # setup strategies
    objs = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat1 = ExampleStrategy('strat-01', objs)
    strat1.add_symbol('stock', 'test.sym.10', '1min')
    strat1.add_symbol('stock', 'test.sym.11', '1min')
    strat2 = ExampleStrategy('strat-02', objs)
    strat2.add_symbol('stock', 'test.sym.10', '1min')
    port.add_strategy(strat1)
    port.add_strategy(strat2)
    return mdm, oms, port, pm, strat1, strat2


def test_initialize():
    oms = OrderManager('unit_test', None)
    pm = PositionManager('pm_test', oms, None)
    port = portfolio.Portfolio('port_test', oms, pm)
    assert isinstance(port.uuid, str)
    assert isinstance(port.position_manager, PositionManager)

    port.setup_market_data(datalib.MarketDataManager(None, None))
    assert isinstance(port.market_data_manager, datalib.MarketDataManager)


def test_add_strategies():
    oms = OrderManager('unit_test', None)
    pm = PositionManager('pm_test', oms, None)
    port = portfolio.Portfolio('port_test', oms, pm)
    objs = namedtuple('OB', 'portfolio, position_manager')(port, pm)
    strat = ExampleStrategy('TEST1', objs)
    port.add_strategy(strat)
    assert port.strategy_ids == ['TEST1']

    strat2 = ExampleStrategy('TEST2', objs)
    port.add_strategy(strat2)
    assert set(port.strategy_ids) == {'TEST1', 'TEST2'}


def test_process_orders():
    oms = OrderManager('unit_test', None)
    pm = PositionManager('pm_test', oms, None)
    port = portfolio.Portfolio('port_test', oms, pm)
    csvdf = datalib.CsvDataFeed(os.path.normpath("./puma/data/tests/inst/csv_data_feed"))
    hdm = datalib.HistoricalDataManager(csvdf, **db_credentials)
    ldm = datalib.LiveDataManager(csvdf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)
    objs = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = ExampleStrategy('TEST1', objs)

    # register strategies
    port.add_strategy(strat)
    strat.add_symbols([('stock', 'test.sym.1', '1D'), ('stock', 'test.sym.2', '1D')])

    strat.order('stock', 'test.sym.1', 'B', 100, 'LIMIT', 100.5)
    strat.order('stock', 'test.sym.2', 'S', 55, 'LIMIT', 5.5)

    # confirm order has not been attached to a portfolio
    soq = oms.orders_df()
    assert soq.get_entire_column('portfolio_uuid', as_list=True) == [None, None]
    assert soq.get_entire_column('portfolio_id', as_list=True) == [None, None]

    port.process_orders()
    soq = oms.orders_df()
    assert len(soq) == 2
    assert soq.get_entire_column('state', as_list=True) == ['STAGED', 'STAGED']
    assert soq.get_entire_column('portfolio_uuid', as_list=True) == [port.uuid, port.uuid]
    assert soq.get_entire_column('portfolio_id', as_list=True) == [port.id, port.id]

    order = oms.orders_list({'symbol': 'test.sym.1'})[0]
    assert order.state == 'STAGED'
    assert order.symbol == 'test.sym.1'
    assert order.buy_sell == 'buy'
    assert order.quantity == 100
    assert order.type == 'LIMIT'
    assert order.details == {'price': 100.5}
    assert order.portfolio_id == 'port_test'
    assert order.portfolio_uuid == port.uuid


def test_intent_set_get():
    oms = OrderManager('unit_test', None)
    pm = PositionManager('pm_test', oms, None)
    port = portfolio.Portfolio('port_test', oms, pm)

    # add intents
    port.set_intent('strat-01', 'stock', 'test.sym.2', 100)
    port.set_intent('strat-01', 'stock', 'test.sym.1', 50)
    port.set_intent('strat-02', 'stock', 'test.sym.1', -75)

    expected = rc.DataFrame({'target': [50, 100, -75], 'order': [None, None, None]}, columns=['target', 'order'],
                            index=[('strat-01', 'stock', 'test.sym.1'), ('strat-01', 'stock', 'test.sym.2'),
                                   ('strat-02', 'stock', 'test.sym.1')], sort=True)

    assert_frame_equal(port.intents, expected)

    # a new set_intent overrides old
    port.set_intent('strat-02', 'stock', 'test.sym.1', -50)
    expected = rc.DataFrame({'target': [50, 100, -50], 'order': [None, None, None]}, columns=['target', 'order'],
                            index=[('strat-01', 'stock', 'test.sym.1'), ('strat-01', 'stock', 'test.sym.2'),
                                   ('strat-02', 'stock', 'test.sym.1')], sort=True)
    assert_frame_equal(port.intents, expected)

    # get the intent
    actual = port.get_intent('strat-02', 'stock', 'test.sym.1')
    assert actual == {'target': -50, 'order': None, 'index': ('strat-02', 'stock', 'test.sym.1')}

    # get on invalid fails
    with pytest.raises(ValueError):
        port.get_intent('strat-02', 'stock', 'BAD')


def test_new_intents():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add some intents
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)
    port.set_intent('strat-01', 'stock', 'test.sym.10', 50)
    port.set_intent('strat-02', 'stock', 'test.sym.10', -75)

    # update the market data
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')

    # process intents
    port.process_intents()

    # confirm that all targets reset to none
    assert all(x is None for x in port.intents.get(columns='target', as_list=True))

    # check values of the orders
    expected1 = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.10', 'buy',
                      50, 'LIMIT', price=44.0)
    actual1 = port.intents.get_cell(('strat-01', 'stock', 'test.sym.10'), 'order')
    assert_orders_equal(actual1, expected1, check_id=False)
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.10'})
    assert_orders_equal(actual1, orders[0])

    expected2 = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.11', 'buy',
                      100, 'LIMIT', price=100.25)
    actual2 = port.intents.get_cell(('strat-01', 'stock', 'test.sym.11'), 'order')
    assert_orders_equal(actual2, expected2, check_id=False)
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.11'})
    assert_orders_equal(actual2, orders[0])

    expected3 = Order(port.uuid, 'portfolio.' + port.id, strat2.uuid, strat2.strategy_id, 'stock', 'test.sym.10',
                      'sell',
                      75, 'LIMIT', price=44.0)
    actual3 = port.intents.get_cell(('strat-02', 'stock', 'test.sym.10'), 'order')
    assert_orders_equal(actual3, expected3, check_id=False)
    orders = oms.orders_list({'strategy_id': 'strat-02', 'symbol': 'test.sym.10'})
    assert_orders_equal(actual3, orders[0])


def test_intents_with_no_data():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add some intents
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)
    port.set_intent('strat-01', 'stock', 'test.sym.10', 50)
    port.set_intent('strat-02', 'stock', 'test.sym.10', -75)

    # update the market data to a valid date, then an empty None date
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    mdm.bartime = '2010-01-04 17:00:00'
    mdm.update('stock', '1min')

    # confirm current bar is None
    expected = Bar(pd.Timestamp('2010-01-04 17:00:00', tz='America/New_York'), None, None, None, None, None)
    assert mdm.current_bar('stock', 'test.sym.10', '1min') == expected
    assert mdm.current_bar('stock', 'test.sym.11', '1min') == expected

    # process intents
    port.process_intents()

    # check values of the orders use last valid price
    assert port.intents.get_cell(('strat-01', 'stock', 'test.sym.10'), 'order').details['price'] == 44.0
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.10'})
    assert orders[0].details['price'] == 44.0

    assert port.intents.get_cell(('strat-01', 'stock', 'test.sym.11'), 'order').details['price'] == 100.25
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.11'})
    assert orders[0].details['price'] == 100.25

    assert port.intents.get_cell(('strat-02', 'stock', 'test.sym.10'), 'order').details['price'] == 44.0
    orders = oms.orders_list({'strategy_id': 'strat-02', 'symbol': 'test.sym.10'})
    assert orders[0].details['price'] == 44.0

    # modify intents
    port.set_intent('strat-01', 'stock', 'test.sym.11', 10)
    port.set_intent('strat-01', 'stock', 'test.sym.10', 5)
    port.set_intent('strat-02', 'stock', 'test.sym.10', -7)

    port.process_intents()

    # confirm the modifications
    assert port.intents.get_cell(('strat-01', 'stock', 'test.sym.10'), 'order').details['price'] == 44.0
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.10'})
    assert orders[0].details['price'] == 44.0
    assert orders[0].quantity == 5

    assert port.intents.get_cell(('strat-01', 'stock', 'test.sym.11'), 'order').details['price'] == 100.25
    orders = oms.orders_list({'strategy_id': 'strat-01', 'symbol': 'test.sym.11'})
    assert orders[0].details['price'] == 100.25
    assert orders[0].quantity == 10

    assert port.intents.get_cell(('strat-02', 'stock', 'test.sym.10'), 'order').details['price'] == 44.0
    orders = oms.orders_list({'strategy_id': 'strat-02', 'symbol': 'test.sym.10'})
    assert orders[0].quantity == 7


def test_unch_intent():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add some intents
    port.set_intent('strat-01', 'stock', 'test.sym.10', 50)

    # update the market data
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')

    # process intents
    port.process_intents()

    # with outstanding order, set new target = old target, confirm that order quantity is same, price updated
    port.set_intent('strat-01', 'stock', 'test.sym.10', 50)
    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()

    expected = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.10', 'buy',
                     50, 'LIMIT', price=43.93)
    expected.state = 'REPLACE_REQUESTED'
    actual = port.intents[('strat-01', 'stock', 'test.sym.10'), 'order']
    assert_orders_equal(actual, expected, check_id=False)


def test_chg_intent_qty():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add intents and process
    port.set_intent('strat-01', 'stock', 'test.sym.11', 50)
    port.set_intent('strat-01', 'stock', 'test.sym.10', -100)
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_intents()

    # with outstanding order, modify up the quantity
    port.set_intent('strat-01', 'stock', 'test.sym.11', 65)
    # with outstanding order, modify down the quantity
    port.set_intent('strat-01', 'stock', 'test.sym.10', -85)

    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()

    # test modify up
    expected = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.11', 'buy',
                     65, 'LIMIT', price=99.33)
    expected.state = 'REPLACE_REQUESTED'
    actual = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']
    assert_orders_equal(actual, expected, check_id=False)

    # test modify down
    expected = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.10', 'sell',
                     85, 'LIMIT', price=43.93)
    expected.state = 'REPLACE_REQUESTED'
    actual = port.intents[('strat-01', 'stock', 'test.sym.10'), 'order']
    assert_orders_equal(actual, expected, check_id=False)


def test_no_intent():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add intents
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)
    port.set_intent('strat-01', 'stock', 'test.sym.10', None)

    # process
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']

    # since there was no intent, and no prior order
    assert port.intents[('strat-01', 'stock', 'test.sym.10'), 'order'] is None

    # Now there is no intent, which will cancel order
    port.set_intent('strat-01', 'stock', 'test.sym.11', None)
    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()

    assert actual.state == 'CANCEL_REQUESTED'
    assert port.intents[('strat-01', 'stock', 'test.sym.11'), 'order'] is None

    # An intent of 0.0 is not the same as no intent and will create an order
    pm.enter_trade(port.id, strat1.strategy_id, mdm.bartime, 'stock', 'test.sym.11', 'buy', 100, 100.25)
    port.set_intent('strat-01', 'stock', 'test.sym.11', 0.0)
    mdm.bartime = '2010-01-04 09:33:00'
    mdm.update('stock', '1min')
    port.process_intents()

    actual = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']
    assert actual.buy_sell == 'sell'
    assert actual.quantity == 100.0


def test_target_eq_actual_intent():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add intents
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)

    # process
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']

    # With outstanding order, set target = actual, which will cancel order
    pm.enter_trade(port.id, strat1.strategy_id, mdm.bartime, 'stock', 'test.sym.11', 'buy', 100, 100.25)

    # process
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)
    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()

    assert actual.state == 'CANCEL_REQUESTED'
    assert port.intents[('strat-01', 'stock', 'test.sym.11'), 'order'] is None

    # confirm the current position size
    current_position = pm.get_value('strat-01', 'stock', 'test.sym.11', 'current_position')
    assert current_position == 100.0

    # now set the intent again to the current position and no order should be created
    port.set_intent('strat-01', 'stock', 'test.sym.11', current_position)
    mdm.bartime = '2010-01-04 09:33:00'
    mdm.update('stock', '1min')
    port.process_intents()

    assert port.intents[('strat-01', 'stock', 'test.sym.11'), 'order'] is None


def test_intent_flip_direction():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add intents and process
    port.set_intent('strat-01', 'stock', 'test.sym.11', 100)
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual1 = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']

    # new intent, flip sign on the intent and process
    port.set_intent('strat-01', 'stock', 'test.sym.11', -100)
    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual2 = port.intents[('strat-01', 'stock', 'test.sym.11'), 'order']

    # assert that the first order is cancel requested
    assert actual1.state == 'CANCEL_REQUESTED'

    # new order created
    expected = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.11', 'sell',
                     100, 'LIMIT', price=99.33)
    assert_orders_equal(actual2, expected, check_id=False)


def test_already_closed_intent():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add intents and process
    port.set_intent('strat-02', 'stock', 'test.sym.10', 100)
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual1 = port.intents[('strat-02', 'stock', 'test.sym.10'), 'order']

    # Put outstanding order into closed state
    actual1.state = 'FILLED'
    actual1.closed = True

    # now attempt to modify, should create new order
    port.set_intent('strat-02', 'stock', 'test.sym.10', 150)
    mdm.bartime = '2010-01-04 09:32:00'
    mdm.update('stock', '1min')
    port.process_intents()
    actual2 = port.intents[('strat-02', 'stock', 'test.sym.10'), 'order']

    assert id(actual1) != id(actual2)
    # new order created
    expected = Order(port.uuid, 'portfolio.' + port.id, strat2.uuid, strat2.strategy_id, 'stock', 'test.sym.10', 'buy',
                     150, 'LIMIT', price=43.93)
    assert_orders_equal(actual2, expected, check_id=False, check_replaces=True)

    # now put this order into closed state, and then try to cancel, should skip
    actual2.state = 'FILLED'
    actual2.closed = True
    port.set_intent('strat-02', 'stock', 'test.sym.10', None)
    mdm.bartime = '2010-01-04 09:33:00'
    mdm.update('stock', '1min')
    port.process_intents()
    assert port.intents[('strat-02', 'stock', 'test.sym.10'), 'order'] is None


def test_bad_intents():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()
    # test intent to product type not in strategy
    port.set_intent('strat-02', 'BAD', 'test.sym.10', 100)
    with pytest.raises(RuntimeError):
        port.process_intents()

    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()
    # test intent to symbol not in strategy
    port.set_intent('strat-02', 'stock', 'BAD_SYM', 100)
    with pytest.raises(RuntimeError):
        port.process_intents()


def test_process_orders_and_intents():
    mdm, oms, port, pm, strat1, strat2 = setup_for_intents()

    # add orders and intents
    strat1.order('stock', 'test.sym.10', 'B', 100, 'LIMIT', 100.5)
    strat1.order('stock', 'test.sym.11', 'S', 55, 'LIMIT', 5.5)
    strat1.intent('stock', 'test.sym.10', 25)

    # process orders
    mdm.bartime = '2010-01-04 09:31:00'
    mdm.update('stock', '1min')
    port.process_orders()

    # confirm orders in open orders
    soq = oms.orders_df()
    assert len(soq) == 3
    assert soq.get_entire_column('state', as_list=True) == ['STAGED', 'STAGED', 'STAGED']
    assert soq.get_entire_column('portfolio_uuid', as_list=True) == [port.uuid, port.uuid, port.uuid]
    assert soq.get_entire_column('portfolio_id', as_list=True) == [port.id, port.id, port.id]
    assert soq.get_entire_column('originator_uuid', as_list=True) == [strat1.uuid, strat1.uuid, port.uuid]
    assert soq.get_entire_column('originator_id', as_list=True) == \
           ['strategy.' + strat1.strategy_id, 'strategy.' + strat1.strategy_id, 'portfolio.' + port.id]

    expected = Order(port.uuid, 'portfolio.' + port.id, strat1.uuid, strat1.strategy_id, 'stock', 'test.sym.10', 'buy',
                     25, 'LIMIT', price=44.0)
    expected.state = 'STAGED'
    actual = oms.orders_list({'symbol': 'test.sym.10'})[1]
    assert_orders_equal(actual, expected, check_id=False, check_replaces=True)
