"""
unit test for Strategy class and related modules
"""

import os
from collections import namedtuple

import pandas as pd
import pytest

import puma.data as datalib
import puma as tw
from config.database import credentials
from config.datetime import NYC
from puma import strategy
from puma.utils import assert_orders_equal

# Global variables
inst_dir = None
db_credentials = {}


def setup_module():
    global inst_dir, db_credentials
    inst_dir = os.path.normpath("./puma/data/tests/inst/")  # the directory of the csv files in test dir
    db_credentials = credentials('test', 'localhost', prefix='db_')


def setup_strategy():
    csvdf = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    hdm = datalib.HistoricalDataManager(csvdf, **db_credentials)
    ldm = datalib.LiveDataManager(csvdf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)
    oms = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port-01', oms, pm)
    ob = namedtuple('OB', 'order_manager, position_manager, market_data_manager')(oms, pm, mdm)
    strat = strategy.ExampleStrategy('strat_id', ob)
    return strat, oms, port, pm, mdm


def test_initialize():
    ob = namedtuple('OB', 'order_manager')(tw.OrderManager('unit_test', None))
    strat = strategy.ExampleStrategy('test_id', ob)

    assert strat.strategy_id == 'test_id'
    assert isinstance(strat.order_manager, tw.OrderManager)
    assert strat.barcount is None


def test_start():
    strat, oms, port, pm, mdm = setup_strategy()

    # test that if strategy not attached to a Portfolio will raise error
    with pytest.raises(RuntimeError):
        strat.start()

    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    port.add_strategy(strat)
    strat.start()
    assert strat._started is True
    assert strat.barcount == 0

    # strategy not attached to order manager will fail to start
    ob = namedtuple('OB', 'market_data_manager')(mdm)
    strat = strategy.ExampleStrategy('test_id', ob)
    with pytest.raises(RuntimeError):
        strat.start()


def test_stop():
    strat, oms, port, pm, mdm = setup_strategy()

    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    port.add_strategy(strat)
    strat.start()
    assert strat._started is True
    assert strat.start_stop == 1

    strat.on_stop(pd.Timestamp('2010-03-06 09:45:00', tz=NYC))
    assert strat.start_stop == 0
    assert strat.stopped == pd.Timestamp('2010-03-06 09:45:00', tz=NYC)


def test_immutable_attributes():
    ob = namedtuple('OB', 'order_manager')(tw.OrderManager('unit_test', None))
    strat = strategy.ExampleStrategy('test_id', ob)

    with pytest.raises(AttributeError):
        # noinspection PyPropertyAccess
        strat.strategy_id = 'CANNOT'


def test_add_symbols():
    strat, oms, port, pm, mdm = setup_strategy()

    port.add_strategy(strat)
    assert port == strat.portfolio

    strat.add_symbol('stock', 'MSFT', '1min')
    assert strat.symbol_tuples == [('stock', 'MSFT', '1min')]

    strat.add_symbols([('stock', 'AAPL', '1D'), ('future', 'TY.1C', '1D')])
    assert strat.symbol_tuples == [('stock', 'MSFT', '1min'), ('stock', 'AAPL', '1D'), ('future', 'TY.1C', '1D')]
    assert strat.product_types == {'stock', 'future'}
    assert strat.frequencies == {'1min', '1D'}

    assert strat.symbols == {'stock': {'AAPL', 'MSFT'}, 'future': {'TY.1C'}}

    # attempt to add a symbol not in SymbolDB
    with pytest.raises(AttributeError):
        strat.add_symbol('stock', 'NotInDB', '1D')

    # test that once the start() is called cannot add symbols
    strat.start()
    with pytest.raises(RuntimeError):
        strat.add_symbol('stock', 'YHOO', '1min')


def test_parameters():
    strat, oms, port, pm, mdm = setup_strategy()
    port.add_strategy(strat)

    # raise error if parameters is not a dictionary
    with pytest.raises(AttributeError):
        strat.set_parameters([1, 2, 3])

    params = {'long': 9, 'short': 9.9, 'liz': [1, 2, 3]}
    # error on trying to set parameters with setter
    with pytest.raises(AttributeError):
        # noinspection PyPropertyAccess
        strat.parameters = params

    # The right way to do it
    strat.set_parameters(params)
    assert strat.parameters == params

    # raise error on trying to set parameters after start
    ob = namedtuple('OB', 'market_data_manager, order_manager')(mdm, oms)
    strat = strategy.ExampleStrategy('test_id', ob)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    port.add_strategy(strat)
    strat.start()
    with pytest.raises(RuntimeError):
        strat.set_parameters(params)


def test_create_order():
    strat, oms, port, pm, mdm = setup_strategy()

    # raise error if product_type not in symbols added
    with pytest.raises(RuntimeError):
        strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)

    # add another symbol, raise error because test symbol not added
    strat.add_symbols([('stock', 'AAPL', '1min')])
    with pytest.raises(RuntimeError):
        strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)

    strat.add_symbols([('stock', 'test.sym.1', '1min'), ('stock', 'test.sym.2', '1min')])
    strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)

    actual = oms.orders_list()[0]
    assert actual.product_type == 'stock'
    assert actual.symbol == 'test.sym.1'
    assert actual.buy_sell == 'buy'
    assert actual.quantity == 100
    assert actual.state == 'CREATED'
    assert actual.type == 'LIMIT'
    assert actual.details == {'price': 99.9}

    strat.order('stock', 'test.sym.2', 'S', 55, 'LIMIT', price=10.5)
    assert len(oms.orders_list()) == 2


def test_get_order():
    strat, oms, port, pm, mdm = setup_strategy()

    strat.add_symbols([('stock', 'test.sym.1', '1min')])
    order_uuid = strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)

    actual = strat.get_order(order_uuid)
    assert actual == oms.orders_list()[0]

    assert strat.orders_list() == strat.orders_list({'state': 'CREATED'})


def test_cancel_order():
    strat, oms, port, pm, mdm = setup_strategy()
    strat.add_symbols([('stock', 'test.sym.1', '1min')])
    oid = strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)
    order = oms.order(oid)
    strat.cancel_order(order)

    assert order.state == 'CANCEL_REQUESTED'
    assert order.product_type == 'stock'
    assert order.symbol == 'test.sym.1'
    assert order.buy_sell == 'buy'
    assert order.quantity == 100
    assert order.type == 'LIMIT'
    assert order.details == {'price': 99.9}


def test_replace_order():
    strat, oms, port, pm, mdm = setup_strategy()
    strat.add_symbols([('stock', 'test.sym.1', '1min')])
    oid = strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)
    order = oms.order(oid)

    strat.replace_order(order, 55, price=55.55)
    assert order.state == 'REPLACE_REQUESTED'
    assert order.product_type == 'stock'
    assert order.symbol == 'test.sym.1'
    assert order.buy_sell == 'buy'
    assert order.quantity == 55
    assert order.type == 'LIMIT'
    assert order.details == {'price': 55.55}

    strat.replace_order(order, 66, price=55.55)
    assert order.state == 'REPLACE_REQUESTED'
    assert order.product_type == 'stock'
    assert order.symbol == 'test.sym.1'
    assert order.buy_sell == 'buy'
    assert order.quantity == 66
    assert order.type == 'LIMIT'
    assert order.details == {'price': 55.55}

    strat.replace_order(order, price=77.7)
    assert order.state == 'REPLACE_REQUESTED'
    assert order.product_type == 'stock'
    assert order.symbol == 'test.sym.1'
    assert order.buy_sell == 'buy'
    assert order.quantity == 66
    assert order.type == 'LIMIT'
    assert order.details == {'price': 77.7}


def test_already_closed():
    strat, oms, port, pm, mdm = setup_strategy()
    strat.add_symbols([('stock', 'test.sym.1', '1min')])

    # add an order and move to CANCELED
    oid = strat.order('stock', 'test.sym.1', 'buy', 100, 'LIMIT', price=99.9)
    order = oms.order(oid)
    oms.change_state(order, 'CANCELED')
    oms.close_order(order)

    # attempt to replace, note that state unchanged
    strat.replace_order(order, 50, price=99)
    assert order.state == 'CANCELED'
    assert order.closed is True

    # attempt to cancel, note that state unchanged
    strat.cancel_order(order)
    assert order.state == 'CANCELED'
    assert order.closed is True


def test_on_cancel():
    strat, oms, port, pm, mdm = setup_strategy()
    strat.add_symbols([('stock', 'test.sym.3', '1min')])

    ordr = tw.Order(1001, 'strategy.strat_id', '123', 'strat_id', 'stock', 'test.sym.3', 'sell', 1, 'LIMIT',
                    price=99.99)
    oms.change_state(ordr, 'CANCELED')

    strat.on_cancels(pd.Timestamp('2099-01-01 10:00:00', tz='EST'), [ordr])

    expected = tw.Order(1001, 'strategy.strat_id', '123', 'strat_id', 'stock', 'test.sym.3', 'buy', 99,
                        'LIMIT', price=99.99)
    actual = oms.orders_list({'state': 'CREATED'})[0]

    assert_orders_equal(actual, expected, check_id=False)


def test_on_fill():
    strat, oms, port, pm, mdm = setup_strategy()
    strat.add_symbols([('stock', 'MSFT', '1min')])

    ordr = tw.Order(1001, 'orig_id', '123', 'strat_id', 'stock', 'MSFT', 'sell', 50, 'LIMIT', price=44.50)
    ordr.add_fill(123, pd.Timestamp('2010-06-01 09:31:00', tz=NYC), pd.Timestamp('2010-06-01 09:31:00', tz=NYC),
                  50, 44.50, -0.5)

    strat.on_fills('2010-06-01 09:31:00', [ordr])

    expected = tw.Order(1001, 'strategy.strat_id', 1001, 'strat_id', 'stock', 'MSFT', 'buy', 50, 'LIMIT', price=43.50)
    actual = oms.orders_list({'state': 'CREATED'})[0]

    assert_orders_equal(actual, expected, check_id=False)


def test_set_get_intent():
    strat, oms, port, pm, mdm = setup_strategy()
    port.add_strategy(strat)
    strat.add_symbol('stock', 'test.sym.10', '1min')

    # new intent
    strat.intent('stock', 'test.sym.10', -75)
    assert strat.get_intent('stock', 'test.sym.10') == -75
    assert port.get_intent('strat_id', 'stock', 'test.sym.10')['target'] == -75

    # update intent
    strat.intent('stock', 'test.sym.10', 50)
    assert strat.get_intent('stock', 'test.sym.10') == 50
    assert port.get_intent('strat_id', 'stock', 'test.sym.10')['target'] == 50

    # try to get intent for invalid product_type or symbol
    assert strat.get_intent('stock', 'BAD') is None
    assert strat.get_intent('BAD', 'test.sym.10') is None

    # try to set intent for invalid product_type
    strat.intent('BAD', 'test.sym.10', 1)
    with pytest.raises(RuntimeError):
        port.process_intents()
    strat.intent('BAD', 'test.sym.10', None)

    # try to set intent for invalid symbol
    strat.intent('stock', 'BAD_SYMBOL', 1)
    with pytest.raises(RuntimeError):
        port.process_intents()


def test_get_position():
    strat, oms, port, pm, mdm = setup_strategy()
    port.add_strategy(strat)
    strat.add_symbol('stock', 'test.sym.10', '1min')

    # raw get returns none if there is no position
    assert strat.get('stock', 'test.sym.10', 'current_position') is None

    # when there is no position returns zero
    assert strat.position('stock', 'test.sym.10') == 0

    pm.enter_trade('123', 'strat_id', pd.Timestamp('2010-01-01', tz=NYC), 'stock', 'test.sym.10', 'sell', 50, 100.10)
    assert strat.position('stock', 'test.sym.10') == -50

    pm.enter_trade('123', 'strat_id', pd.Timestamp('2010-01-01', tz=NYC), 'stock', 'test.sym.10', 'buy', 75, 100.10)
    assert strat.position('stock', 'test.sym.10') == 25

    # raw get
    assert strat.get('stock', 'test.sym.10', 'current_position') == 25


def test_bod_eod():
    strat, oms, port, pm, mdm = setup_strategy()

    mdm.bartime = '2017-02-01 09:30:00'
    strat.on_begin_of_day(mdm.bartime)
    assert strat.new_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.days_done == []
    assert strat.open_days == []
    assert strat.closed_days == []

    strat.on_market_open(mdm.bartime)
    assert strat.new_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.days_done == []
    assert strat.open_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.closed_days == []

    mdm.bartime = '2017-02-01 16:00:00'
    strat.on_market_close(mdm.bartime)
    assert strat.new_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.days_done == []
    assert strat.open_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.closed_days == [pd.Timestamp('2017-02-01 16:00:00', tz=NYC)]

    strat.on_end_of_day(mdm.bartime)
    assert strat.new_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.days_done == [pd.Timestamp('2017-02-01 16:00:00', tz=NYC)]
    assert strat.open_days == [pd.Timestamp('2017-02-01 09:30:00', tz=NYC)]
    assert strat.closed_days == [pd.Timestamp('2017-02-01 16:00:00', tz=NYC)]


def test_empty_methods():
    oms = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    ob = namedtuple('OB', 'order_manager, position_manager')(oms, pm)
    strat = strategy.EmptyStrategy('empty', ob)

    strat.on_initialize()
    strat.on_start()
    strat.on_bar('2000-01-01')
    strat.on_fills('2000-01-01', [])
    strat.on_cancels('2000-01-01', [])
