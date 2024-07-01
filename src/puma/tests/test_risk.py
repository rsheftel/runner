"""
unit tests for the Risk class
"""

import os
from collections import namedtuple

import pytest

import data as datalib
import puma as tw

from puma import OrderManager, risk
from puma.utils import assert_orders_equal

# Global variables



def test_initialize():
    oms = OrderManager('unit_test', None)
    rk = risk.Risk(oms)
    assert isinstance(rk, risk.Risk)


def test_process_order():
    oms = OrderManager('unit_test', None)
    rk = risk.Risk(oms)
    order = tw.Order('1001', 'orig_01', '123-456', 'strat', 'stock', 'TEST', 'Sell', 62, 'LIMIT', price=10)
    oms.new_order(order)

    assert order.state == 'CREATED'

    oms.market_state('stock', True)
    rk.process_order(order)
    actual = oms.orders_list()[0]
    assert_orders_equal(actual, order)
    assert actual.state == 'RISK_ACCEPTED'


def test_market_closed():
    oms = OrderManager('unit_test', None)
    rk = risk.Risk(oms)

    # make an order confirm it is created
    order = tw.Order('1001', 'orig_01', '123-456', 'strat', 'stock', 'TEST', 'Sell', 62, 'LIMIT', price=10)
    oms.new_order(order)
    assert order.state == 'CREATED'

    # process it prior to the market opening, risk fails
    with pytest.raises(KeyError):
        rk.process_order(order)

    # now create an order and process after the market is opened
    order = tw.Order('1001', 'orig_01', '123-456', 'strat', 'stock', 'TEST', 'Buy', 50, 'LIMIT', price=50)
    oms.new_order(order)

    oms.market_state('stock', True)
    rk.process_order(order)
    actual = oms.orders_list()[-1]
    assert_orders_equal(actual, order)
    assert actual.state == 'RISK_ACCEPTED'

    # now create an order and process after the market is closed
    order = tw.Order('1001', 'orig_01', '123-456', 'strat', 'stock', 'TEST', 'Sell', 25, 'LIMIT', price=100)
    oms.new_order(order)

    oms.market_state('stock', False)
    rk.process_order(order)
    actual = oms.orders_list()[-1]
    assert_orders_equal(actual, order)
    assert actual.state == 'RISK_REJECTED'


def test_process_port_orders():
    oms = OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    csvdf = datalib.CsvDataFeed(os.path.normpath("./puma/data/tests/inst/csv_data_feed"))
    hdm = datalib.HistoricalDataManager(csvdf, host="temp")
    ldm = datalib.LiveDataManager(csvdf, host="temp")
    mdm = datalib.MarketDataManager(hdm, ldm)
    objs = namedtuple('OB', 'portfolio, order_manager, market_data_manager')(port, oms, mdm)
    strat = tw.strategy.ExampleStrategy('TEST1', objs)
    port.add_strategy(strat)
    rk = risk.Risk(oms)
    strat.add_symbols([('stock', 'test.sym.1', '1D'), ('stock', 'test.sym.2', '1D')])

    strat.order('stock', 'test.sym.1', 'B', 1000, 'LIMIT', 100.5)
    strat.order('stock', 'test.sym.2', 'S', 55, 'LIMIT', 5.5)

    oms.market_state('stock', True)
    port.process_orders()
    rk.process_portfolio_orders(port)

    aoq = oms.orders_list({'state': 'RISK_ACCEPTED'})
    assert len(aoq) == 1
    actual_order = aoq[0]
    test_order = tw.Order('123-456', 'strategy.TEST1', '123-456', 'TEST1', 'stock', 'test.sym.2', 'S', 55, 'LIMIT',
                          price=5.5)
    assert_orders_equal(actual_order, test_order, check_state=False, check_state_df=False, check_id=False)
    assert actual_order.state == 'RISK_ACCEPTED'
    assert actual_order.state_df['state'].to_list() == ['CREATED', 'STAGED', 'RISK_ACCEPTED']

    roq = oms.orders_list({'state': 'RISK_REJECTED'})
    assert len(roq) == 1
    actual_order = roq[0]
    test_order = tw.Order('123-456', 'strategy.TEST1', '123-456', 'TEST1', 'stock', 'test.sym.1', 'B', 1000, 'LIMIT',
                          price=100.5)
    test_order.closed = True
    assert_orders_equal(actual_order, test_order, check_state=False, check_state_df=False, check_id=False)
    assert actual_order.state == 'RISK_REJECTED'
    assert actual_order.state_df['state'].to_list() == ['CREATED', 'STAGED', 'RISK_REJECTED']

    # test that risk will only work on STAGED orders
    oms.change_state(aoq[0], 'LIVE')
    rk.process_portfolio_orders(port)
    assert aoq[0].state == 'LIVE'


def test_process_replace():
    oms = OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.Portfolio('port_test', oms, pm)
    csvdf = datalib.CsvDataFeed(os.path.normpath("./puma/data/tests/inst/csv_data_feed"))
    hdm = datalib.HistoricalDataManager(csvdf, host="temp")
    ldm = datalib.LiveDataManager(csvdf, host="temp")
    mdm = datalib.MarketDataManager(hdm, ldm)
    objs = namedtuple('OB', 'portfolio, order_manager, market_data_manager')(port, oms, mdm)
    strat = tw.strategy.ExampleStrategy('TEST1', objs)
    port.add_strategy(strat)
    rk = risk.Risk(oms)
    strat.add_symbols([('stock', 'test.sym.1', '1D'), ('stock', 'test.sym.2', '1D')])

    strat.order('stock', 'test.sym.1', 'B', 50, 'LIMIT', 100.5)

    oms.market_state('stock', True)
    port.process_orders()

    # process orders and confirm moved to RISK_ACCEPTED state
    rk.process_portfolio_orders(port)

    aoq = oms.orders_list({'state': 'RISK_ACCEPTED'})
    assert len(aoq) == 1
    actual_order = aoq[0]
    test_order = tw.Order('123-456', 'strategy.TEST1', '123-456', 'TEST1', 'stock', 'test.sym.1', 'B', 50, 'LIMIT',
                          price=100.5)
    assert_orders_equal(actual_order, test_order, check_state=False, check_state_df=False, check_id=False)
    assert actual_order.state == 'RISK_ACCEPTED'
    assert actual_order.state_df['state'].to_list() == ['CREATED', 'STAGED', 'RISK_ACCEPTED']

    # change the order state to LIVE
    oms.change_state(actual_order, 'LIVE')

    # create a replacement and confirm that it stays in REPLACE_REQUESTED state is accepted
    oms.replace_order(actual_order, 75, price=90.9)
    rk.process_portfolio_orders(port)
    test_order = tw.Order('123-456', 'strategy.TEST1', '123-456', 'TEST1', 'stock', 'test.sym.1', 'B', 75, 'LIMIT',
                          price=90.9)
    assert_orders_equal(actual_order, test_order, check_state=False, check_state_df=False, check_id=False)
    assert actual_order.state == 'REPLACE_REQUESTED'
    assert actual_order.state_df['state'].to_list() == ['CREATED', 'STAGED', 'RISK_ACCEPTED', 'LIVE',
                                                        'REPLACE_REQUESTED']

    # now create a replacement that is RISK_REJECTED
    oms.replace_order(actual_order, 999, price=50.5)
    rk.process_portfolio_orders(port)
    test_order = tw.Order('123-456', 'strategy.TEST1', '123-456', 'TEST1', 'stock', 'test.sym.1', 'B', 75, 'LIMIT',
                          price=90.9)
    assert_orders_equal(actual_order, test_order, check_state=False, check_state_df=False, check_id=False)
    assert actual_order.state == 'REPLACE_REQUESTED'
    assert actual_order.state_df['state'].to_list() == ['CREATED', 'STAGED', 'RISK_ACCEPTED', 'LIVE',
                                                        'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_REQUESTED']
