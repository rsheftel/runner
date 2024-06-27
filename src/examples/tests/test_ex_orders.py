"""
General integration style tests that can be run in the unit test framework
"""

import os
from collections import namedtuple

import montauk.data as datalib
import montauk.tomahawk as tw
from config.database import credentials
from montauk.data.data_manager import LiveDataManager
from montauk.data.market_data_manager import MarketDataManager

# Global variables
test_login = None
inst_dir = None


def setup_module():
    global test_login, inst_dir
    test_login = credentials('test')
    inst_dir = os.path.normpath("./montauk/data/tests/inst/")  # the directory of the csv files in test dir


def test_order_out():
    # Turn on logging
    # futils.setup_logging()

    # setup the environment objects
    oms = tw.order_manager.OrderManager('unit_test', None)
    pm = tw.PositionManager('pm_test', oms, None)
    port = tw.portfolio.Portfolio('port_test', oms, pm)
    risk = tw.risk.Risk(oms)

    # Paper broker and exchange
    exchange = tw.exchange.PaperExchange()
    broker = tw.PaperBroker('broker_01', oms, exchange)

    # Setup market data
    datafeed = datalib.data_feed.CsvDataFeed(inst_dir + '/csv_data_feed')
    lmds = LiveDataManager(datafeed, test_login['username'], test_login['password'], 'localhost')
    mdm = MarketDataManager(None, lmds)

    # Setup object and attach to the Portfolio
    ob = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = tw.strategy.ExampleStrategy('test_01', ob)
    port.add_strategy(strat)

    # Add the symbols to the strategy that will be traded
    strat.add_symbols([('stock', 'test.sym.3', '1min'), ('stock', 'MSFT', '1min'), ('stock', 'AAPL', '1min')])

    # create some orders in the strategy
    strat.order('stock', 'test.sym.3', 'b', 1000, 'LIMIT', 99.99)
    strat.order('stock', 'MSFT', 's', 50, 'LIMIT', 44.50)
    strat.order('stock', 'AAPL', 'b', 75, 'LIMIT', 52.52)

    assert len(oms.open_orders_df()) == 3

    # put the market in open state
    oms.market_state('stock', True)

    # process the strategy orders to the OMS
    port.process_orders()
    risk.process_portfolio_orders(port)

    assert all(oms.open_orders_df().isin('symbol', ['MSFT', 'AAPL']))
    assert oms.closed_orders_df().get(0, 'symbol') == 'test.sym.3'

    # send the orders to market
    broker.send_orders()
    assert sum(oms.open_orders_df().isin('state', ['SENT'])) == 2

    # setup the market data manager
    mdm.add_symbols('stock', ['test.sym.3', 'AAPL', 'MSFT'], '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')

    # exchange processes the orders, this is for sim only
    exchange.process_orders(mdm)

    # OMS gets order status. In prod run this in a cycle until the next update
    broker.update_order_states()

    assert all(oms.closed_orders_df().isin('symbol', ['test.sym.3', 'AAPL']))
    assert all(oms.closed_orders_df().isin('state', ['RISK_REJECTED', 'FILLED']))

    assert oms.open_orders_df().get(0, 'state') == 'LIVE'
    assert oms.open_orders_df().get(0, 'symbol') == 'MSFT'


def test_order_in():
    # Turn on logging
    # futils.setup_logging()

    # setup all the objects
    oms = tw.order_manager.OrderManager('unit_test', None)
    tap = tw.position_manager.PositionManager('pm_test', oms, None)
    port = tw.portfolio.Portfolio('port_test', oms, tap)
    risk = tw.risk.Risk(oms)
    exchange = tw.exchange.PaperExchange()
    broker = tw.PaperBroker('broker_01', oms, exchange)
    datafeed = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    lmds = LiveDataManager(datafeed, test_login['username'], test_login['password'], 'localhost')
    mdm = MarketDataManager(None, lmds)

    ob = namedtuple('OB', 'order_manager, market_data_manager')(oms, mdm)
    strat = tw.strategy.ExampleStrategy('test_01', ob)
    port.add_strategy(strat)
    strat.add_symbols([('stock', 'test.sym.3', '1min'), ('stock', 'MSFT', '1min'), ('stock', 'AAPL', '1min')])

    strat.order('stock', 'test.sym.3', 'b', 1000, 'LIMIT', 99.99)
    strat.order('stock', 'MSFT', 's', 50, 'LIMIT', 44.50)
    strat.order('stock', 'AAPL', 'b', 25, 'LIMIT', 52.52)

    # put the market in open state
    oms.market_state('stock', True)

    # process the strategy orders to the OMS
    port.process_orders()
    risk.process_portfolio_orders(port)

    # send the orders to market
    broker.send_orders()

    # setup the market data manager
    mdm.add_symbols('stock', ['test.sym.3', 'AAPL', 'MSFT'], '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')

    # exchange processes the orders, this is for sim only
    exchange.process_orders(mdm)

    # OMS gets order status. In prod run this in a cycle until the next update
    broker.update_order_states()
    closed = oms.closed_orders_df()
    assert closed.get(1, 'state') == 'FILLED'
    assert closed.get(1, 'symbol') == 'AAPL'
    assert closed.get(1, 'fill_price') == 52.52
    assert closed.get(1, 'fill_quantity') == 25

    order_dict = tap.book_fills()
    assert order_dict['strategy.test_01'][0].state == 'FILLED'
    assert order_dict['strategy.test_01'][0].symbol == 'AAPL'
    assert order_dict['strategy.test_01'][0].fill_price == 52.52
    assert order_dict['strategy.test_01'][0].fill_quantity == 25

    # get the open orders for MSFT & cancel then
    msft_order = oms.orders_list({'symbol': 'MSFT', 'state': 'LIVE'})[0]
    oms.change_state(msft_order, 'CANCEL_REQUESTED')
    broker.send_cancel(msft_order)

    mdm.bartime = '2010-01-01 09:32:00'
    mdm.update('stock', '1min')
    exchange.process_orders(mdm)
    broker.update_order_states()

    # verify the order was canceled
    assert oms.closed_orders_df().get(1, 'state') == 'CANCELED'
