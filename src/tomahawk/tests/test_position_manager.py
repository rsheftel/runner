"""
unit tests for PositionManager class and associated functions
"""

import pandas as pd
import pytest
import raccoon as rc
from numpy.testing import assert_almost_equal as np_assert_almost_equal
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from pytest import approx
from raccoon.utils import assert_frame_equal, assert_series_equal

import montauk.data as datalib
import montauk.database.symbol as symboldb
import montauk.database.utils as dbutils
import montauk.metric as metric
import montauk.tomahawk as tw
from config.database import credentials
from config.datetime import NYC
from montauk.database import tapdb
from montauk.tomahawk import position_manager

# Global variables
db_credentials = {}
seng = None
prod_tapdb = None
temp_tapdb = None


def setup_module():
    global seng, db_credentials, prod_tapdb, temp_tapdb
    test_login = credentials('test', 'localhost')
    seng = symboldb.symbol_engine('stock', **test_login)
    db_credentials = credentials('test', 'localhost', prefix='db_')

    # setup temp tapdb
    prod_tapdb = tapdb.tapdb_engine(**test_login)
    temp_tapdb = dbutils.make_engine('temp_tapdb', **test_login)
    dbutils.copy_table_schema(prod_tapdb, temp_tapdb)


def teardown_module():
    prod_tapdb.dispose()
    temp_tapdb.dispose()


def test_initialize():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    assert isinstance(pm, position_manager.PositionManager)
    assert isinstance(pm.positions_df, rc.DataFrame)
    assert pm.new_trades_df is None
    assert isinstance(pm.uuid, str)
    assert pm.id == 'testpm'


def setup_objects():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('test_unit', oms, temp_tapdb)
    pm.setup_market_data(mdm, '1min')
    return mdm, pm, oms


def test_trade_id():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    # without doing anything the trade id is None
    assert pm.trade_id is None

    # Call new_trade_id with increment is True returns new trade id and updates internal counter
    assert pm.new_trade_id(increment=True) == 1
    assert pm.trade_id == 1

    # Call to new_trade_id with increment False gets a new trade id, but does NOT increment internal counter
    assert pm.new_trade_id(increment=False) == 2
    assert pm.trade_id == 1


def test_position_df():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    assert len(pm.positions_df) == 0

    pm.initialize_row({'originator_id': 'strategy.strat_1', 'strategy_id': 'strat_1', 'product_type': 'stock',
                       'symbol': 'TEST1'})
    assert pm.get_value('strat_1', 'stock', 'TEST1', 'current_position') == 0

    pm.set_value('strat_1', 'stock', 'TEST1', 'current_position', 99)
    assert pm.get_value('strat_1', 'stock', 'TEST1', 'current_position') == 99

    pm.initialize_row({'originator_id': 'orig_1', 'strategy_id': 'strat_2', 'product_type': 'stock', 'symbol': 'TEST1'})
    pm.set_value('strat_2', 'stock', 'TEST1', 'start_position', 55)
    assert pm.get_value('strat_2', 'stock', 'TEST1', 'start_position') == 55

    pm.initialize_row({'originator_id': 'orig_1', 'strategy_id': 'strat_2', 'product_type': 'stock', 'symbol': 'TEST2'})
    pm.set_value('strat_2', 'stock', 'TEST2', 'start_position', 5)
    assert pm.get_value('strat_2', 'stock', 'TEST2', 'start_position') == 5

    assert pm.positions_df.index == [('strat_1', 'stock', 'TEST1'), ('strat_2', 'stock', 'TEST1'),
                                     ('strat_2', 'stock', 'TEST2')]

    # wrong key returns None
    assert pm.get_value('BAD', 'stock', 'WORSE', 'current_position') is None

    # setting a wrong key raises error
    with pytest.raises(ValueError):
        pm.set_value('BAD', 'stock', 'WORSE', 'current_position', 99)


def test_insert_trade():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    trade = {'originator_id': 'test-id', 'bartime': pd.Timestamp('2016-01-01 09:35:00'), 'product_type': 'stock',
             'symbol': 'TEST', 'buy_sell': 'buy', 'quantity': 100, 'price': 50.0}

    pm._insert_trade(trade)
    actual = pm.new_trades[0]
    assert actual == trade

    actual_df = pm.new_trades_df
    assert set(actual_df.columns) == {'originator_id', 'bartime', 'timestamp', 'id', 'product_type', 'symbol',
                                      'buy_sell', 'quantity', 'price'}
    assert len(actual_df) == 1


def test_update_position():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    # first trade into an empty position manager
    trade = {'strategy_id': 'test-id', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:35:00'),
             'product_type': 'stock', 'symbol': 'TEST', 'buy_sell': 'buy', 'quantity': 100, 'price': 50.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'start_position') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_quantity') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_quantity') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'net_quantity') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_avg_price') == 50

    # trade same direction same strategy and symbol
    trade = {'strategy_id': 'test-id', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:45:00'),
             'product_type': 'stock', 'symbol': 'TEST', 'buy_sell': 'buy', 'quantity': 55, 'price': 42.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 155
    assert pm.get_value('test-id', 'stock', 'TEST', 'start_position') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_quantity') == 155
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_quantity') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'net_quantity') == 155
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_avg_price') == (100 * 50 + 55 * 42) / (100 + 55)

    # trade opposite direction same strategy and symbol
    trade = {'strategy_id': 'test-id', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:45:00'),
             'product_type': 'stock', 'symbol': 'TEST', 'buy_sell': 'sell', 'quantity': 75, 'price': 22.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 80
    assert pm.get_value('test-id', 'stock', 'TEST', 'start_position') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_quantity') == 155
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_quantity') == 75
    assert pm.get_value('test-id', 'stock', 'TEST', 'net_quantity') == 80
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_avg_price') == (100 * 50 + 55 * 42) / (100 + 55)
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_avg_price') == 22

    # new strategy and symbol trade
    trade = {'strategy_id': 'test-id-2', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:45:00'),
             'product_type': 'stock', 'symbol': 'TEST2', 'buy_sell': 'sell', 'quantity': 10, 'price': 15.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'current_position') == -10
    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'start_position') == 0
    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'buy_quantity') == 0
    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'sell_quantity') == 10
    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'net_quantity') == -10
    assert pm.get_value('test-id-2', 'stock', 'TEST2', 'sell_avg_price') == 15

    # same strategy new symbol
    trade = {'strategy_id': 'test-id-2', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:45:00'),
             'product_type': 'stock', 'symbol': 'TEST3', 'buy_sell': 'buy', 'quantity': 100, 'price': 15.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'current_position') == 100
    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'start_position') == 0
    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'buy_quantity') == 100
    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'sell_quantity') == 0
    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'net_quantity') == 100
    assert pm.get_value('test-id-2', 'stock', 'TEST3', 'buy_avg_price') == 15

    # new strategy same symbol
    trade = {'strategy_id': 'test-id-3', 'originator_id': 'orig_id', 'bartime': pd.Timestamp('2016-01-01 09:45:00'),
             'product_type': 'stock', 'symbol': 'TEST3', 'buy_sell': 'buy', 'quantity': 100, 'price': 15.0}
    pm._update_position_df(trade)

    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'current_position') == 100
    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'start_position') == 0
    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'buy_quantity') == 100
    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'sell_quantity') == 0
    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'net_quantity') == 100
    assert pm.get_value('test-id-3', 'stock', 'TEST3', 'buy_avg_price') == 15


def test_lexsort():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    # enter two trades out of lexicographical order
    pm.enter_trade('ORIG2', 'BBB', pd.Timestamp('2010-01-05 13:04:00', tz=NYC), 'stock', 'TEST', 'buy', 100, 50)
    pm.enter_trade('ORIG1', 'AAA', pd.Timestamp('2010-01-05 13:04:00', tz=NYC), 'stock', 'TEST', 'buy', 100, 50)
    assert pm.positions_df.index == [('AAA', 'stock', 'TEST'), ('BBB', 'stock', 'TEST')]


def test_get_value():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    pm.enter_trade('orig-id', 'test-id', pd.Timestamp('2010-01-05 13:04:00', tz=NYC), 'stock', 'TEST', 'buy', 100, 50)
    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 100

    # test that asking for an index that does not exist returns None
    assert pm.get_value('test-id', 'stock', 'BADSYM', 'current_position') is None


def test_enter_trade():
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)

    pm.enter_trade('orig-id', 'test-id', pd.Timestamp('2010-01-05 13:04:00', tz=NYC), 'stock', 'TEST', 'buy', 100, 50)

    # FILLED order
    order = tw.Order('1001', 'strategy.test-id', '123-456', 'test-id', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=100)
    order.state = 'FILLED'
    order.add_fill(111, pd.Timestamp('2029-01-01 12:31:13', tz=NYC),
                   pd.Timestamp('2020-01-01 09:30:00', tz=NYC), 100, 100, -1.0)
    pm.enter_trade_from_order(order)

    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 200
    assert pm.get_value('test-id', 'stock', 'TEST', 'start_position') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_quantity') == 200
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_quantity') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'net_quantity') == 200
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_avg_price') == 75
    assert pm.get_value('test-id', 'stock', 'TEST', 'commission') == -1.0

    # PARTIALLY_FILLED order
    order = tw.Order('1001', 'strategy.test-id', '123-456', 'test-id', 'stock', 'TEST', 'sell', 200, 'LIMIT', price=110)
    order.state = 'PARTIALLY_FILLED'
    order.add_fill(112, pd.Timestamp('2020-01-01 12:30:00', tz=NYC), pd.Timestamp('2000-01-01 12:30:00', tz=NYC),
                   100, 110, -1.0)
    pm.enter_trade_from_order(order)

    assert pm.get_value('test-id', 'stock', 'TEST', 'current_position') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'start_position') == 0
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_quantity') == 200
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_quantity') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'net_quantity') == 100
    assert pm.get_value('test-id', 'stock', 'TEST', 'buy_avg_price') == 75
    assert pm.get_value('test-id', 'stock', 'TEST', 'sell_avg_price') == 110
    assert pm.get_value('test-id', 'stock', 'TEST', 'commission') == -2.0

    # Cannot enter order not in FILLED state
    order = tw.Order('1001', 'strategy.test-id', '123-456', 'test-id', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=100)
    order.state = 'LIVE'
    with pytest.raises(ValueError):
        pm.enter_trade_from_order(order)

    # Must be buy or sell
    with pytest.raises(ValueError):
        pm.enter_trade('orig-id', 'test-id', pd.Timestamp('2010-01-05 13:04:00', tz=NYC),
                       'stock', 'TEST', 'BAD', 100, 50)


def test_prior_day_close():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm)

    # initialize prior close with no positions
    pm.initialize_prior_close()
    assert len(pm.positions_df) == 0

    # enter some trades
    mdm.bartime = '2010-01-05 09:31:00'
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 44.4)

    pm.initialize_prior_close()
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'prior_close_price') == 67.98
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'prior_close_price') == 49.51

    # add trades to existing and a new symbol
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'sell', 100, 75)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.11', 'sell', 50, 110.75)

    pm.initialize_prior_close()
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'prior_close_price') == 67.98
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'prior_close_price') == 49.51
    assert pm.get_value('test-id', 'stock', 'test.sym.11', 'prior_close_price') == 108.37


def test_current_price():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm, '1min')

    # enter some trades
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')

    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 44.4)

    pm.initialize_prior_close()
    pm.update_current_prices()

    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'current_price') == 69.22
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_price') == 50.11

    # enter new trades and roll forward the time
    mdm.bartime = '2010-01-05 09:32:00'
    mdm.update('stock', '1min')

    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'sell', 100, 72)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.11', 'sell', 50, 108.55)

    pm.initialize_prior_close()
    pm.update_current_prices()

    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'current_price') == 68.97
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_price') == 49.66
    assert pm.get_value('test-id', 'stock', 'test.sym.11', 'current_price') == 99.88

    # roll forward to a time with missing prices, confirm uses last valid bar
    mdm.bartime = '2010-01-05 17:00:00'
    mdm.update('stock', '1min')

    # confirm the current bar is empty, None data
    expected = {'datetime': pd.Timestamp('2010-01-05 17:00:00', tz=NYC), 'open': None, 'high': None, 'low': None,
                'close': None, 'volume': None}
    assert mdm.current_bar('stock', 'test.sym.9', '1min') == expected
    assert mdm.current_bar('stock', 'test.sym.10', '1min') == expected
    assert mdm.current_bar('stock', 'test.sym.11', '1min') == expected

    # confirm that the last valid bar was used for everything
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'current_price') == 68.97
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_price') == 49.66
    assert pm.get_value('test-id', 'stock', 'test.sym.11', 'current_price') == 99.88


def test_today_close():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm, '1min')

    # enter some trades
    mdm.bartime = '2010-01-05 09:32:00'
    mdm.update('stock', '1min')

    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 44.4)
    pm.enter_trade('orig-id', 'test-id', mdm.bartime, 'stock', 'test.sym.11', 'sell', 50, 108.55)

    # update to live prices
    pm.initialize_prior_close()
    pm.update_current_prices()

    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'current_price') == 68.97
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_price') == 49.66
    assert pm.get_value('test-id', 'stock', 'test.sym.11', 'current_price') == 99.88

    # load today's EOD 1D market data
    mdm.bartime = '2010-01-05 16:00:00'
    mdm.update('stock', '1D')

    # Insert today's close
    pm.insert_today_close()
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'current_price') == 52.97
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_price') == 60.43
    assert pm.get_value('test-id', 'stock', 'test.sym.11', 'current_price') == 88.11


def test_update_pnl():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm, '1min')

    # pnl with no positions will just skip the calculation
    pm.update_pnl()

    # flat open, flat close
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70, commission=-10.0)
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'sell', 200, 80, commission=-20.0)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 60)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 72, commission=-10.0)

    pm.update_pnl()

    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'buy_pnl'), 2) == 596.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'sell_pnl'), 2) == 2404.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'trade_pnl'), 2) == 3000.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'position_pnl'), 2) == 0.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'gross_pnl'), 2) == 3000.0
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'commission') == approx(-30.0)
    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'net_pnl') == approx(2970.0)

    # open and close of bar
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'buy_pnl'), 2) == 0.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'sell_pnl'), 2) == 2249.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'trade_pnl'), 2) == 2249.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'position_pnl'), 2) == -60.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'gross_pnl'), 2) == 2189.0
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'commission') == approx(-10.0)
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'net_pnl') == approx(2179)

    mdm.bartime = '2010-01-05 09:32:00'
    mdm.update('stock', '1min')
    pm.enter_trade('orig-id3', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'buy', 50, 70)

    pm.update_pnl()

    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'buy_pnl'), 2) == -1024.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'sell_pnl'), 2) == 2249.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'trade_pnl'), 2) == 1224.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'position_pnl'), 2) == -7.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'gross_pnl'), 2) == 1217.0
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'commission') == approx(-10.0)
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'net_pnl') == approx(1207.0)

    # roll the bar one head, only position pnl changes
    mdm.bartime = '2010-01-05 09:33:00'
    mdm.update('stock', '1min')

    pm.update_pnl()

    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'buy_pnl'), 2) == -1024.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'sell_pnl'), 2) == 2249.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'trade_pnl'), 2) == 1224.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'position_pnl'), 2) == -20.5
    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'gross_pnl'), 2) == 1204.0
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'commission') == approx(-10.0)
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'net_pnl') == approx(1194.0)


def test_metrics():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm, '1min')

    # add metrics
    pnl = metric.PositionManagerMetric(mdm, pm, 'net_pnl', sum)
    pm.add_eod_metric(pnl, 'net_pnl_all')

    # flat open, flat close
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70, commission=-10.0)
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'sell', 200, 80, commission=-20.0)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 60)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 72, commission=-10.0)

    pm.update_pnl()

    assert pm.get_value('test-id', 'stock', 'test.sym.9', 'net_pnl') == approx(2970.0)
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'net_pnl') == approx(2179)

    pm.calculate_eod_metrics(mdm.bartime)

    expected = 2970.0 + 2179
    actual = pm.eod_metrics['net_pnl_all'].value(0)
    assert actual == approx(expected)


def test_start_position_pnl():
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)

    # setup the PositionManager
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('testpm', oms, None)
    pm.setup_market_data(mdm, '1min')

    # enter some trades
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')

    # create row then force open positions in test.sym.10
    pm.initialize_row({'strategy_id': 'test-id', 'product_type': 'stock', 'symbol': 'test.sym.10'})
    pm.set_value('test-id', 'stock', 'test.sym.10', 'start_position', 50)
    pm.set_value('test-id', 'stock', 'test.sym.10', 'current_position', 50)
    pm.update_pnl()

    # confirm the pnl calculation on only start position
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'position_pnl') == approx(50 * (50.11 - 49.51))
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'gross_pnl') == approx(50 * (50.11 - 49.51))

    # enter some trades for test.sym.9 and 10
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70, commission=-10.0)
    pm.enter_trade('orig-id1', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'sell', 200, 80, commission=-20.0)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 60)
    pm.enter_trade('orig-id2', 'test-id', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 72, commission=-10.0)

    pm.update_pnl()

    # confirm PnL for position with no start position
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'trade_pnl'), 2) == 3000.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'position_pnl'), 2) == 0.0
    assert round(pm.get_value('test-id', 'stock', 'test.sym.9', 'gross_pnl'), 2) == 3000.0

    # confirm PnL for start position and trades
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'start_position') == 50.0
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'current_position') == -50.0

    assert round(pm.get_value('test-id', 'stock', 'test.sym.10', 'trade_pnl'), 2) == 2249.0
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'position_pnl') == approx(50 * (50.11 - 49.51) - 60)
    assert pm.get_value('test-id', 'stock', 'test.sym.10', 'gross_pnl') == approx(50 * (50.11 - 49.51) - 60 + 2249)


def test_book_fills():
    om = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('testpm', om, None)

    order1 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=10)
    om.new_order(order1)
    order2 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'sell', 250, 'LIMIT', price=25)
    om.new_order(order2)
    order3 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 500, 'LIMIT', price=50)
    om.new_order(order3)

    # FILLED Order
    order1.add_fill(100, pd.Timestamp('2010-05-05 14:10:00', tz=NYC), pd.Timestamp('2000-01-01 10:00:00', tz=NYC),
                    100, 10, -1.0)
    om.set_booked(order1, False)
    om.change_state(order1, 'FILLED')

    # PARTIALLY_FILLED Order
    order2.add_fill(101, pd.Timestamp('2010-05-05 14:10:00', tz=NYC), pd.Timestamp('2000-01-01 10:00:00', tz=NYC),
                    50, 25, -0.50)
    om.set_booked(order2, False)
    om.change_state(order2, 'PARTIALLY_FILLED')

    # check the to be booked
    assert om.to_be_booked_list() == [order1, order2]

    # Run book_fills
    pm.book_fills()
    assert om.to_be_booked_list() == []

    expected = rc.DataFrame({'strategy_id': ['stat_id', 'stat_id'],
                             'originator_id': ['strategy.stat_id', 'strategy.stat_id'],
                             'bartime': [pd.Timestamp('2000-01-01 15:00:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:00:00', tz='UTC')],
                             'product_type': ['stock', 'stock'], 'symbol': ['TEST', 'TEST'],
                             'buy_sell': ['buy', 'sell'], 'quantity': [100, 50], 'price': [10, 25],
                             'commission': [-1.0, -0.5], 'uuid': [order1.uuid, order2.uuid], 'fill_id': [100, 101]},
                            columns=['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                                     'quantity', 'price', 'commission', 'uuid', 'fill_id'])
    actual = pm.new_trades_df[['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                               'quantity', 'price', 'commission', 'uuid', 'fill_id']]
    assert_frame_equal(actual, expected)

    actual = pm.positions_df
    expected = rc.DataFrame({'current_position': 50.0, 'start_position': 0, 'net_quantity': 50.0,
                             'buy_quantity': 100.0, 'sell_quantity': 50.0, 'buy_avg_price': 10.0,
                             'sell_avg_price': 25.0, 'buy_pnl': 0.0, 'sell_pnl': 0.0, 'trade_pnl': 0.0,
                             'position_pnl': 0.0, 'gross_pnl': 0.0, 'commission': -1.5, 'net_pnl': 0.0,
                             'prior_close_price': None, 'current_price': None},
                            index=[('stat_id', 'stock', 'TEST')], sort=True,
                            index_name=('strategy_id', 'product_type', 'symbol'), columns=actual.columns)
    assert_frame_equal(actual, expected)

    assert order1.closed is True
    assert order1.booked is True
    assert order2.closed is False
    assert order2.booked is True

    # PARTIALLY_FILLED some more
    order2.add_fill(102, pd.Timestamp('2010-05-05 14:11:00', tz=NYC), pd.Timestamp('2000-01-01 10:15:00', tz=NYC),
                    25, 25, -0.25)
    om.set_booked(order2, False)
    om.change_state(order2, 'PARTIALLY_FILLED')

    assert om.to_be_booked_list() == [order2]
    pm.book_fills()
    assert om.to_be_booked_list() == []

    expected = rc.DataFrame({'strategy_id': ['stat_id', 'stat_id', 'stat_id'],
                             'originator_id': ['strategy.stat_id', 'strategy.stat_id', 'strategy.stat_id'],
                             'bartime': [pd.Timestamp('2000-01-01 15:00:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:00:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:15:00', tz='UTC')],
                             'product_type': ['stock', 'stock', 'stock'], 'symbol': ['TEST', 'TEST', 'TEST'],
                             'buy_sell': ['buy', 'sell', 'sell'], 'quantity': [100, 50, 25], 'price': [10, 25, 25],
                             'commission': [-1.0, -0.5, -0.25], 'uuid': [order1.uuid, order2.uuid, order2.uuid],
                             'fill_id': [100, 101, 102]},
                            columns=['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                                     'quantity', 'price', 'commission', 'uuid', 'fill_id'])
    actual = pm.new_trades_df[['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                               'quantity', 'price', 'commission', 'uuid', 'fill_id']]
    assert_frame_equal(actual, expected)
    assert order2.closed is False
    assert order2.booked is True

    # FILL it
    order2.add_fill(103, pd.Timestamp('2010-05-05 14:12:00', tz=NYC), pd.Timestamp('2000-01-01 10:20:00', tz=NYC),
                    25, 25, -0.25)
    om.set_booked(order2, False)
    om.change_state(order2, 'FILLED')

    assert om.to_be_booked_list() == [order2]
    pm.book_fills()
    assert om.to_be_booked_list() == []

    expected = rc.DataFrame({'strategy_id': ['stat_id', 'stat_id', 'stat_id', 'stat_id'],
                             'originator_id': ['strategy.stat_id', 'strategy.stat_id', 'strategy.stat_id',
                                               'strategy.stat_id'],
                             'bartime': [pd.Timestamp('2000-01-01 15:00:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:00:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:15:00', tz='UTC'),
                                         pd.Timestamp('2000-01-01 15:20:00', tz='UTC')],
                             'product_type': ['stock', 'stock', 'stock', 'stock'],
                             'symbol': ['TEST', 'TEST', 'TEST', 'TEST'], 'buy_sell': ['buy', 'sell', 'sell', 'sell'],
                             'quantity': [100, 50, 25, 25], 'price': [10, 25, 25, 25],
                             'commission': [-1.0, -0.5, -0.25, -0.25],
                             'uuid': [order1.uuid, order2.uuid, order2.uuid, order2.uuid],
                             'fill_id': [100, 101, 102, 103]},
                            columns=['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                                     'quantity', 'price', 'commission', 'uuid', 'fill_id'])
    actual = pm.new_trades_df[['originator_id', 'strategy_id', 'bartime', 'product_type', 'symbol', 'buy_sell',
                               'quantity', 'price', 'commission', 'uuid', 'fill_id']]
    assert_frame_equal(actual, expected)
    assert order2.closed is True
    assert order2.booked is True


def test_process_closed_orders():
    om = tw.OrderManager('unit_test', None)
    pm = tw.PositionManager('testpm', om, None)

    # test with orders in all closed states
    order1 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=10)
    order1.state = 'FILLED'
    order1.add_fill(111, pd.Timestamp('2000-05-05 12:13:14', tz=NYC),
                    pd.Timestamp('1990-01-01 09:30:00', tz=NYC), 100, 10, -0.10)
    order1.booked = False

    om.new_order(order1)
    order2 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'sell', 200, 'LIMIT', price=20)
    order2.state = 'RISK_REJECTED'
    om.new_order(order2)
    order3 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 300, 'LIMIT', price=30)
    order3.state = 'REJECTED'
    om.new_order(order3)
    order4 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'S', 400, 'LIMIT', price=40)
    order4.state = 'CANCELED'
    om.new_order(order4)

    assert om.orders_list({'strategy_id': 'stat_id', 'state': 'FILLED'}) == [order1]
    assert om.orders_list({'strategy_id': 'stat_id', 'state': 'RISK_REJECTED'}) == [order2]
    assert om.orders_list({'strategy_id': 'stat_id', 'state': 'REJECTED'}) == [order3]
    assert om.orders_list({'originator_id': 'strategy.stat_id', 'state': 'CANCELED'}) == [order4]

    assert om.closed_orders_df().get_entire_column('closed', as_list=True) == [False, False, False, False]
    assert om.closed_orders_df().get_entire_column('state', as_list=True) == [
        'FILLED', 'RISK_REJECTED', 'REJECTED', 'CANCELED'
    ]

    result = pm.book_fills()
    assert result == {'strategy.stat_id': [order1]}
    assert om.closed_orders_df().get_entire_column('closed', as_list=True) == [True, False, False, False]
    assert om.closed_orders_df().get_entire_column('state', as_list=True) == [
        'FILLED', 'RISK_REJECTED', 'REJECTED', 'CANCELED'
    ]

    # test with new batch of orders, but none in FILLED
    order5 = tw.Order('22', 'strategy.stat_id-2', '123-456-1', 'stat_id-2', 'stock', 'TEST', 'sell', 200, 'LIMIT',
                      price=20)
    order5.state = 'RISK_REJECTED'
    om.new_order(order5)
    order6 = tw.Order('22', 'strategy.stat_id-2', '123-456-1', 'stat_id-2', 'stock', 'TEST', 'buy', 300, 'LIMIT',
                      price=30)
    order6.state = 'REJECTED'
    om.new_order(order6)
    order7 = tw.Order('22', 'strategy.stat_id-2', '123-456-1', 'stat_id-2', 'stock', 'TEST', 'S', 400, 'LIMIT',
                      price=40)
    order7.state = 'CANCELED'
    om.new_order(order7)

    # confirm that the closed dict is correct
    actual = om.closed_orders_df()[['uuid', 'state', 'closed', 'strategy_id', 'originator_id']]
    expected = rc.DataFrame({'uuid': [order1.uuid, order2.uuid, order3.uuid, order4.uuid, order5.uuid,
                                      order6.uuid, order7.uuid],
                             'state': ['FILLED', 'RISK_REJECTED', 'REJECTED', 'CANCELED', 'RISK_REJECTED', 'REJECTED',
                                       'CANCELED'],
                             'closed': [True, False, False, False, False, False, False],
                             'strategy_id': ['stat_id', 'stat_id', 'stat_id', 'stat_id', 'stat_id-2', 'stat_id-2',
                                             'stat_id-2'],
                             'originator_id': ['strategy.stat_id', 'strategy.stat_id', 'strategy.stat_id',
                                               'strategy.stat_id', 'strategy.stat_id-2', 'strategy.stat_id-2',
                                               'strategy.stat_id-2']}, columns=actual.columns)
    assert_frame_equal(actual, expected)

    # process the orders in closed and confirm there are no new orders entered
    result = pm.book_fills()
    assert result == {}
    actual = om.closed_orders_df()[['uuid', 'state', 'closed', 'strategy_id', 'originator_id']]
    assert_frame_equal(actual, expected)

    # test with new batch of only FILLED orders
    order8 = tw.Order('1001', 'strategy.stat_id', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 550, 'LIMIT', price=55)
    order8.state = 'FILLED'
    order8.add_fill(222, pd.Timestamp('2000-05-05 12:13:14', tz=NYC),
                    pd.Timestamp('1990-01-01 09:30:00', tz=NYC), 550, 55, -5.50)
    order8.booked = False
    om.new_order(order8)

    order9 = tw.Order('22', 'strategy.stat_id-2', '123-456', 'stat_id-2', 'stock', 'TEST', 'sell', 440, 'LIMIT',
                      price=44)
    order9.state = 'FILLED'
    order9.add_fill(333, pd.Timestamp('2000-05-05 12:13:14', tz=NYC),
                    pd.Timestamp('1990-01-01 09:30:00', tz=NYC), 440, 44, -4.40)
    order9.booked = False
    om.new_order(order9)

    result = pm.book_fills()
    assert result == {'strategy.stat_id': [order8], 'strategy.stat_id-2': [order9]}
    actual = om.closed_orders_df()[['uuid', 'strategy_id', 'closed']]
    expected = rc.DataFrame({'uuid': [order1.uuid, order2.uuid, order3.uuid, order4.uuid, order5.uuid,
                                      order6.uuid, order7.uuid, order8.uuid, order9.uuid],
                             'closed': [True, False, False, False, False, False, False, True, True],
                             'strategy_id': ['stat_id', 'stat_id', 'stat_id', 'stat_id', 'stat_id-2', 'stat_id-2',
                                             'stat_id-2', 'stat_id', 'stat_id-2']}, columns=actual.columns)
    assert_frame_equal(actual, expected)

    # test with no orders in any closed states
    result = pm.book_fills()
    assert result == {}


def test_save_load_positions():
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('test_unit', oms, temp_tapdb)

    trade_time = pd.Timestamp('2010-01-05 13:04:00', tz='America/New_York')
    pm.enter_trade('orig', 'test.strat.1', trade_time, 'stock', 'test.sym.1', 'buy', 10, 50)
    pm.enter_trade('orig', 'test.strat.1', trade_time, 'stock', 'test.sym.2', 'sell', 8, 4.4)
    pm.enter_trade('orig', 'test.strat.2', trade_time, 'stock', 'test.sym.1', 'buy', 25, 75)

    pm.save_positions(pd.Timestamp('2010-08-01 16:00', tz='America/New_York'))

    # Get all for source
    expected = pd.DataFrame({'source': ['test_unit'] * 3, 'strategy': ['test.strat.1', 'test.strat.1', 'test.strat.2'],
                             'product_type': ['stock'] * 3, 'symbol': ['test.sym.1', 'test.sym.2', 'test.sym.1'],
                             'datetime': [pd.Timestamp('2010-08-01 20:00:00', tz='UTC')] * 3,
                             'position': [10, -8, 25.0]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])
    actual = tapdb.get_positions(temp_tapdb, source='test_unit')
    pd_assert_frame_equal(actual.sort_values(by=['strategy', 'symbol']).reset_index(drop=True), expected)

    # add more trades at later date
    trade_time = pd.Timestamp('2010-01-05 14:10:00', tz='America/New_York')
    pm.enter_trade('orig', 'test.strat.2', trade_time, 'stock', 'test.sym.2', 'sell', 100, 15)
    pm.enter_trade('orig', 'test.strat.1', trade_time, 'stock', 'test.sym.2', 'sell', 8, 4.4)
    pm.enter_trade('orig', 'test.strat.2', trade_time, 'stock', 'test.sym.1', 'buy', 50, 55)

    pm.save_positions(pd.Timestamp('2010-08-02 16:00', tz='America/New_York'))

    # Get latest date all sources
    expected = pd.DataFrame({'source': ['test_unit'] * 4,
                             'strategy': ['test.strat.1', 'test.strat.1', 'test.strat.2', 'test.strat.2'],
                             'product_type': ['stock'] * 4,
                             'symbol': ['test.sym.1', 'test.sym.2', 'test.sym.1', 'test.sym.2'],
                             'datetime': [pd.Timestamp('2010-08-02 20:00:00', tz='UTC')] * 4,
                             'position': [10, -16, 75.0, -100]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    max_datetime = tapdb.max_datetime(temp_tapdb, 'test_unit')
    actual = tapdb.get_positions(temp_tapdb, source='test_unit', datetime=max_datetime)
    pd_assert_frame_equal(actual.sort_values(by=['strategy', 'symbol']).reset_index(drop=True), expected)

    # get by strategy (across times? odd but yes)
    expected = pd.DataFrame({'source': ['test_unit'] * 4,
                             'strategy': ['test.strat.1', 'test.strat.1', 'test.strat.1', 'test.strat.1'],
                             'product_type': ['stock'] * 4,
                             'symbol': ['test.sym.1', 'test.sym.2', 'test.sym.1', 'test.sym.2'],
                             'datetime': [pd.Timestamp('2010-08-01 20:00:00', tz='UTC'),
                                          pd.Timestamp('2010-08-01 20:00:00', tz='UTC'),
                                          pd.Timestamp('2010-08-02 20:00:00', tz='UTC'),
                                          pd.Timestamp('2010-08-02 20:00:00', tz='UTC')],
                             'position': [10, -8, 10, -16.0]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(temp_tapdb, source='test_unit', strategy='test.strat.1')
    pd_assert_frame_equal(actual.sort_values(by=['datetime', 'symbol']).reset_index(drop=True), expected)

    # get by strategy & datetime
    expected = pd.DataFrame({'source': ['test_unit'] * 2,
                             'strategy': ['test.strat.1', 'test.strat.1'],
                             'product_type': ['stock'] * 2,
                             'symbol': ['test.sym.1', 'test.sym.2'],
                             'datetime': [pd.Timestamp('2010-08-02 20:00:00', tz='UTC'),
                                          pd.Timestamp('2010-08-02 20:00:00', tz='UTC')],
                             'position': [10, -16.0]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(temp_tapdb, source='test_unit', strategy='test.strat.1', datetime=max_datetime)
    pd_assert_frame_equal(actual.sort_values(by=['datetime', 'symbol']).reset_index(drop=True), expected)

    # enter trade to get a position to zero
    pm.enter_trade('orig', 'test.strat.1', pd.Timestamp('2010-01-05 14:10:00', tz='America/New_York'), 'stock',
                   'test.sym.2', 'buy', 16, 4.4)
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.2', 'current_position') == 0.0

    # save positions and confirm the zero line in the save
    save_time = pd.Timestamp('2010-08-03 16:00', tz='America/New_York')
    pm.save_positions(save_time)

    actual = tapdb.get_positions(temp_tapdb, source='test_unit', strategy='test.strat.1', datetime=save_time)
    assert actual.loc[actual['symbol'] == 'test.sym.2', 'position'].array[0] == 0

    # Now load the positions for the first datetime
    pm.load_positions(pd.Timestamp('2010-08-01 16:00', tz='America/New_York'))

    expected = rc.DataFrame({'current_position': [10.0, -8, 25], 'start_position': [10.0, -8, 25],
                             'prior_close_price': [None] * 3, 'current_price': [None] * 3, 'net_quantity': [0] * 3,
                             'net_pnl': [0] * 3},
                            index=[('test.strat.1', 'stock', 'test.sym.1'),
                                   ('test.strat.1', 'stock', 'test.sym.2'),
                                   ('test.strat.2', 'stock', 'test.sym.1')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    actual = pm.positions_df
    actual = actual[expected.columns]
    assert_frame_equal(actual, expected)

    # load positions for the max datetime
    pm.load_positions(tapdb.max_datetime(temp_tapdb, source='test_unit'))

    expected = rc.DataFrame({'current_position': [10.0, 75, -100], 'start_position': [10.0, 75, -100],
                             'prior_close_price': [None] * 3, 'current_price': [None] * 3, 'net_quantity': [0] * 3,
                             'net_pnl': [0] * 3},
                            index=[('test.strat.1', 'stock', 'test.sym.1'),
                                   ('test.strat.2', 'stock', 'test.sym.1'),
                                   ('test.strat.2', 'stock', 'test.sym.2')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    actual = pm.positions_df
    actual = actual[expected.columns]
    assert_frame_equal(actual, expected)

    # confirm the zero position is not inserted into the positions_df
    assert ('test.strat.1', 'stock', 'test.sym.2') not in actual.index


def test_save_positions_df():
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('test_unit', oms, temp_tapdb)

    trade_time = pd.Timestamp('2009-10-15 16:00:00', tz='America/New_York')
    pm.enter_trade('orig', 'test.strat.1', trade_time, 'stock', 'test.sym.1', 'buy', 10, 50)
    pm.enter_trade('orig', 'test.strat.1', trade_time, 'stock', 'test.sym.2', 'sell', 8, 4.4)
    pm.enter_trade('orig', 'test.strat.2', trade_time, 'stock', 'test.sym.1', 'buy', 25, 75)

    pm.save_positions_df(pd.Timestamp('2010-08-01 16:00', tz='America/New_York'))
    actual = tapdb.get_positions_df(temp_tapdb, pm.id, pd.Timestamp('2010-08-01 16:00', tz='America/New_York'))
    assert_frame_equal(actual, pm.positions_df)


def test_stop():
    mdm, pm, oms = setup_objects()

    # add metrics
    pnl = metric.PositionManagerMetric(mdm, pm, 'gross_pnl', sum)
    pm.add_eod_metric(pnl, 'gross_pnl_all')

    # enter some trades
    mdm.bartime = '2010-01-05 09:32:00'
    mdm.update('stock', '1min')

    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 44.4)
    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.11', 'sell', 50, 108.55)

    # roll forward one step
    mdm.bartime = '2010-01-05 09:33:00'
    mdm.update('stock', '1min')

    # Run stop process and check
    pm.stop()

    assert pm.get_value('test.strat.1', 'stock', 'test.sym.9', 'gross_pnl') == approx(100 * (68.33 - 70.0))
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.10', 'gross_pnl') == approx(100 * (44.4 - 49.92))
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.11', 'gross_pnl') == approx(50 * (108.55 - 99.45))

    # Check TAPDB positions
    expected = pd.DataFrame({'source': ['test_unit'] * 3, 'strategy': ['test.strat.1'] * 3,
                             'product_type': ['stock'] * 3, 'symbol': ['test.sym.10', 'test.sym.11', 'test.sym.9'],
                             'datetime': [pd.Timestamp('2010-01-05 14:33:00', tz='UTC')] * 3,
                             'position': [-100.0, -50, 100]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    max_datetime = tapdb.max_datetime(temp_tapdb, 'test_unit')
    actual = tapdb.get_positions(temp_tapdb, source='test_unit', datetime=max_datetime)
    pd_assert_frame_equal(actual.sort_values(by=['strategy', 'symbol']).reset_index(drop=True), expected)

    # Get the stored positions_df
    saved = tapdb.get_positions_df(temp_tapdb, 'test_unit', pd.Timestamp('2010-01-05 09:33:00', tz='America/New_York'))
    assert_frame_equal(pm.positions_df, saved)

    # check that EOD metrics are calculated on stop()
    assert pnl[0] == approx(100 * (68.33 - 70.0) + 100 * (44.4 - 49.92) + 50 * (108.55 - 99.45))


def test_end_of_day():
    mdm, pm, oms = setup_objects()

    # add metrics
    pnl = metric.PositionManagerMetric(mdm, pm, 'gross_pnl', sum)
    pm.add_eod_metric(pnl, 'gross_pnl_all')

    # enter some trades
    mdm.bartime = '2010-01-05 09:32:00'
    mdm.update('stock', '1min')

    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.10', 'sell', 100, 44.4)
    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.11', 'sell', 50, 108.55)

    # update to live prices
    pm.initialize_prior_close()
    pm.update_current_prices()

    # load today's EOD 1D market data
    mdm.bartime = '2010-01-05 16:00:00'
    mdm.update('stock', '1D')

    # Run EOD process and check
    pm.end_of_day()
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.9', 'gross_pnl') == approx(100 * (52.97 - 70.0))
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.10', 'gross_pnl') == approx(100 * (44.4 - 60.43))
    assert pm.get_value('test.strat.1', 'stock', 'test.sym.11', 'gross_pnl') == approx(50 * (108.55 - 88.11))

    # Check TAPDB positions
    expected = pd.DataFrame({'source': ['test_unit'] * 3, 'strategy': ['test.strat.1'] * 3,
                             'product_type': ['stock'] * 3, 'symbol': ['test.sym.10', 'test.sym.11', 'test.sym.9'],
                             'datetime': [pd.Timestamp('2010-01-05 21:00:00', tz='UTC')] * 3,
                             'position': [-100.0, -50, 100]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    max_datetime = tapdb.max_datetime(temp_tapdb, 'test_unit')
    actual = tapdb.get_positions(temp_tapdb, source='test_unit', datetime=max_datetime)
    pd_assert_frame_equal(actual.sort_values(by=['strategy', 'symbol']).reset_index(drop=True), expected)

    # Get the stored positions_df
    saved = tapdb.get_positions_df(temp_tapdb, 'test_unit', pd.Timestamp('2010-01-05 16:00:00', tz='America/New_York'))
    assert_frame_equal(pm.positions_df, saved)

    # check the metric calculation
    expected_pnl = 100 * (52.97 - 70.0) + 100 * (44.4 - 60.43) + 50 * (108.55 - 88.11)
    expected = rc.Series([expected_pnl], index_name='datetime', sort=True,
                         index=[pd.Timestamp('2010-01-05 16:00:00', tz=NYC)])
    actual = pm.eod_metrics['gross_pnl_all'].data
    assert_series_equal(actual, expected, np_assert_almost_equal)


def test_begin_of_day():
    mdm, pm, oms = setup_objects()
    # enter some trades
    mdm.bartime = pd.Timestamp('2010-01-04 09:32:00', tz=NYC)
    mdm.update('stock', '1min')
    pm.enter_trade('orig-id', 'test.strat.1', mdm.bartime, 'stock', 'test.sym.9', 'buy', 100, 70)
    pm.enter_trade('orig-id', 'test.strat.2', mdm.bartime, 'stock', 'test.sym.10', 'sell', 10, 44.4)
    pm.enter_trade('orig-id', 'test.strat.2', mdm.bartime, 'stock', 'test.sym.11', 'buy', 500, 108.55)

    # execute EOD
    pm.initialize_prior_close()
    mdm.bartime = '2010-01-04 16:00:00'
    mdm.update('stock', '1D')
    pm.end_of_day()

    # roll the bar to the opening next day and execute the begin of day
    mdm.bartime = '2010-01-05 09:30:00'
    pm.begin_of_day()

    expected = rc.DataFrame({'current_position': [100.0, -10, 500], 'start_position': [100.0, -10, 500],
                             'prior_close_price': [67.98, 49.51, 108.37], 'current_price': [None] * 3},
                            index=[('test.strat.1', 'stock', 'test.sym.9'),
                                   ('test.strat.2', 'stock', 'test.sym.10'),
                                   ('test.strat.2', 'stock', 'test.sym.11')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    actual = pm.positions_df
    actual = actual[expected.columns]
    assert_frame_equal(actual, expected)

    # re-initialize the entire object stack to emulate a new live day
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = datalib.HistoricalDataManager(symboldf, **db_credentials)
    ldm = datalib.LiveDataManager(symboldf, **db_credentials)
    mdm = datalib.MarketDataManager(hdm, ldm)
    oms = tw.OrderManager('unit_test', None)
    pm = position_manager.PositionManager('test_unit', oms, temp_tapdb)
    pm.setup_market_data(mdm, '1min')

    assert len(pm.positions_df) == 0
    mdm.bartime = '2010-01-05 09:30'
    pm.begin_of_day()

    expected = rc.DataFrame({'current_position': [100.0, -10, 500], 'start_position': [100.0, -10, 500],
                             'prior_close_price': [67.98, 49.51, 108.37], 'current_price': [None] * 3},
                            index=[('test.strat.1', 'stock', 'test.sym.9'),
                                   ('test.strat.2', 'stock', 'test.sym.10'),
                                   ('test.strat.2', 'stock', 'test.sym.11')],
                            index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    actual = pm.positions_df
    actual = actual[expected.columns]
    assert_frame_equal(actual, expected)
