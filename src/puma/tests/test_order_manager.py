"""
unit tests of the OrderManager class
"""

import database.utils as dbutils
import puma as tw
import pandas as pd
import pytest
import raccoon as rc
from database import tapdb, metadb, strategydb
from puma import order_manager
from puma.utils import assert_orders_equal
from utils import collections
from raccoon.utils import assert_frame_equal

prod_tapdb = None
temp_tapdb = None


def setup_module():
    global prod_tapdb, temp_tapdb
    
    tapdb.delete_db("temp")
    strategydb.delete_db("temp")
    strategydb.create_db("temp")
    tapdb.create_db("temp")
    prod_tapdb = tapdb.engine(host="temp")
    temp_strategydb = strategydb.engine(host="temp")

    # attach the stock symbolDB
    metadb.delete_db("temp", "stock")
    metadb.create_db("temp", "stock")
    seng = metadb.engine("temp", "stock")
    dbutils.attach_schema(prod_tapdb, "stock", "temp")

    # setup default data
    dbutils.upload_name(seng, "symbol", "TEST")
    dbutils.upload_name(seng, "symbol", "AAPL")
    dbutils.upload_name(seng, "symbol", "MSFT")
    strategydb.insert_strategy(temp_strategydb, "test.strat.1")
    tapdb.insert_source(prod_tapdb, "test_unit")

    temp_tapdb = dbutils.make_engine('temp_tapdb', host="temp", existing=False)
    dbutils.copy_table_schema(prod_tapdb, temp_tapdb)

    # dispose of unneeded engines
    seng.dispose()
    temp_strategydb.dispose()


def teardown_module():
    prod_tapdb.dispose()
    temp_tapdb.dispose()


def test_construction():  # Global variables
    om = order_manager.OrderManager('unit_test', temp_tapdb)
    assert om.id == 'unit_test'


def test_new_order():
    om = order_manager.OrderManager('unit_test', None)
    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'STAGED'

    om.new_order(order)
    actual = om.orders_list()
    assert_orders_equal(order, actual[0])


def test_get_order():
    om = order_manager.OrderManager('unit_test', None)

    order1 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 10, 'LIMIT', price=11)
    order1.state = 'CREATED'
    om.new_order(order1)
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'AAPL', 'sell', 20, 'LIMIT', price=22)
    order2.state = 'STAGED'
    om.new_order(order2)

    actual = om.order(order1.uuid)
    assert actual == order1


def test_order_list():
    om = order_manager.OrderManager('unit_test', None)

    # test with no orders
    actual = om.orders_list()
    assert actual == []

    # now with some orders
    order1 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 10, 'LIMIT', price=11)
    order1.state = 'CREATED'
    om.new_order(order1)
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'AAPL', 'sell', 20, 'LIMIT', price=22)
    order2.state = 'STAGED'
    om.new_order(order2)
    order3 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'MSFT', 'buy', 30, 'LIMIT', price=33)
    order3.state = 'LIVE'
    om.new_order(order3)
    order4 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'sell', 40, 'LIMIT', price=44)
    order4.state = 'FILLED'
    om.close_order(order4)
    om.set_booked(order4, True)
    om.new_order(order4)
    order5 = tw.Order('002-002', 'orig_02', '123-456', 'stat_id', 'stock', 'PART', 'buy', 50, 'LIMIT', price=55)
    order5.state = 'FILLED'
    order5.booked = False
    om.new_order(order5)
    order6 = tw.Order('002-002', 'orig_02', '123-456', 'stat_id', 'stock', 'NEXT', 'buy', 100, 'LIMIT', price=100)
    order6.state = 'PARTIALLY_FILLED'
    om.new_order(order6)

    actual = om.orders_list()
    assert actual == [order1, order2, order3, order4, order5, order6]

    actual = om.orders_list({'state': 'LIVE'})
    assert actual == [order3]

    actual = om.orders_list({'symbol': 'TEST'})
    assert actual == [order1, order4]

    actual = om.orders_list({'symbol': 'TEST', 'state': ['LIVE', 'FILLED']})
    assert actual == [order4]

    actual = om.orders_list({'symbol': 'MSFT', 'state': ['FILLED']})
    assert actual == []

    actual = om.orders_list({'symbol': ['AAPL', 'MSFT'], 'state': tw.order.states()['open']})
    assert actual == [order2, order3]

    actual = om.orders_list({'closed': True})
    assert actual == [order4]

    actual = om.orders_list({'booked': True})
    assert actual == [order4]

    actual = om.orders_list({'booked': False})
    assert actual == [order5]

    actual = om.orders_list({'closed': True, 'booked': False})
    assert actual == []

    om.set_booked(order6, False)
    actual = om.orders_list({'booked': False})
    assert actual == [order5, order6]

    actual = om.orders_list({'originator_id': 'orig_02'})
    assert actual == [order5, order6]


def test_order_dfs():
    om = order_manager.OrderManager('unit_test', None)
    assert len(om.orders_df()) == 0
    assert len(om.open_orders_df()) == 0
    assert len(om.closed_orders_df()) == 0

    order1 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order1.state = 'STAGED'
    om.new_order(order1)

    actual = om.orders_df()
    expected = rc.DataFrame(order1.to_dict(), columns=actual.columns)
    assert_frame_equal(actual, expected)

    actual = om.open_orders_df().get_columns(0, list(order1.to_dict().keys()), as_dict=True)
    del actual['index']
    expected = order1.to_dict()
    assert actual == expected

    # test when there are no closed and no processed
    assert len(om.closed_orders_df()) == 0

    # add another open order1
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'future', 'TEST', 'buy', 5, 'LIMIT', price=4)
    order2.state = 'STAGED'
    om.new_order(order2)

    actual = om.open_orders_df()
    expected = rc.DataFrame(collections.invert_list_of_dict([order1.to_dict(), order2.to_dict()]),
                            columns=actual.columns)
    assert_frame_equal(actual, expected)

    # use a filter on the open orders
    actual = om.open_orders_df({'product_type': 'future'})
    expected = rc.DataFrame(order2.to_dict(), columns=actual.columns)
    assert_frame_equal(actual, expected)

    # test closed and no processed
    order3 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'sell', 55, 'LIMIT', price=400)
    order3.state = 'FILLED'
    om.new_order(order3)

    actual = om.closed_orders_df()
    expected = rc.DataFrame(order3.to_dict(), columns=actual.columns)
    assert_frame_equal(actual, expected)

    # test closed and processed
    om.close_order(order3)
    actual = om.orders_df({'closed': True})
    expected = rc.DataFrame(order3.to_dict(), columns=actual.columns)
    assert_frame_equal(actual, expected)

    order4 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST2', 'buy', 455, 'LIMIT', price=4)
    order4.state = 'FILLED'
    om.new_order(order4)

    actual = om.closed_orders_df()
    expected = rc.DataFrame(collections.invert_list_of_dict([order3.to_dict(), order4.to_dict()]),
                            columns=actual.columns)
    assert_frame_equal(actual, expected)

    # test using filter on closed_orders_df()
    actual = om.closed_orders_df({'symbol': 'TEST2'})
    expected = rc.DataFrame(order4.to_dict(), columns=actual.columns)
    assert_frame_equal(actual, expected)

    actual = om.closed_orders_df({'product_type': 'stock'})
    expected = rc.DataFrame(collections.invert_list_of_dict([order3.to_dict(), order4.to_dict()]),
                            columns=actual.columns)
    assert_frame_equal(actual, expected)

    # test from all
    actual = om.orders_df({'symbol': 'TEST'})
    expected = rc.DataFrame(collections.invert_list_of_dict([order1.to_dict(), order2.to_dict(), order3.to_dict()]),
                            columns=actual.columns)
    assert_frame_equal(actual, expected)


def test_change_state():
    om = order_manager.OrderManager('unit_test', None)
    assert len(om.open_orders_df()) == 0

    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)

    actual = om._get_orders()
    assert len(actual) == 1
    assert_orders_equal(order, actual[order.uuid, 'object'])
    assert actual[order.uuid, 'state'] == 'CREATED'

    om.change_state(order, 'STAGED')
    actual = om._get_orders()
    assert len(actual) == 1
    assert actual[order.uuid, 'state'] == 'STAGED'
    assert_orders_equal(order, actual[order.uuid, 'object'])

    assert om.orders_list({'state': tw.order.states()['open']}) == [order]
    assert om.orders_list({'state': tw.order.states()['closed']}) == []

    om.change_state(order, 'FILLED')
    actual = om._get_orders()
    assert len(actual) == 1
    assert actual[order.uuid, 'state'] == 'FILLED'
    assert_orders_equal(order, actual[order.uuid, 'object'])

    assert om.orders_list({'state': tw.order.states()['open']}) == []
    assert om.orders_list({'state': tw.order.states()['closed']}) == [order]

    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order2.state = 'CREATED'
    om.new_order(order2)
    om.change_state(order2, 'LIVE')
    assert om.orders_list({'state': 'LIVE'}) == [order2]
    assert om.orders_list({'state': tw.order.states()['closed']}) == [order]


def test_close_order():
    om = order_manager.OrderManager('unit_test', None)
    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)

    assert len(om.open_orders_df()) == 1
    assert len(om.closed_orders_df()) == 0

    # cannot close order that is not in closed state
    with pytest.raises(RuntimeError):
        om.close_order(order)

    om.change_state(order, 'FILLED')
    om.close_order(order)

    assert len(om.open_orders_df()) == 0
    actual = om.closed_orders_df()
    assert len(actual) == 1
    assert order.closed is True
    assert actual.get_entire_column('closed', as_list=True) == [True]

    # add another order and make sure the data frame works with multiple orders
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order2.state = 'CREATED'
    om.new_order(order2)

    actual = om.orders_df()
    assert actual.get_entire_column('closed', as_list=True) == [True, False]


def test_replace_order():
    om = order_manager.OrderManager('unit_test', None)
    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)

    assert len(om.open_orders_df()) == 1
    assert len(om.closed_orders_df()) == 0

    om.replace_order(order, 100, price=10.0)

    actual = om.open_orders_df()
    assert len(actual) == 1
    assert actual.get_entire_column('state', as_list=True) == ['REPLACE_REQUESTED']
    assert actual.get_entire_column('quantity', as_list=True) == [100]
    assert actual.get_entire_column('details', as_list=True) == [{'price': 10.0}]


def test_set_booked():
    om = order_manager.OrderManager('unit_test', None)
    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)
    # order in booked value of None, does not show up in to be booked
    assert om.to_be_booked_list() == []

    # change the booked value to False and it does show up
    om.change_state(order, 'FILLED')
    om.set_booked(order, False)
    assert om.to_be_booked_list() == [order]

    # change booked to True and it is gone from list
    om.set_booked(order, True)
    assert om.to_be_booked_list() == []

    # add another order and make sure the data frame works with multiple orders
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order2.state = 'CREATED'
    om.new_order(order2)

    actual = om.orders_df()
    assert actual.get_entire_column('booked', as_list=True) == [True, None]
    # change the booked value to False and it does show up
    om.change_state(order2, 'FILLED')
    om.set_booked(order2, False)
    assert om.to_be_booked_list() == [order2]


def test_to_be_booked_list():
    om = order_manager.OrderManager('unit_test', None)
    assert len(om.open_orders_df()) == 0

    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)

    # confirm that with nothing in a closed state it is empty
    actual = om.to_be_booked_list()
    assert actual == []

    # add another order in an open state
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order2.state = 'LIVE'
    om.new_order(order2)

    # now change first order to FILLED
    om.set_booked(order, False)
    om.change_state(order, 'FILLED')
    actual = om.to_be_booked_list()
    assert actual == [order]
    assert actual == om.orders_list({'state': 'FILLED'})

    # now book that first order and see it flow out of the to_be_booked_list but still in closed
    om.set_booked(order, True)
    om.close_order(order)
    assert om.to_be_booked_list() == []
    assert om.orders_list({'state': tw.order.states()['closed']}) == [order]

    # move the next to closed state CANCELED, confirm does not show on to be closed
    om.change_state(order2, 'CANCELED')
    assert om.to_be_booked_list() == []
    assert om.orders_list({'state': tw.order.states()['closed']}) == [order, order2]

    # add another order in PARTIALLY_FILLED state
    order3 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order3.state = 'PARTIALLY_FILLED'
    om.new_order(order3)
    om.set_booked(order3, False)

    assert om.to_be_booked_list() == [order3]


def test_cancels_to_process():
    om = order_manager.OrderManager('unit_test', None)
    assert len(om.open_orders_df()) == 0

    order = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order.state = 'CREATED'
    om.new_order(order)

    # confirm that with nothing in a closed state it is empty
    actual = om.cancels_to_process()
    assert actual == []

    # add another order in an open state
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 75, 'LIMIT', price=40)
    order2.state = 'LIVE'
    om.new_order(order2)

    # now change first order to CANCELED
    om.change_state(order, 'CANCELED')
    actual = om.cancels_to_process()
    assert actual == [order]
    assert actual == om.orders_list({'state': 'CANCELED'})

    # now process cancel first order and see it flow out of the list but still in closed
    om.close_order(order)
    assert om.cancels_to_process() == []
    assert om.orders_list({'state': tw.order.states()['closed']}) == [order]


def test_market_state():
    om = order_manager.OrderManager('unit_test', None)

    # no state set will cause error
    with pytest.raises(KeyError):
        om.market_state('stock')

    om.market_state('stock', True)
    assert om.market_state('stock') is True

    om.market_state('stock', False)
    assert om.market_state('stock') is False

    with pytest.raises(ValueError):
        om.market_state('stock', 'BAD')


def test_stop():
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    om = order_manager.OrderManager('test_unit', temp_tapdb)

    # create some orders
    order1 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 10, 'LIMIT', price=11)
    order1.state = 'CREATED'
    om.new_order(order1)
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'AAPL', 'sell', 20, 'LIMIT', price=22)
    order2.state = 'STAGED'
    om.new_order(order2)

    # run stop
    om.stop(pd.Timestamp('2017-03-07 12:00', tz='America/New_York'))

    # confirm the orders have NOT been flushed, but have been saved
    assert len(om.orders_df()) == 2
    assert om.orders_list() == [order1, order2]

    actual = tapdb.get_orders_df(temp_tapdb, 'test_unit', pd.Timestamp('2017-03-07 12:00', tz='America/New_York'))

    # only compare some columns as others are persisted with representation of Timestamp
    cols = ['quantity', 'symbol', 'event_type', 'closed', 'strategy_uuid', 'buy_sell', 'product_type', 'originator_id',
            'type', 'strategy_id', 'state', 'originator_id', 'uuid', 'details']

    assert_frame_equal(actual[cols], om.orders_df()[cols])


def test_end_of_day():
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])
    om = order_manager.OrderManager('test_unit', temp_tapdb)

    # create some orders
    order1 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'TEST', 'buy', 10, 'LIMIT', price=11)
    order1.state = 'CREATED'
    om.new_order(order1)
    order2 = tw.Order('001-001', 'orig_01', '123-456', 'stat_id', 'stock', 'AAPL', 'sell', 20, 'LIMIT', price=22)
    order2.state = 'STAGED'
    om.new_order(order2)

    assert len(om.orders_df()) == 2
    assert om.orders_list() == [order1, order2]

    # capture the orders before save for future compare
    order_snapshot = om.orders_df()

    # run end of day
    om.end_of_day(pd.Timestamp('2017-03-07 16:00', tz='America/New_York'))

    # confirm the orders have been saved and flushed
    assert len(om.orders_df()) == 0
    assert om.orders_list() == []

    actual = tapdb.get_orders_df(temp_tapdb, 'test_unit', pd.Timestamp('2017-03-07 16:00', tz='America/New_York'))

    # only compare some columns as others are persisted with representation of Timestamp
    cols = ['quantity', 'symbol', 'event_type', 'closed', 'strategy_uuid', 'buy_sell', 'product_type', 'originator_id',
            'type', 'strategy_id', 'state', 'originator_id', 'uuid', 'details']

    assert_frame_equal(actual[cols], order_snapshot[cols])
