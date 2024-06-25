"""
unit test for PaperExchange
"""

import os
from collections import namedtuple

import data as datalib
import pandas as pd
import pytest
import raccoon as rc
from data import data_manager, market_data_manager
from puma import exchange
from raccoon.utils import assert_frame_equal

# Global variables
inst_dir = None
fill_tuple = namedtuple('Fill', "id, timestamp, quantity, price")


def setup_module():
    global inst_dir
    inst_dir = os.path.normpath("./puma/data/tests/inst/")  # the directory of the csv files in test dir


def test_initialize():
    pe = exchange.PaperExchange()
    assert isinstance(pe.uuid, str)
    assert pe.live_frequency == '1min'

    pe = exchange.PaperExchange(live_frequency='1D')
    assert pe.live_frequency == '1D'

    pe.live_frequency = '5min'
    assert pe.live_frequency == '5min'

    assert_frame_equal(pe.open_orders_df, rc.DataFrame())
    assert_frame_equal(pe.closed_orders_df, rc.DataFrame())


def test_set_parameters():
    params = {'fill_multiplier': 0.25}
    paper_ex = exchange.PaperExchange(parameters=params)

    assert paper_ex.parameters['fill_multiplier'] == 0.25

    # test with bad parameter
    with pytest.raises(ValueError):
        exchange.PaperExchange(parameters={'BAD': 1})


def test_receive_order():
    pe = exchange.PaperExchange()
    order_id = pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)

    # test retrieve by order_id
    expected = {'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                'price': 10.1, 'quantity': 30, 'order_id': order_id, 'state': 'LIVE', 'fill_quantity': None,
                'fill_price': None, 'replaces': [exchange.Exchange_Replace(quantity=30, details={'price': 10.1})]}
    assert set(pe.get_order(order_id)) == set(expected)

    # test the open orders list works
    actual = pe.open_orders_list[0]
    assert actual == expected

    # test the open orders data frame works
    actual = pe.open_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'state': 'LIVE', 'buy_sell': 'sell',
                             'order_type': 'LIMIT', 'price': 10.1, 'quantity': 30,
                             'order_id': actual.get(columns='order_id', as_list=True)[0], 'fill_quantity': None,
                             'fill_price': None,
                             'replaces': [[exchange.Exchange_Replace(quantity=30, details={'price': 10.1})]]},
                            index=actual.get(columns='order_id', as_list=True))[actual.columns]
    assert_frame_equal(actual, expected)

    # add another order and check open_orders_df
    pe.receive_order('stock', 'test.sym.9', 'buy', 22, 'LIMIT', price=15.0)

    actual = pe.open_orders_df
    expected = rc.DataFrame({'product_type': ['stock', 'stock'], 'symbol': ['test.sym.3', 'test.sym.9'],
                             'state': ['LIVE'] * 2, 'buy_sell': ['sell', 'buy'], 'order_type': ['LIMIT'] * 2,
                             'price': [10.1, 15.0], 'quantity': [30, 22],
                             'order_id': actual.get(columns='order_id', as_list=True), 'fill_quantity': [None] * 2,
                             'fill_price': [None] * 2,
                             'replaces': [[exchange.Exchange_Replace(quantity=30, details={'price': 10.1})],
                                          [exchange.Exchange_Replace(quantity=22, details={'price': 15.0})]]},
                            index=actual.get(columns='order_id', as_list=True))[actual.columns]
    assert_frame_equal(actual, expected)


def test_fill_order():
    pe = exchange.PaperExchange()

    # a fully FILLED order
    pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')
    ordr = pe.open_order(ordr_id)

    pe.fill_order(ordr, 30, pd.Timestamp('2000-10-10 12:01:01', tz='America/New_York'))
    actual = pe.closed_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 10.1, 'quantity': 30, 'state': 'FILLED', 'fill_price': 10.1, 'fill_quantity': 30,
                             'close_bar_timestamp': pd.Timestamp('2000-10-10 16:01:01', tz='UTC'), 'order_id': ordr_id,
                             'fills': [[fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:01:01', tz='UTC'), 30,
                                                   10.10)]],
                             'replaces': [[exchange.Exchange_Replace(30, {'price': 10.1})]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)

    # PARTIALLY_FILLED order
    ordr_id2 = pe.receive_order('stock', 'test.sym.3', 'buy', 100, 'LIMIT', price=20.2)
    ordr2 = pe.open_order(ordr_id2)

    pe.fill_order(ordr2, 50, pd.Timestamp('2000-10-10 12:12:12', tz='America/New_York'))

    # test the only the FILLED order shows up in closed_orders
    assert pe.closed_orders_list == [ordr]

    # PARTIALLY_FILLED is in the open orders
    assert ordr2['state'] == 'PARTIALLY_FILLED'
    assert ordr2['fill_quantity'] == 50
    assert ordr2['fill_price'] == 20.2

    actual = pe.open_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'buy', 'order_type': 'LIMIT',
                             'price': 20.2, 'quantity': 100, 'state': 'PARTIALLY_FILLED', 'fill_price': 20.2,
                             'fill_quantity': 50, 'order_id': ordr_id2,
                             'fills': [
                                 [fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:12:12', tz='UTC'), 50, 20.2)]],
                             'replaces': [[exchange.Exchange_Replace(100, {'price': 20.2})]]},
                            index=[ordr_id2], columns=actual.columns)
    assert_frame_equal(actual, expected)

    # Fill the balance
    pe.fill_order(ordr2, 50, pd.Timestamp('2000-10-10 12:15:00', tz='America/New_York'))

    assert pe.open_orders_list == []
    assert pe.closed_orders_list == [ordr, ordr2]

    assert ordr2['state'] == 'FILLED'
    assert ordr2['fill_quantity'] == 100
    assert ordr2['fill_price'] == 20.2
    assert ordr2['fills'] == [fill_tuple(pe.fill_id - 2, pd.Timestamp('2000-10-10 16:12:12', tz='UTC'), 50, 20.2),
                              fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:15:00', tz='UTC'), 50, 20.2)]

    actual = pe.closed_orders_df
    expected = rc.DataFrame({'product_type': ['stock', 'stock'], 'symbol': ['test.sym.3', 'test.sym.3'],
                             'buy_sell': ['sell', 'buy'], 'order_type': ['LIMIT', 'LIMIT'],
                             'price': [10.1, 20.2], 'quantity': [30, 100], 'state': ['FILLED', 'FILLED'],
                             'fill_price': [10.1, 20.2], 'fill_quantity': [30, 100],
                             'close_bar_timestamp': [pd.Timestamp('2000-10-10 16:01:01', tz='UTC'),
                                                     pd.Timestamp('2000-10-10 16:15:00', tz='UTC')],
                             'order_id': [ordr_id, ordr_id2],
                             'fills': [
                                 [fill_tuple(pe.fill_id - 3, pd.Timestamp('2000-10-10 16:01:01', tz='UTC'), 30, 10.10)],
                                 [fill_tuple(pe.fill_id - 2, pd.Timestamp('2000-10-10 16:12:12', tz='UTC'), 50, 20.2),
                                  fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:15:00', tz='UTC'), 50, 20.2)]
                             ],
                             'replaces': [[exchange.Exchange_Replace(30, {'price': 10.1})],
                                          [exchange.Exchange_Replace(100, {'price': 20.2})]]},
                            index=[ordr_id, ordr_id2], columns=actual.columns)
    assert_frame_equal(actual, expected)


def test_fill_quantity():
    pe = exchange.PaperExchange()
    pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')
    ordr = pe.open_order(ordr_id)

    quantity = pe.fill_quantity(ordr, {'datetime': pd.Timestamp('2010-01-01 09:40:00'), 'open': 100, 'high': 101,
                                       'low': 99, 'close': 100, 'volume': 500})
    assert quantity == 30

    # max at half of total volume
    ordr_id = pe.receive_order('stock', 'test.sym.3', 'sell', 300, 'LIMIT', price=10.10)
    ordr = pe.open_order(ordr_id)
    assert ordr['quantity'] == 300
    quantity = pe.fill_quantity(ordr, {'datetime': pd.Timestamp('2010-01-01 09:40:00'), 'open': 100, 'high': 101,
                                       'low': 99, 'close': 100, 'volume': 201})
    assert quantity == 100

    # get the fill amount when some has already been filled
    ordr['fill_quantity'] = 100
    quantity = pe.fill_quantity(ordr, {'datetime': pd.Timestamp('2010-01-01 09:41:00'), 'open': 100, 'high': 101,
                                       'low': 99, 'close': 100, 'volume': 1000})
    assert quantity == 200

    # use fill_multiplier parameter set to 0.25
    pe = exchange.PaperExchange(parameters={'fill_multiplier': 0.25})
    pe.receive_order('stock', 'test.sym.3', 'sell', 300, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')
    ordr = pe.open_order(ordr_id)

    quantity = pe.fill_quantity(ordr, {'datetime': pd.Timestamp('2010-01-01 09:40:00'), 'open': 100, 'high': 101,
                                       'low': 99, 'close': 100, 'volume': 500})
    assert quantity == 500 * 0.25


def test_cancel_order():
    pe = exchange.PaperExchange()
    pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')
    ordr = pe.open_order(ordr_id)

    # get the order status
    assert pe.get_order(ordr_id)['state'] == 'LIVE'

    # receive a cancel request
    pe.receive_cancel(ordr_id)
    assert pe.get_order(ordr_id)['state'] == 'CANCEL_SENT'

    # cancel the order
    pe.cancel_order(ordr, pd.Timestamp('2000-10-10 12:01:01'))

    actual = pe.closed_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 10.1, 'quantity': 30, 'state': 'CANCELED',
                             'close_bar_timestamp': pd.Timestamp('2000-10-10 12:01:01'), 'order_id': ordr_id,
                             'fill_quantity': None, 'fill_price': None,
                             'replaces': [[exchange.Exchange_Replace(30, {'price': 10.1})]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)
    assert_frame_equal(pe.open_orders_df, rc.DataFrame())

    # attempting to cancel a closed order will pass
    id_closed = pe.receive_order('stock', 'test.sym.3', 'buy', 10, 'LIMIT', price=10.10)
    closed_order = pe.open_order(id_closed)
    pe.fill_order(closed_order, 10, pd.Timestamp('2002-02-02', tz='America/New_York'))
    pe.receive_cancel(id_closed)


def test_replace_order():
    pe = exchange.PaperExchange()
    pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')
    ordr = pe.open_order(ordr_id)

    # get the order status
    assert ordr['state'] == 'LIVE'

    # receive a replace request
    pe.receive_replace(ordr_id, quantity=50, details={'price': 55.5})
    assert ordr['state'] == 'REPLACE_SENT'
    assert ordr['replaces'] == [exchange.Exchange_Replace(30, {'price': 10.10}),
                                exchange.Exchange_Replace(50, {'price': 55.5})]

    # replace the order
    pe.replace_order(ordr, pd.Timestamp('2000-10-10 12:01:01'))

    actual = pe.open_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 55.5, 'quantity': 50, 'state': 'LIVE', 'order_id': ordr_id,
                             'fill_quantity': None, 'fill_price': None,
                             'replaces': [[exchange.Exchange_Replace(quantity=30, details={'price': 10.1}),
                                           exchange.Exchange_Replace(quantity=50, details={'price': 55.5})]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)
    assert len(pe.closed_orders_df) == 0

    # now partially fill the order
    pe.fill_order(ordr, 40, pd.Timestamp('2000-10-10 12:05:00', tz='America/New_York'))

    actual = pe.open_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 55.5, 'quantity': 50, 'state': 'PARTIALLY_FILLED', 'order_id': ordr_id,
                             'fill_quantity': 40, 'fill_price': 55.5,
                             'replaces': [[exchange.Exchange_Replace(quantity=30, details={'price': 10.1}),
                                           exchange.Exchange_Replace(quantity=50, details={'price': 55.5})]],
                             'fills': [[fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:05:00', tz='UTC'), 40,
                                                   55.5)]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)
    assert len(pe.closed_orders_df) == 0

    # now replace the order below the filled_quantity and it should move to FILLED state
    pe.receive_replace(ordr_id, quantity=40, details={'price': 22.2})
    pe.replace_order(ordr, pd.Timestamp('2000-10-10 12:10:00'))

    actual = pe.closed_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 22.2, 'quantity': 40, 'state': 'FILLED', 'order_id': ordr_id,
                             'fill_quantity': 40, 'fill_price': 55.5,
                             'close_bar_timestamp': pd.Timestamp('2000-10-10 12:10:00'),
                             'replaces': [[exchange.Exchange_Replace(quantity=30, details={'price': 10.1}),
                                           exchange.Exchange_Replace(quantity=50, details={'price': 55.5}),
                                           exchange.Exchange_Replace(quantity=40, details={'price': 22.2})]],
                             'fills': [[fill_tuple(pe.fill_id - 1, pd.Timestamp('2000-10-10 16:05:00', tz='UTC'), 40,
                                                   55.5)]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)
    assert len(pe.open_orders_df) == 0

    # attempting to replace a closed order will pass
    id_closed = pe.receive_order('stock', 'test.sym.3', 'buy', 10, 'LIMIT', price=10.10)
    closed_order = pe.open_order(id_closed)
    pe.fill_order(closed_order, 10, pd.Timestamp('2002-02-02', tz='America/New_York'))
    pe.receive_replace(id_closed, 100, details={'price': 50})


def test_process_orders():
    csvdf = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    lmdm = data_manager.LiveDataManager(csvdf)
    mdm = market_data_manager.MarketDataManager(None, lmdm)
    mdm.add_symbols('stock', 'test.sym.3', '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')
    pe = exchange.PaperExchange()
    id_1 = pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    id_2 = pe.receive_order('stock', 'test.sym.3', 'buy', 130, 'LIMIT', price=10.10)
    id_3 = pe.receive_order('stock', 'test.sym.3', 'buy', 500, 'LIMIT', price=1.10)
    id_4 = pe.receive_order('stock', 'test.sym.3', 'sell', 250, 'LIMIT', price=200.0)

    # cancel one of the orders, replace another
    pe.receive_cancel(id_3)
    pe.receive_replace(id_4, 100, {'price': 90.9})

    pe.process_orders(mdm)
    assert pe.closed_orders_list == [pe.get_order(id_1), pe.get_order(id_3)]
    assert pe.open_orders_list == [pe.get_order(id_2), pe.get_order(id_4)]

    # order fully FILLED
    expected = {'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                'price': 10.1, 'quantity': 30, 'state': 'FILLED', 'fill_price': 10.1, 'fill_quantity': 30,
                'fills': [fill_tuple(pe.fill_id - 2, pd.Timestamp('2010-01-01 09:31:00', tz='EST'), 30, 10.1)],
                'close_bar_timestamp': pd.Timestamp('2010-01-01 09:31:00', tz='EST'), 'order_id': id_1,
                'replaces': [exchange.Exchange_Replace(30, {'price': 10.10})]}
    assert pe.get_order(id_1) == expected

    # order still LIVE
    expected = {'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'buy', 'order_type': 'LIMIT',
                'price': 10.1, 'quantity': 130, 'state': 'LIVE', 'fill_price': None, 'fill_quantity': None,
                'order_id': id_2, 'replaces': [exchange.Exchange_Replace(130, {'price': 10.10})]}
    assert pe.get_order(id_2) == expected

    # order CANCELED
    expected = {'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'buy', 'order_type': 'LIMIT',
                'price': 1.1, 'quantity': 500, 'state': 'CANCELED', 'fill_quantity': None, 'fill_price': None,
                'close_bar_timestamp': pd.Timestamp('2010-01-01 09:31:00', tz='EST'), 'order_id': id_3,
                'replaces': [exchange.Exchange_Replace(500, {'price': 1.1})]}
    assert pe.get_order(id_3) == expected

    # order replaced and PARTIALLY FILLED
    expected = {'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                'price': 90.9, 'quantity': 100, 'state': 'PARTIALLY_FILLED', 'fill_price': 90.9, 'fill_quantity': 50,
                'fills': [fill_tuple(pe.fill_id - 1, pd.Timestamp('2010-01-01 09:31:00', tz='EST'), 50, 90.9)],
                'order_id': id_4,
                'replaces': [exchange.Exchange_Replace(250, {'price': 200.0}),
                             exchange.Exchange_Replace(100, {'price': 90.9})]}
    assert pe.get_order(id_4) == expected

    # Add a non-LIMIT order, test raise error
    id_bad = pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'MARKET', price=10.10)
    with pytest.raises(RuntimeError):
        pe.process_order(pe.open_order(id_bad), mdm)


def test_process_orders_missing_data():
    csvdf = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    lmdm = data_manager.LiveDataManager(csvdf)
    mdm = market_data_manager.MarketDataManager(None, lmdm)
    mdm.add_symbols('stock', 'test.sym.3', '1min')
    pe = exchange.PaperExchange()

    # setup orders
    id_1 = pe.receive_order('stock', 'test.sym.3', 'buy', 50, 'LIMIT', price=101.0)
    id_2 = pe.receive_order('stock', 'test.sym.3', 'sell', 50, 'LIMIT', price=90.0)
    id_3 = pe.receive_order('stock', 'test.sym.3', 'buy', 50, 'LIMIT', price=1.99)
    id_4 = pe.receive_order('stock', 'test.sym.3', 'sell', 50, 'LIMIT', price=199.99)

    # setup bar and process orders
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')
    pe.process_orders(mdm)

    # confirm open and closed orders
    assert pe.closed_orders_list == [pe.get_order(id_1), pe.get_order(id_2)]
    assert pe.open_orders_list == [pe.get_order(id_3), pe.get_order(id_4)]

    # orders fully FILLED
    assert pe.get_order(id_1)['state'] == 'FILLED'
    assert pe.get_order(id_2)['state'] == 'FILLED'

    # orders still LIVE
    assert pe.get_order(id_3)['state'] == 'LIVE'
    assert pe.get_order(id_4)['state'] == 'LIVE'

    # new orders
    id_5 = pe.receive_order('stock', 'test.sym.3', 'buy', 50, 'LIMIT', price=101.0)
    id_6 = pe.receive_order('stock', 'test.sym.3', 'sell', 50, 'LIMIT', price=90.0)

    # setup bar with None data and process orders
    mdm.bartime = '2010-01-01 09:31:45'  # Note the :45 seconds
    mdm.update('stock', '1min')

    # confirm that the low and high are none
    assert mdm.current_bar('stock', 'test.sym.3', '1min')['low'] is None
    assert mdm.current_bar('stock', 'test.sym.3', '1min')['high'] is None

    # process & confirm open and closed orders, id 5 & 6 remain because no good data
    pe.process_orders(mdm)
    assert pe.closed_orders_list == [pe.get_order(id_1), pe.get_order(id_2)]
    assert pe.open_orders_list == [pe.get_order(id_3), pe.get_order(id_4), pe.get_order(id_5), pe.get_order(id_6)]

    # orders fully FILLED
    assert pe.get_order(id_1)['state'] == 'FILLED'
    assert pe.get_order(id_2)['state'] == 'FILLED'

    # orders still LIVE
    assert pe.get_order(id_3)['state'] == 'LIVE'
    assert pe.get_order(id_4)['state'] == 'LIVE'
    assert pe.get_order(id_5)['state'] == 'LIVE'
    assert pe.get_order(id_6)['state'] == 'LIVE'

    # setup bar with good data and process orders
    mdm.bartime = '2010-01-01 09:32:00'
    mdm.update('stock', '1min')

    # confirm that the low and high are not None
    assert mdm.current_bar('stock', 'test.sym.3', '1min')['low'] is not None
    assert mdm.current_bar('stock', 'test.sym.3', '1min')['high'] is not None

    # process & confirm open and closed orders, id 5 & 6 now closed
    pe.process_orders(mdm)
    assert pe.closed_orders_list == [pe.get_order(id_1), pe.get_order(id_2), pe.get_order(id_5), pe.get_order(id_6)]
    assert pe.open_orders_list == [pe.get_order(id_3), pe.get_order(id_4)]

    # orders fully FILLED
    assert pe.get_order(id_1)['state'] == 'FILLED'
    assert pe.get_order(id_2)['state'] == 'FILLED'
    assert pe.get_order(id_5)['state'] == 'FILLED'
    assert pe.get_order(id_6)['state'] == 'FILLED'

    # orders still LIVE
    assert pe.get_order(id_3)['state'] == 'LIVE'
    assert pe.get_order(id_4)['state'] == 'LIVE'


def market_close():
    pe = exchange.PaperExchange()
    pe.receive_order('stock', 'test.sym.3', 'sell', 30, 'LIMIT', price=10.10)
    ordr_id = pe.open_orders_df.get(pe.open_orders_df.index[0], 'order_id')

    # get the order status
    assert pe.get_order(ordr_id)['state'] == 'LIVE'

    # run market close
    pe.market_close(pd.Timestamp('2000-10-10 16:00:00', tz='America/New_York'))

    actual = pe.closed_orders_df
    expected = rc.DataFrame({'product_type': 'stock', 'symbol': 'test.sym.3', 'buy_sell': 'sell', 'order_type': 'LIMIT',
                             'price': 10.1, 'quantity': 30, 'state': 'CANCELED',
                             'close_bar_timestamp': pd.Timestamp('2000-10-10 16:00:00', tz='America/New_York'),
                             'order_id': ordr_id, 'fill_quantity': None, 'fill_price': None,
                             'replaces': [[exchange.Exchange_Replace(30, {'price': 10.1})]]},
                            index=[ordr_id], columns=actual.columns)
    assert_frame_equal(actual, expected)
    assert_frame_equal(pe.open_orders_df, rc.DataFrame())
