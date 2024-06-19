"""
unit tests for Broker class and concrete implementations
"""

import os

import puma.data as datalib
import puma as tw
import pandas as pd
import pytest
import raccoon as rc
from config.database import credentials
from config.datetime import NYC
from puma import broker
from puma.exchange import Exchange_Fill
from puma.utils import assert_orders_equal
from raccoon.utils import assert_frame_equal

# Global variables
inst_dir = None
db_credentials = {}
lmdm = None


def setup_module():
    global inst_dir, db_credentials, lmdm
    inst_dir = os.path.normpath("./puma/data/tests/inst/")  # the directory of the csv files in test dir
    db_credentials = credentials('test', 'localhost', prefix='db_')
    csvdf = datalib.CsvDataFeed(inst_dir + '/csv_data_feed')
    lmdm = datalib.LiveDataManager(csvdf, **db_credentials)


def test_initialize():
    paper_ex = tw.PaperExchange()
    oms = tw.OrderManager('unit_test', None)
    bro = broker.PaperBroker('broker_id', oms, paper_ex)

    assert isinstance(bro._order_id, int)
    assert isinstance(bro.uuid, str)
    assert bro.broker_id == 'broker_id'


def test_set_parameters():
    params = {'stock_fee_per_share': 99.9}
    paper_ex = tw.PaperExchange()
    oms = tw.OrderManager('unit_test', None)
    bro = broker.PaperBroker('broker_id', oms, paper_ex, parameters=params)

    assert bro.parameters['stock_fee_per_share'] == 99.9

    # test with bad parameter
    with pytest.raises(ValueError):
        _ = broker.PaperBroker('broker_id', oms, paper_ex, parameters={'BAD': 1})


def test_order_to_exchange():
    ordr = tw.Order(1001, 'orig_id', 123, 'stat_id', 'stock', 'TEST', 's', 250, 'LIMIT', price=15.15)
    paper_ex = tw.PaperExchange()
    oms = tw.OrderManager('unit_test', None)
    bro = broker.PaperBroker('broker_id', oms, paper_ex)

    bro.send_order_to_exchange(ordr)
    assert isinstance(ordr.exchange_order_id, int)
    assert ordr.exchange_order_id % 10 == 1

    order_df = paper_ex.open_orders_df
    num = order_df.index[0]
    exchange_order = tw.Order(1001, 'orig_id', '123-456', 'stat_id', 'stock', order_df.get(num, 'symbol'),
                              order_df.get(num, 'buy_sell'), order_df.get(num, 'quantity'),
                              order_df.get(num, 'order_type'), price=order_df.get(num, 'price'))

    assert_orders_equal(ordr, exchange_order, check_id=False, check_fills_df=False)


def test_send_order():
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)

    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    # send the order and verify the order got the exchange and the state changed
    bro.send_order(order)
    exch_orders = exchange.open_orders_df
    assert order.broker_order_id % 1000 == 100
    assert order.exchange_order_id == exch_orders.index[0]
    assert order.state == 'SENT'

    # test that sending the order not in RISK_ACCEPTED fails
    with pytest.raises(RuntimeError):
        bro.send_order(order)


def test_send_cancel():
    mdm = datalib.MarketDataManager(None, lmdm)
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    # Cancel the order before it gets to the exchange
    om.change_state(order, 'CANCEL_REQUESTED')
    bro.send_cancel_requested()
    assert order.state == 'CANCELED'
    assert om.orders_list({'state': 'CANCELED'}) == [order]

    # send the order and verify the order got the exchange and the state changed
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'test.sym.3', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    bro.send_order(order)
    exch_orders = exchange.open_orders_df
    assert order.broker_order_id % 1000 == 100
    assert order.exchange_order_id == exch_orders.index[0]
    assert order.state == 'SENT'

    # request cancel and send requests
    om.change_state(order, 'CANCEL_REQUESTED')
    bro.send_cancel_requested()
    assert order.state == 'CANCEL_SENT'
    assert om.orders_list({'state': 'CANCEL_SENT'}) == [order]
    assert bro.get_exchange_order(order)['state'] == 'CANCEL_SENT'

    # process the bar to get the order canceled
    mdm.add_symbols('stock', 'test.sym.3', '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')
    exchange.process_orders(mdm)
    bro.update_order_states()

    assert order.state == 'CANCELED'
    assert len(om.closed_orders_df()) == 2
    assert bro.get_exchange_order(order)['state'] == 'CANCELED'


def test_send_replace():
    mdm = datalib.MarketDataManager(None, lmdm)
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    # send the order and verify the order got the exchange and the state changed
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'test.sym.3', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    bro.send_order(order)
    exch_orders = exchange.open_orders_df
    assert order.broker_order_id % 1000 == 100
    assert order.exchange_order_id == exch_orders.index[0]
    assert order.state == 'SENT'

    # request replace and send requests
    om.replace_order(order, 75, price=51.0)
    assert order.state == 'REPLACE_REQUESTED'
    bro.send_replace_requested()
    assert order.state == 'REPLACE_SENT'
    assert om.orders_list({'state': 'REPLACE_SENT'}) == [order]
    assert bro.get_exchange_order(order)['state'] == 'REPLACE_SENT'

    # process the bar to get the order replaced
    mdm.add_symbols('stock', 'test.sym.3', '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')
    exchange.process_orders(mdm)
    bro.update_order_states()
    assert len(om.open_orders_df()) == 2

    exchange_order = bro.get_exchange_order(order)
    assert order.state == 'LIVE'
    assert exchange_order['state'] == 'LIVE'

    assert order.quantity == 75
    assert exchange_order['quantity'] == 75

    assert order.details['price'] == 51.0
    assert exchange_order['price'] == 51.0

    # now partially fill the order
    mdm.bartime = '2010-01-01 09:32:00'
    exchange.fill_order(exchange_order, 50, pd.Timestamp('2010-01-01 09:32:00', tz='America/New_York'))
    bro.update_order_states()

    # now make the replace quantity less than already filled
    om.replace_order(order, 10)
    bro.send_replace_requested()
    exchange.process_orders(mdm)
    bro.update_order_states()

    assert order.state == 'FILLED'
    assert order.closed is True
    assert order.fill_quantity == 50


def test_send_replace_failure():
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)

    # Replace the order before it gets to the exchange raises error
    om.replace_order(order, 100, price=15)
    with pytest.raises(RuntimeError):
        bro.send_replace_requested()


def test_send_orders():
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)

    # populate the order manager with a couple of orders in the RISK_ACCEPTED state
    order1 = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order1.state = 'RISK_ACCEPTED'
    om.new_order(order1)

    order2 = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'sell', 5, 'LIMIT', price=51)
    order2.state = 'RISK_ACCEPTED'
    om.new_order(order2)

    # send the orders via broker and confirm the broker and exchange ID as test of success. Confirm send sequence
    bro.send_orders()
    exch_orders = exchange.open_orders_df

    assert isinstance(order1.broker_order_id, int)
    assert order1.exchange_order_id == exch_orders.index[0]
    assert order1.state == 'SENT'

    assert isinstance(order2.broker_order_id, int)
    assert order2.exchange_order_id == exch_orders.index[1]
    assert order2.state == 'SENT'

    assert order1.broker_order_id < order2.broker_order_id
    assert order1.exchange_order_id < order2.exchange_order_id

    # now a replace request and confirm change in state
    om.replace_order(order1, 99, price=22)
    bro.send_orders()
    exch_orders = exchange.open_orders_df

    assert order1.state == 'REPLACE_SENT'
    assert exch_orders.get(order1.exchange_order_id, 'state') == 'REPLACE_SENT'

    # now request a cancel and confirm change in state
    om.change_state(order1, 'CANCEL_REQUESTED')
    bro.send_orders()
    exch_orders = exchange.open_orders_df

    assert order1.state == 'CANCEL_SENT'
    assert exch_orders.get(order1.exchange_order_id, 'state') == 'CANCEL_SENT'


def test_get_order_state():
    ordr = tw.Order(1001, 'orig_id', '123-456', 'stat_id', 'stock', 'TEST', 's', 250, 'LIMIT', price=15.15)
    paper_ex = tw.PaperExchange()
    oms = tw.OrderManager('unit_test', None)
    bro = broker.PaperBroker('broker_id', oms, paper_ex)

    bro.send_order_to_exchange(ordr)

    res = bro.get_exchange_order(ordr)
    assert res['state'] == 'LIVE'


def test_commission():
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)

    od = tw.Order(1001, 'orig_id', '123-456', 'test_orig', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=50.0)
    assert bro.commission(od, Exchange_Fill(None, None, 100, 50)) == -1.0

    od = tw.Order(1001, 'orig_id', '123-456', 'test_orig', 'stock', 'TEST', 'buy', 100, 'LIMIT', price=50.0)
    assert bro.commission(od, Exchange_Fill(None, None, 75, 50)) == -0.75

    # will fail for any product_type other than stock
    od = tw.Order(1001, 'orig_id', '123-456', 'test_orig', 'future', 'TEESTH8', 'buy', 100, 'LIMIT', price=50.0)
    with pytest.raises(ValueError):
        bro.commission(od, Exchange_Fill(None, None, 75, 50))


def test_process_fills():
    # setup broker, exchange and order manager
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    bro = tw.PaperBroker('test_broker', om, exchange)

    # send an order to the exchange via the broker
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 400, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)
    bro.send_order(order)

    # Do multiple fills on one bar, leave in PARTIALLY_FILLED state
    exchange_order = bro.get_exchange_order(order)
    exchange.fill_order(exchange_order, 100, pd.Timestamp('2000-01-01 09:45:00', tz='America/New_York'))
    exchange.fill_order(exchange_order, 200, pd.Timestamp('2000-01-01 09:45:00', tz='America/New_York'))
    om.change_state(order, 'PARTIALLY_FILLED')
    assert exchange_order['state'] == 'PARTIALLY_FILLED'
    assert order.state == 'PARTIALLY_FILLED'
    assert order.booked is None
    assert exchange_order['fill_quantity'] == 300

    # process the fills on the order
    bro.process_fills(order, exchange_order)
    assert order.booked is False
    assert order.closed is False
    assert order.fill_price == 15
    assert order.fill_quantity == 300

    expected = rc.DataFrame({'bartime': [pd.Timestamp('2000-01-01 09:45:00', tz=NYC).tz_convert('UTC')] * 2,
                             'quantity': [100, 200],
                             'price': [15, 15], 'commission': [-1.0, -2.0], 'booked': [False, False]},
                            columns=['bartime', 'quantity', 'price', 'commission', 'booked'],
                            index=[exchange_order['fills'][0].id, exchange_order['fills'][1].id])
    actual = order.fills[['bartime', 'quantity', 'price', 'commission', 'booked']]
    assert_frame_equal(actual, expected)


def test_update_order_state():
    # setup broker, exchange and order manager
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    brkr = tw.PaperBroker('test_broker', om, exchange)

    # send an order to the exchange via the broker
    order = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order.state = 'RISK_ACCEPTED'
    om.new_order(order)
    brkr.send_order(order)

    assert order.state == 'SENT'

    # get the order state
    brkr.update_order_state(order)
    assert order.state == 'LIVE'
    assert order.closed is False
    assert order.booked is None

    # partially fill the order
    exch_order = exchange.open_orders_list[0]
    exchange.fill_order(exch_order, fill_quantity=30, timestamp=pd.Timestamp('2010-05-05 12:00:00', tz=NYC))
    brkr.update_order_state(order)
    assert order.state == 'PARTIALLY_FILLED'
    assert order.fill_price == 15
    assert order.fill_quantity == 30
    assert order.commission == -0.30
    assert order.booked is False
    assert order.closed is False

    # fill the order on the exchange and get the state
    exch_order = exchange.open_orders_list[0]
    exchange.fill_order(exch_order, fill_quantity=10, timestamp=pd.Timestamp('2010-05-05 12:05:00', tz=NYC))
    brkr.update_order_state(order)

    assert order.state == 'FILLED'
    assert order.fill_price == order.details['price']
    assert order.fill_quantity == order.quantity
    assert order.commission == -0.40
    assert order.closed is False
    assert order.booked is False


def test_update_order_states():
    exchange = tw.PaperExchange()
    om = tw.OrderManager('unit_test', None)
    brkr = tw.PaperBroker('test_broker', om, exchange)

    # populate the order manager with a couple of orders in the RISK_ACCEPTED state
    order1 = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'buy', 40, 'LIMIT', price=15)
    order1.state = 'RISK_ACCEPTED'
    om.new_order(order1)

    order2 = tw.Order(1001, 'orig_id', '123-456', 'stat', 'stock', 'TEST', 'sell', 5, 'LIMIT', price=51)
    order2.state = 'RISK_ACCEPTED'
    om.new_order(order2)

    # send order to broker and confirm they are in the SENT state
    brkr.send_orders()
    assert sum(om.open_orders_df().equality('state', value='SENT')) == 2

    # update the orders and confirm they are now LIVE
    brkr.update_order_states()
    assert sum(om.open_orders_df().equality('state', value='LIVE')) == 2

    # fill one of the orders and confirm that it is moved to FILLED
    exch_order = exchange.open_orders_list[0]
    exchange.fill_order(exch_order, fill_quantity=40, timestamp=pd.Timestamp('2010-05-05 12:00:00', tz=NYC))

    brkr.update_order_states()
    assert len(om.open_orders_df()) == 1
    assert sum(om.open_orders_df().equality('state', value='LIVE')) == 1
    assert om.open_orders_df()[0, 'exchange_order_id'] == order2.exchange_order_id

    assert_orders_equal(om.orders_list({'state': 'FILLED'})[0], order1)
    assert len(om.closed_orders_df()) == 1
    assert sum(om.closed_orders_df().equality('state', value='FILLED')) == 1
    assert om.closed_orders_df()[0, 'broker_order_id'] == order1.broker_order_id

    # replace the second order
    om.replace_order(order2, 50, price=77.7)
    brkr.send_replace_requested()
    exch_order = exchange.open_orders_list[0]
    exchange.replace_order(exch_order, '2010-05-05 12:00:00')

    brkr.update_order_states()
    assert len(om.open_orders_df()) == 1
    assert sum(om.open_orders_df().equality('state', value='LIVE')) == 1
    assert om.open_orders_df()[0, 'exchange_order_id'] == order2.exchange_order_id

    # cancel the second order
    om.change_state(order2, 'CANCEL_REQUESTED')
    brkr.send_cancel_requested()
    exch_order = exchange.open_orders_list[0]
    exchange.cancel_order(exch_order, '2010-05-05 12:00:00')

    brkr.update_order_states()
    assert len(om.closed_orders_df()) == 2
    assert sum(om.closed_orders_df().equality('state', value='CANCELED')) == 1
    assert om.closed_orders_df()[1, 'exchange_order_id'] == order2.exchange_order_id
