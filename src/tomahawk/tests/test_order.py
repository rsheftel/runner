"""
unit tests for Order() class
"""

from unittest import mock

import pandas as pd
import pytest
import raccoon as rc
from config.datetime import NYC
from montauk.tomahawk import order
from raccoon.utils import assert_frame_equal


def test_states():
    assert set(order.states().keys()) == {'closed', 'open'}
    assert set(order.allowable_states()) == {'CREATED', 'STAGED', 'RISK_ACCEPTED', 'SENT', 'LIVE', 'RISK_REJECTED',
                                             'REJECTED', 'FILLED', 'CANCELED', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                             'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT',
                                             'PARTIALLY_FILLED'}


def test_state_group():
    assert order.state_group() == {'CREATED': 'open',
                                   'STAGED': 'open',
                                   'RISK_ACCEPTED': 'open',
                                   'SENT': 'open',
                                   'LIVE': "open",
                                   'CANCEL_REQUESTED': 'open',
                                   'CANCEL_SENT': 'open',
                                   'REPLACE_REQUESTED': 'open',
                                   'REPLACE_REJECTED': 'open',
                                   'REPLACE_SENT': 'open',
                                   'PARTIALLY_FILLED': 'open',
                                   'RISK_REJECTED': 'closed',
                                   'REJECTED': 'closed',
                                   'FILLED': 'closed',
                                   'CANCELED': 'closed'}


def test_allowable_transitions():
    assert set(order.allowable_transitions('CREATED')) == {'STAGED', 'RISK_ACCEPTED', 'SENT', 'LIVE', 'RISK_REJECTED',
                                                           'REJECTED', 'FILLED', 'CANCELED', 'CANCEL_REQUESTED',
                                                           'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED',
                                                           'REPLACE_SENT', 'PARTIALLY_FILLED'}

    assert set(order.allowable_transitions('STAGED')) == {'RISK_ACCEPTED', 'SENT', 'LIVE', 'RISK_REJECTED',
                                                          'REJECTED', 'FILLED', 'CANCELED', 'CANCEL_REQUESTED',
                                                          'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED',
                                                          'REPLACE_SENT', 'PARTIALLY_FILLED'}

    assert set(order.allowable_transitions('SENT')) == {'LIVE', 'RISK_REJECTED',
                                                        'REJECTED', 'FILLED', 'CANCELED', 'CANCEL_REQUESTED',
                                                        'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED',
                                                        'REPLACE_SENT', 'PARTIALLY_FILLED'}

    assert set(order.allowable_transitions('CANCEL_REQUESTED')) == {'RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED',
                                                                    'CANCEL_SENT', 'REPLACE_REQUESTED',
                                                                    'REPLACE_REJECTED', 'REPLACE_SENT',
                                                                    'PARTIALLY_FILLED'}

    assert set(order.allowable_transitions('CANCEL_SENT')) == {'RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED',
                                                               'CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED',
                                                               'REPLACE_REJECTED', 'REPLACE_SENT', 'PARTIALLY_FILLED'}

    assert set(order.allowable_transitions('REPLACE_REQUESTED')) == {'RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED',
                                                                     'CANCEL_REQUESTED', 'CANCEL_SENT',
                                                                     'REPLACE_REQUESTED', 'REPLACE_REJECTED',
                                                                     'REPLACE_SENT', 'PARTIALLY_FILLED', 'LIVE'}

    assert set(order.allowable_transitions('REPLACE_SENT')) == {'RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED',
                                                                'CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED',
                                                                'REPLACE_REJECTED', 'REPLACE_SENT', 'PARTIALLY_FILLED',
                                                                'LIVE'}

    assert set(order.allowable_transitions('PARTIALLY_FILLED')) == {'RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED',
                                                                    'CANCEL_REQUESTED', 'CANCEL_SENT',
                                                                    'REPLACE_REQUESTED', 'REPLACE_REJECTED',
                                                                    'REPLACE_SENT', 'PARTIALLY_FILLED'}


def test_initialize():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    assert od.event_type == 'ORDER'
    assert isinstance(od.create_timestamp, pd.Timestamp)
    assert od.originator_uuid == 1001
    assert od.originator_id == 'orig_1'
    assert od.strategy_uuid == 123
    assert od.strategy_id == 'test_id'
    assert od.symbol == 'TEST'
    assert od.buy_sell == 'buy'
    assert od.quantity == 1000
    assert od.type == 'LIMIT'
    assert od.state == 'CREATED'
    assert od.closed is False
    assert od.fill_price is None
    assert od.fill_quantity is None
    assert od.commission is None
    assert od.booked is None
    assert od.portfolio_id is None
    assert od.portfolio_uuid is None

    df = od.state_df
    assert isinstance(df, rc.DataFrame)
    assert df['state'].to_list() == ['CREATED']

    df = od.fills
    assert isinstance(df, rc.DataFrame)
    assert len(df) == 0

    expected = rc.DataFrame({'quantity': 1000, 'details': {'price': 99.99}}, columns=['quantity', 'details'])
    actual = od.replaces
    assert_frame_equal(actual, expected)

    with pytest.raises(ValueError):
        order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'BAD_DIR', 1000, 'LIMIT', price=99.99)

    with mock.patch('montauk.tomahawk.Order.allowable_order_types', new=mock.Mock(return_value=['LIMIT', 'MARKET'])):
        od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'MARKET')
    assert od.type == 'MARKET'
    assert od.details == {}


# noinspection PyPropertyAccess
def test_immutable_attributes():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'buy', 1000, 'LIMIT', price=99.99)
    with pytest.raises(AttributeError):
        od.originator_id = 'new_id'
    with pytest.raises(AttributeError):
        od.symbol = 'NEWSYM'
    with pytest.raises(AttributeError):
        od.buy_sell = 'sell'
    with pytest.raises(AttributeError):
        od.quantity = 10
    with pytest.raises(AttributeError):
        od.type = 'MKT'
    with pytest.raises(AttributeError):
        od.details = 'new details'
    with pytest.raises(AttributeError):
        od.fill_price = 9
    with pytest.raises(AttributeError):
        od.fill_quantity = 100
    with pytest.raises(AttributeError):
        od.commission = 0.10


def test_add_fill():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    assert od.fill_price is None
    assert od.fill_quantity is None
    assert od.commission is None
    assert len(od.fills) == 0

    od.add_fill(123, pd.Timestamp('2010-01-01 09:30:01', tz=NYC), pd.Timestamp('2010-01-01 09:30:00', tz=NYC),
                50, 100, 0.50)

    assert od.fill_price == 100
    assert od.fill_quantity == 50
    assert od.commission == 0.50
    expected = rc.DataFrame({'timestamp': pd.Timestamp('2010-01-01 09:30:01', tz=NYC).tz_convert('UTC'),
                             'bartime': pd.Timestamp('2010-01-01 09:30:00', tz=NYC).tz_convert('UTC'), 'quantity': 50,
                             'price': 100, 'commission': 0.50, 'booked': False}, sort=False,
                            columns=['timestamp', 'bartime', 'quantity', 'price', 'commission', 'booked'], index=[123])
    assert_frame_equal(od.fills, expected)

    od.add_fill(124, pd.Timestamp('2010-01-01 09:30:03', tz=NYC), pd.Timestamp('2010-01-01 09:31:00', tz=NYC),
                200, 50, 2.00)

    assert od.fill_price == (200 * 50 + 50 * 100) / 250
    assert od.fill_quantity == 250
    assert od.commission == 2.50
    expected.set_row(124, {'timestamp': pd.Timestamp('2010-01-01 09:30:03', tz=NYC).tz_convert('UTC'),
                           'bartime': pd.Timestamp('2010-01-01 09:31:00', tz=NYC), 'quantity': 200,
                           'price': 50, 'commission': 2.00, 'booked': False})
    assert_frame_equal(od.fills, expected)


def test_replace_order():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    assert_frame_equal(od.replaces, rc.DataFrame({'quantity': 1000, 'details': {'price': 99.99}},
                                                 columns=['quantity', 'details']))

    # replace with quantity and details
    od.replace(500, price=50.5)
    expected = rc.DataFrame({'quantity': [1000, 500], 'details': [{'price': 99.99}, {'price': 50.5}]},
                            columns=['quantity', 'details'])
    assert_frame_equal(od.replaces, expected)
    assert od.quantity == 500
    assert od.details['price'] == 50.5

    # replace with quantity no details
    od.replace(444)
    expected = rc.DataFrame({'quantity': [1000, 500, 444], 'details': [{'price': 99.99}, {'price': 50.5}, {}]},
                            columns=['quantity', 'details'])
    assert_frame_equal(od.replaces, expected)
    assert od.quantity == 444
    assert od.details['price'] == 50.5

    # replace with details, no quantity
    od.replace(price=33.3)
    expected = rc.DataFrame({'quantity': [1000, 500, 444, 444],
                             'details': [{'price': 99.99}, {'price': 50.5}, {}, {'price': 33.3}]},
                            columns=['quantity', 'details'])
    assert_frame_equal(od.replaces, expected)
    assert od.quantity == 444
    assert od.details['price'] == 33.3


def test_order_type():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    assert od.type == 'LIMIT'
    assert od.details == {'price': 99.99}

    assert od.allowable_order_types() == ['LIMIT']

    with pytest.raises(ValueError):
        order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'MARKET')


def test_change_state():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    assert od.state_df['state'].to_list() == ['CREATED']

    # cannot set to bad state name
    with pytest.raises(ValueError):
        od.state = 'BAD'

    od.state = 'STAGED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED']

    od.state = 'LIVE'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE']

    # State transition cannot go backwards if it is past CANCEL_REQUESTED
    with pytest.raises(AttributeError):
        od.state = 'STAGED'

    od.state = 'CANCEL_REQUESTED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED']

    od.state = 'CANCEL_SENT'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT']

    # But PARTIALLY_FILLED, CANCEL and REPLACE items can rotate
    od.state = 'PARTIALLY_FILLED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED']

    od.state = 'REPLACE_REQUESTED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED']

    od.state = 'REPLACE_SENT'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT']

    od.state = 'CANCEL_REQUESTED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT',
                                              'CANCEL_REQUESTED']

    od.state = 'CANCEL_SENT'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT',
                                              'CANCEL_REQUESTED', 'CANCEL_SENT']

    # But cannot go back to LIVE or earlier
    with pytest.raises(AttributeError):
        od.state = 'LIVE'

    # a REPLACE_REQUESTED can be pushed back to LIVE
    od.state = 'REPLACE_REQUESTED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT',
                                              'CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED']

    od.state = 'LIVE'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT',
                                              'CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'LIVE']

    # can move to closed state
    od.state = 'FILLED'
    assert od.state_df['state'].to_list() == ['CREATED', 'STAGED', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                                              'PARTIALLY_FILLED', 'REPLACE_REQUESTED', 'REPLACE_SENT',
                                              'CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'LIVE',
                                              'FILLED']

    # Once an order is in a closed state, the state cannot change
    with pytest.raises(RuntimeError):
        od.state = 'CANCELED'


def test_closed():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    assert od.closed is False
    od.closed = True
    assert od.closed is True
    with pytest.raises(AttributeError):
        od.closed = False


def test_to_dict():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    actual = od.to_dict()
    assert '_fills_df' not in actual.keys()
    assert '_state_df' not in actual.keys()
    assert '_replaces_df' not in actual.keys()
    subset = {k: v for k, v in actual.items() if k in ['details', 'quantity', 'state']}
    assert subset == {'details': {'price': 99.99}, 'quantity': 1000, 'state': 'CREATED'}


def test_add_portfolio():
    od = order.Order(1001, 'orig_1', 123, 'test_id', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    assert od.portfolio_id is None
    assert od.portfolio_uuid is None

    od.portfolio_uuid = '123-456'
    od.portfolio_id = 'test_port'

    assert od.portfolio_uuid == '123-456'
    assert od.portfolio_id == 'test_port'

    with pytest.raises(AttributeError):
        od.portfolio_id = 'cannot_reset'

    with pytest.raises(AttributeError):
        od.portfolio_uuid = 'cannot_reset'


def test_list_to_dict():
    od1 = order.Order(1001, 'orig_1', 123, 'id1', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    od2 = order.Order(1001, 'orig_1', 123, 'id2', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    od3 = order.Order(1001, 'orig_3', 123, 'id3', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    od4 = order.Order(1001, 'orig_2', 123, 'id1', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    od5 = order.Order(1001, 'orig_2', 123, 'id2', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)

    expected = {'id1': [od1, od4], 'id2': [od2, od5], 'id3': [od3]}
    actual = order.list_to_dict([od1, od2, od3, od4, od5], 'strategy_id')
    assert actual == expected

    expected = {'orig_1': [od1, od2], 'orig_2': [od4, od5], 'orig_3': [od3]}
    actual = order.list_to_dict([od1, od2, od3, od4, od5], 'originator_id')
    assert actual == expected


def test_print():
    od = order.Order(1001, 'orig_1', 123, 'id1', 'stock', 'TEST', 'B', 1000, 'LIMIT', price=99.99)
    od.print()
