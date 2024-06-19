"""
Order class
"""

import functools
import logging
import uuid

import pandas as pd
import raccoon as rc

import utils.collections as cutils

log = logging.getLogger(__name__)


@functools.lru_cache(4)
def states():
    """
    This return dict defines the states an Order object can take. To add a new allowable state for the object
    include in the appropriate list. The order of the list represents that transitions, states can only migrate from
    the left to right in the list.

    :return: dict of list of states
    """
    return {'open': ['CREATED', 'STAGED', 'RISK_ACCEPTED', 'SENT', 'LIVE', 'CANCEL_REQUESTED', 'CANCEL_SENT',
                     'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT', 'PARTIALLY_FILLED'],
            'closed': ['RISK_REJECTED', 'REJECTED', 'FILLED', 'CANCELED']}


@functools.lru_cache(4)
def state_group():
    """
    Get the mapping of order states to an order state group (open, closed, etc.)

    :return: Dictionary of states as key and order group as value
    """
    res = {}
    for group, states_list in states().items():
        for state in states_list:
            res[state] = group
    return res


@functools.lru_cache(4)
def allowable_states():
    """
    Returns a list of all allowable states for the Order object.

    :return: list of allowable states
    """
    return cutils.flatten_list(list(states().values()))


@functools.lru_cache()
def allowable_transitions(state):
    """
    For a given state, return a list of allowable transitions from that state.

    :return: list of states
    """
    states_dict = states()
    current_state_index = states_dict['open'].index(state)
    ok_states = states_dict['closed'].copy()

    if current_state_index <= states_dict['open'].index('CANCEL_REQUESTED'):
        state_index = states_dict['open'].index(state)
        ok_states.extend(states_dict['open'][(state_index + 1):])
    elif state in ['CANCEL_REQUESTED', 'CANCEL_SENT', 'PARTIALLY_FILLED']:
        ok_states.extend(['CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT',
                          'PARTIALLY_FILLED'])
    elif state == 'REPLACE_REQUESTED':
        ok_states.extend(['CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT',
                          'PARTIALLY_FILLED', 'LIVE'])
    elif state == 'REPLACE_SENT':
        ok_states.extend(['CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT',
                          'PARTIALLY_FILLED', 'LIVE'])
    elif state == 'REPLACE_REJECTED':
        ok_states.extend(['CANCEL_REQUESTED', 'CANCEL_SENT', 'REPLACE_REQUESTED', 'REPLACE_REJECTED', 'REPLACE_SENT',
                          'PARTIALLY_FILLED', 'LIVE'])
    else:
        raise ValueError('requested state does not have any transitions.')
    return ok_states


def list_to_dict(orders, key):
    """
    Converts a list of Orders to a dictionary of lists of the Orders where the keys of dict are key parameter.

    :param orders: list of Orders
    :param key: name of the Order attribute to use for the dictionary key
    :return: dictionary of lists of Orders
    """
    order_dict = {}
    for order in orders:
        key_value = getattr(order, key)
        if key_value not in order_dict:
            order_dict[key_value] = []
        order_dict[key_value].append(order)
    return order_dict


class Order:
    """
    Order object that contains all the information about an order and the associated methods
    """

    __slots__ = ('_uuid', '_create_timestamp', '_event_type', '_originator_uuid', '_originator_id', '_strategy_uuid',
                 '_strategy_id', '_product_type', '_symbol', '_buy_sell', '_quantity', '_details', '_state_df',
                 '_state', '_closed', '_replaces_df', '_portfolio_uuid', '_portfolio_id', '_broker_order_id',
                 '_exchange_order_id', '_fill_price', '_fill_quantity', '_fills_df', '_commission', '_booked', '_type')

    def __init__(self, originator_uuid, originator_id, strategy_uuid, strategy_id, product_type, symbol, buy_sell,
                 quantity, order_type, **kwargs):
        """
        :param originator_uuid: the UUID of the originating object
        :param originator_id: string ID of the ordinating object
        :param strategy_uuid: the UUID of the strategy
        :param strategy_id: string ID of the strategy
        :param product_type: product type
        :param symbol: symbol name
        :param buy_sell: buy, sell, b or s
        :param quantity: full quantity
        :param order_type: order type. Currently only LIMIT is allowed
        :param kwargs: arguments specific for the order type.
        """
        self._uuid = str(uuid.uuid4())
        self._create_timestamp = pd.Timestamp.now(tz='UTC')

        self._event_type = 'ORDER'
        self._originator_uuid = originator_uuid
        self._originator_id = originator_id
        self._strategy_uuid = strategy_uuid
        self._strategy_id = strategy_id
        self._product_type = product_type
        self._symbol = symbol
        self._type = None

        if buy_sell.lower() in ['buy', 'b']:
            self._buy_sell = 'buy'
        elif buy_sell.lower() in ['sell', 's']:
            self._buy_sell = 'sell'
        else:
            raise ValueError("buy_sell value must be in ['buy', 'sell', 'b', 's']")

        self._quantity = quantity
        self._set_order_type(order_type)
        self._details = {}
        self._set_order_details(order_type, **kwargs)
        self._state_df = rc.DataFrame(columns=['timestamp', 'state'], sort=True)
        self._state = None
        self.state = 'CREATED'
        self._closed = False
        self._replaces_df = rc.DataFrame({'quantity': quantity, 'details': kwargs}, columns=['quantity', 'details'])

        self._portfolio_uuid = None
        self._portfolio_id = None
        self._broker_order_id = None
        self._exchange_order_id = None
        self._fill_price = None
        self._fill_quantity = None
        self._fills_df = rc.DataFrame(columns=['timestamp', 'bartime', 'quantity', 'price', 'commission', 'booked'],
                                      sort=False)
        self._commission = None
        self._booked = None

    @property
    def uuid(self):
        return self._uuid

    @property
    def create_timestamp(self):
        return self._create_timestamp

    @property
    def event_type(self):
        return self._event_type

    @property
    def originator_uuid(self):
        return self._originator_uuid

    @property
    def originator_id(self):
        return self._originator_id

    @property
    def strategy_uuid(self):
        return self._strategy_uuid

    @property
    def strategy_id(self):
        return self._strategy_id

    @property
    def product_type(self):
        return self._product_type

    @property
    def symbol(self):
        return self._symbol

    @property
    def buy_sell(self):
        return self._buy_sell

    @property
    def quantity(self):
        return self._quantity

    @property
    def portfolio_uuid(self):
        return self._portfolio_uuid

    @portfolio_uuid.setter
    def portfolio_uuid(self, uuid_value):
        if self._portfolio_uuid:
            raise AttributeError(f'portfolio_uuid is already set to {self._portfolio_uuid} cannot set to new value')
        else:
            self._portfolio_uuid = uuid_value

    @property
    def portfolio_id(self):
        return self._portfolio_id

    @portfolio_id.setter
    def portfolio_id(self, id_str):
        if self._portfolio_id:
            raise AttributeError(f'portfolio_id is already set to {self._portfolio_id} cannot set to new value')
        else:
            self._portfolio_id = id_str

    def add_fill(self, id, timestamp, bartime, quantity, price, commission):
        """
        Add a fill to the order. Can be a partial or complete fill.

        :param id: Fill ID
        :param timestamp: timestamp of the fills
        :param bartime: bartime of the fill
        :param quantity: fill quantity
        :param price: fill price
        :param commission: commission on the fill
        :return: nothing
        """
        # convert timestamps to UTC also ensures they have a timestamp
        timestamp = timestamp.tz_convert('UTC')
        bartime = bartime.tz_convert('UTC')

        self.log('add fill: {}, {}, {}, {}, {}, {}'.format(str(id), timestamp, bartime, str(quantity), str(price),
                                                           str(commission)))
        self._fills_df.set_row(id, {'timestamp': timestamp, 'bartime': bartime, 'quantity': quantity, 'price': price,
                                    'commission': commission, 'booked': False})
        if self._fill_price:
            self._fill_price = (self._fill_price * self._fill_quantity + quantity * price) / \
                               (self._fill_quantity + quantity)
        else:
            self._fill_price = price

        self._fill_quantity = (quantity + self._fill_quantity) if self._fill_quantity else quantity
        self._commission = (commission + self._commission) if self._commission else commission

    @property
    def fill_price(self):
        return self._fill_price

    @property
    def fill_quantity(self):
        return self._fill_quantity

    @property
    def commission(self):
        return self._commission

    @property
    def fills(self):
        return self._fills_df

    @property
    def booked(self):
        return self._booked

    @booked.setter
    def booked(self, status):
        self._booked = status

    @property
    def closed(self):
        return self._closed

    @closed.setter
    def closed(self, boolean):
        if not self._closed:
            self._closed = boolean
        else:
            raise AttributeError('cannot change state of closed trade.')

    @staticmethod
    def allowable_order_types():
        return ['LIMIT']

    def _set_order_type(self, order_type):
        """
        Set the order type

        :param order_type: order type
        :return: nothing
        """
        if order_type not in self.allowable_order_types():
            raise ValueError('Not a valid order type:', order_type)
        self._type = order_type

    def _set_order_details(self, order_type, **kwargs):
        """
        Populate the order.details with the kwargs from the object initialization

        :param order_type: order type
        :param kwargs: the kwargs from the object initialization
        :return:
        """
        if order_type == 'LIMIT':
            self._details['price'] = kwargs['price']

    @property
    def type(self):
        return self._type

    @property
    def details(self):
        return self._details

    @property
    def broker_order_id(self):
        return self._broker_order_id

    @broker_order_id.setter
    def broker_order_id(self, id):
        self._broker_order_id = id

    @property
    def exchange_order_id(self):
        return self._exchange_order_id

    @exchange_order_id.setter
    def exchange_order_id(self, id):
        self._exchange_order_id = id

    @property
    def state_df(self):
        return self._state_df

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        """
        Update and set the state value of the object. This will change the value of the state property and add a row
        to the state_df DataFrame with the timestamp of the change in the state value

        :param state: new state
        :return: nothing
        """
        states_dict = states()
        if self.state in states_dict['closed']:
            raise RuntimeError('Cannot change state of an order already in a closed state.')

        if state in allowable_states():
            # check that the transition is allowable
            if (state != 'CREATED') & (state not in states_dict['closed']):
                if state not in allowable_transitions(self.state):
                    raise AttributeError(f'State transition from {self._state} to {state} not allowed.')
            self._state = state
            self._state_df.set_row(len(self._state_df), {'timestamp': pd.Timestamp.now(tz='UTC'), 'state': state})
            self.log('new state')
        else:
            raise ValueError('Not a valid state:', state)

    @property
    def replaces(self):
        return self._replaces_df

    def replace(self, quantity=None, **kwargs):
        # if quantity is not given, use the prior replace request
        quantity = self._quantity if quantity is None else quantity
        self._replaces_df.set_row(len(self._replaces_df), {'quantity': quantity, 'details': kwargs})
        self._quantity = quantity
        if kwargs:
            self._set_order_details(self.type, **kwargs)
        self.log(f'replacing quantity: {quantity} | details: {kwargs}')

    def log(self, text=None):
        """
        Helper function to output to log. Will output the identifying items for the object with an optional text

        :param text: None or a text string to prepend to the log message
        :return: nothing
        """
        message = [text] if text is not None else []
        message.extend([self.uuid.__str__(), self.state, str(self.originator_id), str(self.strategy_id), self.symbol,
                        self.buy_sell, str(self.quantity), self.type, str(self._details)])
        log.info(' : '.join(message))

    def __repr__(self):
        return f"puma.order.Order : UUID={self.uuid}"

    def print(self):
        """
        Pretty print the object

        :return: nothing
        """
        print('uuid         :', self.uuid)
        print('state        :', self.state)
        print('originator_id:', self.originator_id)
        print('strategy_id  :', self.strategy_id)
        print('symbol       :', self.symbol)
        print('buy/sell     :', self.buy_sell)
        print('quantity     :', self.quantity)
        print('order_type   :', self.type)
        print('details      :', self.details)
        print('broker_id    :', self.broker_order_id)
        print('exchange_id  :', self.exchange_order_id)
        print('fill_price   :', self.fill_price)
        print('fill_qty     :', self.fill_quantity)
        print('commission   :', self.commission)
        print('booked       :', self.booked)
        print('closed       :', self.closed)
        print('\nstate history:')
        print(self._state_df)
        print('\nreplacements :')
        print(self._replaces_df)
        print('\nfills        :')
        print(self.fills)

    def to_dict(self):
        """
        Returns the attributes of the object as a dict

        :return: dict of the object attributes
        """
        res = {}
        for key in self.__slots__:
            if key not in ['_state_df', '_fills_df', '_replaces_df']:
                value = self.__getattribute__(key)
                res[key.lstrip('_')] = value

        for x in range(len(self._state_df)):
            res[self._state_df.get_cell(x, 'state')] = self._state_df.get_cell(x, 'timestamp')
        return res
