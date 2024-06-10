"""
Strategy class
"""

import logging
import types
import uuid
from abc import ABCMeta

import pandas as pd

from montauk.data.structures import SymbolTuple
from montauk.tomahawk.order import Order

log = logging.getLogger(__name__)


class Strategy(metaclass=ABCMeta):
    """
    Abstract base class for strategies. In concrete implementations, only overwrite the on_* methods. The rest should
    be considered protected and the strategy may not run properly if they are overwritten.
    """

    def __init__(self, strategy_id, object_bridge):
        """
        Abstract initialization method. This method at the end calls initialize() which the concrete implementations
        can override. The object_bridge is a data structure or object that will return a pointer to the other objects
        in the ecosystem like OrderManager when called with "." dot notation. For example a namedtuple or a Runner
        object.

        :param strategy_id: unique strategy id as a string
        :param object_bridge: data structure or object which will return ecosystem objects
        """
        self._strategy_id = strategy_id
        self.__uuid = str(uuid.uuid4())
        log.info(f'Strategy initialized: {strategy_id} : {self.uuid}')

        # attached ecosystem objects
        self._portfolio = None  # Portfolio gets attached when the Portfolio add_strategy() is called
        self._order_manager = None
        self._attach_object('order_manager', object_bridge)
        self._position_manager = None
        self._attach_object('position_manager', object_bridge)
        self._market_data_manager = None
        self._attach_object('market_data_manager', object_bridge)

        # Initialize private variable
        self._started = False
        self._symbol_tuples = []
        self._symbols = {}
        self._product_types = set()
        self._frequencies = set()
        self._parameters = None

        # call the initialize method to run the concrete implementation
        self.on_initialize()

    def __repr__(self):
        return '{} at {}: {}'.format(self.__class__, hex(id(self)), self.strategy_id)

    def _attach_object(self, object_name, object_bridge):
        """
        Generic method that will attach and object from the object_bridge to strategy

        :param object_name: string name of the object
        :param object_bridge: The object_bridge is a data structure or object that will return a pointer to the other
        objects in the ecosystem like OrderManager when called with "." dot notation. For example a namedtuple or a
        Runner object.
        :return: nothing
        """
        obj = getattr(object_bridge, object_name, None)
        log.info(f'attaching to {object_name}: {obj}')
        setattr(self, '_' + object_name, obj)

    def attach_portfolio(self, portfolio):
        """
        Attach a Portfolio to the Strategy. This is called from the Portfolio object.

        :param portfolio: Portfolio object
        :return: nothing
        """
        log.info(f'attaching to portfolio: {portfolio}')
        self._portfolio = portfolio

    @property
    def portfolio(self):
        return self._portfolio

    @property
    def position_manager(self):
        return self._position_manager

    @property
    def order_manager(self):
        return self._order_manager

    @property
    def market_data_manager(self):
        return self._market_data_manager

    @property
    def uuid(self):
        return self.__uuid

    @property
    def strategy_id(self):
        return self._strategy_id

    def add_symbol(self, product_type, symbol_name, frequency):
        """
        Adds a symbol to the strategy.

        :param product_type: product type
        :param symbol_name: symbol name
        :param frequency: frequency in standard form
        :return: nothing
        """
        if not self._started:
            log.info(f'adding symbol to strategy {self.strategy_id} : {product_type} {symbol_name} {frequency}')
            self.market_data_manager.add_symbols(product_type, symbol_name, frequency)
            self._symbol_tuples.append(SymbolTuple(product_type, symbol_name, frequency))
            self._product_types.add(product_type)
            self._frequencies.add(frequency)
            if product_type not in self._symbols:
                self._symbols[product_type] = set()
            self._symbols[product_type].add(symbol_name)
        else:
            raise RuntimeError("Cannot add symbol after invoking start() method.")

    def add_symbols(self, symbol_tuples):
        """
        Add a list of symbol tuples to the strategy

        :param symbol_tuples: list of tuples of form (product_type, symbol, frequency
        :return: nothing
        """
        for product_type, symbol_name, frequency in symbol_tuples:
            self.add_symbol(product_type, symbol_name, frequency)

    @property
    def symbol_tuples(self):
        return self._symbol_tuples

    @property
    def symbols(self):
        return self._symbols

    @property
    def frequencies(self):
        return self._frequencies

    @property
    def product_types(self):
        return self._product_types

    def set_parameters(self, parameters):
        """
        Set the strategy parameters

        :param parameters: dict of parameters
        :return: nothing
        """
        log.info('setting parameters')
        if self._started:
            raise RuntimeError('Cannot set parameter after the strategy is running')
        if not isinstance(parameters, dict):
            raise AttributeError('parameters must be a dictionary')
        self._parameters = types.MappingProxyType(parameters)

    @property
    def parameters(self):
        return self._parameters

    def order(self, product_type, symbol, buy_sell, quantity, order_type, price):
        """
        Create an order in the OrderManager that will later be picked up by the Portfolio

        :param product_type: product_type
        :param symbol: symbol
        :param buy_sell: buy, sell, b or s
        :param quantity: full quantity
        :param order_type: order type
        :param price: price
        :return: the Order UUID
        """
        if product_type not in self.symbols:
            raise RuntimeError(f'Cannot create order for product_type not added to strategy: {product_type}')
        if symbol not in self.symbols[product_type]:
            raise RuntimeError(f'Cannot create order for symbol not added to strategy : {product_type} : {symbol}')

        log.info(f'creating order : {symbol} : {buy_sell} : {quantity} : {order_type} : {price}')
        order = Order(self.uuid, 'strategy.' + self.strategy_id, self.uuid, self.strategy_id, product_type, symbol,
                      buy_sell, quantity, order_type, price=price)
        self.order_manager.new_order(order)
        return order.uuid

    def get_order(self, order_uuid):
        """
        Get order object from uuid

        :param order_uuid: Order uuid
        :return: Order object
        """
        return self._order_manager.order(order_uuid)

    def cancel_order(self, order):
        """
        Create a cancel request for an Order object and send it to the OrderManager

        :param order: Order object
        :return: nothing
        """
        log.info(f'cancelling order: {order}')
        if order.closed:
            log.info('attempting to cancel order already in closed state, request ignored.')
        else:
            self.order_manager.change_state(order, 'CANCEL_REQUESTED')

    def replace_order(self, order, quantity=None, **kwargs):
        """
        Creates a replace request for an Order object

        :param order: Order object
        :param quantity: new quantity, if None then no change in quantity
        :param kwargs: order type specific arguments to replace. Any argument not supplied will be left unchanged
        """
        log.info(f'replacing order: {order}')
        if order.closed:
            log.info('attempting to replace order already in closed state, request ignored.')
        else:
            self.order_manager.replace_order(order, quantity, **kwargs)

    def orders_list(self, filter_dict=None):
        """
        Given a filter dict return a list of Objects

        :param filter_dict: standard filter_dict (see OrderManager)
        :return: list of Orders
        """
        if not filter_dict:
            filter_dict = {}
        filter_dict['originator_uuid'] = self.uuid
        return self.order_manager.orders_list(filter_dict)

    def intent(self, product_type, symbol, target):
        """
        Sets the intent for a given (product_type, symbol) for this strategy in the attached portfolio.

        :param product_type: product type
        :param symbol: symbol
        :param target: target quantity for the intent
        :return: nothing
        """
        log.info(f'setting intent ({product_type}, {symbol}) : {target}')
        self._portfolio.set_intent(self._strategy_id, product_type, symbol, target)

    def get_intent(self, product_type, symbol):
        """
        Returns the current target intent for the (product_type, symbol)

        :param product_type: product type
        :param symbol: symbol name
        :return: target of the intent, or None if none exists
        """
        try:
            return self._portfolio.get_intent(self._strategy_id, product_type, symbol)['target']
        except ValueError:
            return None

    def get(self, product_type, symbol, field):
        """
        Get the field from the PoitionManager for a given product_type X symbol

        :param product_type: product type
        :param symbol: symbol
        :param field: field to get value from
        :return: value
        """
        return self._position_manager.get_value(self._strategy_id, product_type, symbol, field)

    def position(self, product_type, symbol):
        """
        Get the current position for a given product_type X symbol

        :param product_type: product type
        :param symbol: symbol
        :return: current position
        """
        position = self.get(product_type, symbol, 'current_position')
        return 0 if position is None else position

    def start(self):
        """
        PROTECTED. Method to turn the strategy from initialization state to running and call the on_start() method.

        :return: nothing
        """
        log.info('running start()')
        if not self.order_manager:
            raise RuntimeError('Cannot start, strategy is not attached to an OrderManager!')
        if not self.portfolio:
            raise RuntimeError('Cannot start, strategy is not attached to a Portfolio!')

        self.on_start()
        self._started = True

    def on_initialize(self):
        """
        Override this in the concrete implementation. Will be run at end of strategy object __init__
        :return: nothing
        """
        pass

    def on_start(self):
        """
        Called on start of strategy. Do not call direct, call start() instead

        :return: nothing
        """
        pass

    def on_stop(self, bartime):
        """
        Called on stop of strategy.

        :return: nothing
        """
        pass

    def on_begin_of_day(self, bartime):
        """
        Called during the begin_of_day sequence

        :param bartime: bartime of the begin of day
        :return: nothing
        """
        pass

    def on_end_of_day(self, bartime):
        """
        Called during the end_of_day sequence

        :param bartime: bartime of the end of day
        :return: nothing
        """
        pass

    def on_market_open(self, bartime):
        """
        Called during the market open sequence

        :param bartime: bartime of the market opening bar
        :return: nothing
        """
        pass

    def on_market_close(self, bartime):
        """
        Called during the market close sequence

        :param bartime: bartime of the market closing bar
        :return: nothing
        """
        pass

    def on_bar(self, bartime):
        """
        Called on every new bar

        :param bartime: bartime of the bar
        :return: nothing
        """
        pass

    def on_fills(self, bartime, fills):
        """
        Called when one or more of the strategy's orders has been filled. There is no assurance that the order of the
        list corresponds to the order of the fills.

        :param bartime: bartime of the fill event
        :param fills: list of Order objects that were FILLED
        :return: nothing
        """
        pass

    def on_cancels(self, bartime, cancels):
        """
        Called when one or more of the strategy's orders has been canceled. There is no assurance that the order of the
        list corresponds to the order of the cancels.

        :param bartime: bartime of the cancel event
        :param cancels: list of Order objects that were CANCELED
        :return: nothing
        """
        pass


class EmptyStrategy(Strategy):
    pass


# noinspection PyAttributeOutsideInit
class ExampleStrategy(Strategy):
    def on_initialize(self):
        self.barcount = None
        self.start_stop = None
        self.new_days = []
        self.days_done = []
        self.open_days = []
        self.closed_days = []

    def on_start(self):
        self.barcount = 0
        self.start_stop = 1

    def on_stop(self, bartime):
        self.start_stop = 0
        self.stopped = bartime

    def on_begin_of_day(self, bartime):
        self.new_days.append(bartime)

    def on_end_of_day(self, bartime):
        self.days_done.append(bartime)

    def on_market_open(self, bartime):
        self.open_days.append(bartime)

    def on_market_close(self, bartime):
        self.closed_days.append(bartime)

    def on_bar(self, bartime):
        log.info('processing new bar.')
        if bartime == pd.Timestamp('2010-01-01 09:30:00', tz='EST'):
            self.order('stock', 'test.sym.3', 'b', 1000, 'LIMIT', 99.99)
            self.order('stock', 'MSFT', 's', 50, 'LIMIT', 44.50)
            self.order('stock', 'AAPL', 'b', 25, 'LIMIT', 52.52)

    def on_fills(self, bartime, fills):
        log.info('processing fills.')
        self.filled_orders = fills
        for fill in fills:
            if (fill.symbol == 'MSFT') & (fill.fill_price == 44.50):
                self.order('stock', 'MSFT', 'b', 50, 'LIMIT', 43.50)

    def on_cancels(self, bartime, cancels):
        log.info('processing cancels')
        self.canceled_orders = cancels
        if bartime == pd.Timestamp('2099-01-01 10:00:00', tz='EST'):
            self.order('stock', 'test.sym.3', 'b', 99, 'LIMIT', 99.99)
