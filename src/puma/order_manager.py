"""
OrderManager class
"""

import logging
import uuid

import raccoon as rc

import utils.collections as cutils
from database import tapdb
from puma import order as tw_order

log = logging.getLogger(__name__)


class OrderManager:
    """
    Order manager
    """

    def __init__(self, order_manager_id: str, tapdb_engine):
        """
        :param order_manager_id: unique string ID
        :param tapdb_engine: sqlalchemy engine for TAPDB
        """
        self.__uuid = str(uuid.uuid4())
        self._order_manager_id = order_manager_id
        self._tapdb = tapdb_engine
        self._initialize_orders_df()
        self._market_state = {}
        log.info(f'OrderManager initialized : {self}')

    @property
    def id(self):
        return self._order_manager_id

    def _initialize_orders_df(self):
        """
        Initialize the orders DataFrame and delete all existing entries. Dangerous to call, do not call direct.

        :return: nothing
        """
        self._orders = rc.DataFrame(columns=['originator_uuid', 'originator_id', 'portfolio_uuid', 'portfolio_id',
                                             'strategy_uuid', 'strategy_id', 'product_type', 'symbol', 'state',
                                             'booked', 'closed', 'object'],
                                    index_name='object_uuid', sort=False)

    def new_order(self, order):
        """
        Insert a new order into the OrderManager.

        :param order: Order object
        :return: nothing
        """
        self._orders.set_row(order.uuid, {'originator_uuid': order.originator_uuid,
                                          'originator_id': order.originator_id,
                                          'portfolio_uuid': order.portfolio_uuid,
                                          'portfolio_id': order.portfolio_id,
                                          'strategy_uuid': order.strategy_uuid,
                                          'strategy_id': order.strategy_id,
                                          'product_type': order.product_type,
                                          'symbol': order.symbol,
                                          'state': order.state,
                                          'booked': order.booked,
                                          'closed': order.closed,
                                          'object': order})

    def change_state(self, order, state):
        """
        Changes the order state and updates the internal data structure

        :param order: Order object
        :param state: new state
        :return: nothing
        """
        if state != order.state:
            self._orders[order.uuid, 'state'] = state
            order.state = state

    def close_order(self, order):
        """
        Closes an order by changing the .close attribute to True and updating the orders DatFrame. This is the safe way
        to change the closed state otherwise the DataFrame may become out of sync with the object.

        :param order: Order object
        :return: nothing
        """
        if order.state not in tw_order.states()['closed']:
            raise RuntimeError(f'Cannot close order because the state {order.state} not a closed state.')
        self._orders[order.uuid, 'closed'] = True
        order.closed = True

    def replace_order(self, order, quantity=None, **kwargs):
        """
        Creates a replace request for an Order object

        :param order: Order object
        :param quantity: new quantity, if None then no change in quantity
        :param kwargs: order type specific arguments to replace. Any argument not supplied will be left unchanged
        """
        order.replace(quantity, **kwargs)
        self.change_state(order, 'REPLACE_REQUESTED')

    def set_booked(self, order, boolean):
        """
        Changes the booked state of an Order and the associated entry in the orders DataFrame. This is the safe way
        to change booked state otherwise the DataFrame may become out of sync with the object.

        :param order: Order object
        :param boolean: booked state
        :return: nothing
        """
        self._orders[order.uuid, 'booked'] = boolean
        order.booked = boolean

    def add_portfolio(self, order, portfolio):
        """
        Adds a Portfolio object to an order so that the order can be properly tracked

        :param order: Order object
        :param portfolio: Portfolio object
        :return: nothing
        """
        order.portfolio_uuid = portfolio.uuid
        order.portfolio_id = portfolio.id
        self._orders[order.uuid, 'portfolio_uuid'] = order.portfolio_uuid
        self._orders[order.uuid, 'portfolio_id'] = order.portfolio_id

    def _get_orders(self, filter_dict=None):
        """
        Returns a DataFrame of the orders with the key identifying columns and a column of the Order object itself.

        :param filter_dict: a dictionary of filters where the key is the column name and the values are a value or
                            list of values to filter for. The filter is an OR filter for items in the list of values,
                            AND filtering between the keys. If None then all values returned
        :return: a view on the orders DataFrame
        """
        screen = [True] * len(self._orders)
        if filter_dict:
            for key in filter_dict:
                filter_list = filter_dict[key] if isinstance(filter_dict[key], list) else [filter_dict[key]]
                screen = cutils.list_and(screen, self._orders.isin(key, filter_list))
        return self._orders.get(indexes=screen)

    def order(self, order_uuid):
        """
        Return an Order object given a UUID

        :param order_uuid: UUID
        :return: Order object
        """
        return self._orders[order_uuid, 'object']

    def orders_list(self, filter_dict=None):
        """
        Return a list of order objects for a given filter

        :param filter_dict: a dictionary of filters where the key is the column name and the values are a value or
                    list of values to filter for. The filter is an OR filter for items in the list of values,
                    AND filtering between the keys. If None then all values returned
        :return: list of Order objects
        """
        return self._get_orders(filter_dict)['object'].to_list()

    def orders_df(self, filter_dict=None):
        """
         Return a DataFrame of the order object's properties for a given filter

         :param filter_dict: a dictionary of filters where the key is the column name and the values are a value or
                     list of values to filter for. The filter is an OR filter for items in the list of values,
                     AND filtering between the keys. If None then all values returned
         :return: pandas DataFrame
         """

        order_list = [x.to_dict() for x in self.orders_list(filter_dict)]
        if order_list:
            values = cutils.invert_list_of_dict(order_list)
            order_df = rc.DataFrame(values, sort=True)
            order_df.sort_columns('create_timestamp')
            order_df.index = list(range(len(order_df)))
            return order_df
        else:
            return rc.DataFrame()

    def open_orders_df(self, filter_dict=None):
        """
        Returns a DataFrame of the open orders

         :param filter_dict: a dictionary of filters where the key is the column name and the values are a value or
                     list of values to filter for. The filter is an OR filter for items in the list of values,
                     AND filtering between the keys. If None then all values returned
         :return: pandas DataFrame
        """
        filters = {'state': tw_order.states()['open']}
        if filter_dict:
            filters.update(filter_dict)
        return self.orders_df(filters)

    def closed_orders_df(self, filter_dict=None):
        """
        Returns a DataFrame of the closed orders

         :param filter_dict: a dictionary of filters where the key is the column name and the values are a value or
                     list of values to filter for. The filter is an OR filter for items in the list of values,
                     AND filtering between the keys. If None then all values returned
         :return: pandas DataFrame
        """
        filters = {'state': tw_order.states()['closed']}
        if filter_dict:
            filters.update(filter_dict)
        return self.orders_df(filters)

    def to_be_booked_list(self):
        """
        Returns a list of order objects that are in a state that they need to be booked by the PositionManager but have
        not yet been booked. Use this to get the objects for the PositionManager to book.

        :return: list of Order objects
        """
        return self.orders_list({'state': ['FILLED', 'PARTIALLY_FILLED'], 'booked': False})

    def cancels_to_process(self):
        """
        Returns a list of Order objects that are in a CANCELED state that need to be processed by the process_cancels
        of the EventProcessor.

        :return: list of Order objects
        """
        return self.orders_list({'state': 'CANCELED', 'closed': False})

    def market_state(self, product_type, state=None):
        """
        Sets or gets the state of the market for a given product_type

        :param product_type: product type
        :param state: True (OPEN) or False (CLOSED) to set state, None to retrieve current state
        :return: If state is None then will return current state
        """
        if state is None:
            return self._market_state[product_type]
        elif type(state) is bool:
            log.info(f'changing market state for product_type {product_type} to {state}')
            self._market_state[product_type] = state
        else:
            raise ValueError('state can only be True (OPEN) or False (CLOSED)')

    def stop(self, datetime):
        """
        Run the stop processes

        :param: datetime of the stop
        :return: nothing
        """
        log.info('running Stop process')
        self.save_orders_df(datetime)

    def end_of_day(self, datetime):
        """
        Run the end of day processes

        :param: datetime of the end of day
        :return: nothing
        """
        log.info('running EOD process')
        self.save_orders_df(datetime)
        self._initialize_orders_df()

    def save_orders_df(self, datetime):
        """
        Save orders DataFrame to the database (connected by the tapdb_engine)

        :param: datetime of the save
        :return: nothing
        """
        tapdb.insert_orders_df(self._tapdb, self.id, datetime, self.orders_df())
