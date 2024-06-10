"""
Exchange class used to define paper exchanges
"""

import datetime
import logging
import uuid
from abc import ABCMeta
from collections import OrderedDict, namedtuple

import raccoon as rc

log = logging.getLogger(__name__)

Exchange_Fill = namedtuple('Fill', "id, timestamp, quantity, price")
Exchange_Replace = namedtuple('Replace', 'quantity, details')


class Exchange(metaclass=ABCMeta):
    def __init__(self):
        self.__uuid = str(uuid.uuid4())

    @property
    def uuid(self):
        return self.__uuid

    def get_order_state(self, order):
        raise NotImplementedError

    def update_market_data(self):
        raise NotImplementedError

    def process_orders(self, market_data_manager):
        raise NotImplementedError


# noinspection PyAbstractClass
class PaperExchange(Exchange):
    """
    Paper exchange to be used in simulation with a PaperBroker()
    """

    def __init__(self, live_frequency='1min', parameters=None):
        """
        Setup for PaperExachange.

        :param live_frequency: time interval that will be used for market data
        :param parameters: dictionary of parameters values to override defaults
        """
        super().__init__()
        self._live_frequency = live_frequency
        self._parameters = {}
        self._set_parameters(parameters)
        self._open_orders = OrderedDict()
        self._closed_orders = OrderedDict()
        self._order_id = int(datetime.datetime.now().strftime('%y%m%d%H%M%S')) * 1000000 + 1
        self._fill_id = int(datetime.datetime.now().strftime('%y%m%d%H%M%S')) * 1000000 + 1
        log.info(f"PaperExchange initialized: {self}")

    @property
    def live_frequency(self):
        return self._live_frequency

    @live_frequency.setter
    def live_frequency(self, frequency):
        self._live_frequency = frequency

    def _set_parameters(self, parameters):
        # set default values
        self._parameters['fill_multiplier'] = 0.5

        if parameters:
            if any(x not in self._parameters for x in parameters):
                raise ValueError('invalid keys in the parameters dict.')
            for key, value in parameters.items():
                self._parameters[key] = value

    @property
    def parameters(self):
        return self._parameters

    @property
    def fill_id(self):
        return self._fill_id

    def receive_order(self, product_type, symbol, buy_sell, quantity, order_type, **kwargs):
        """
        Receive an order from a Broker

        :param product_type: product type
        :param symbol: symbol
        :param buy_sell: buy, sell, B or S
        :param quantity: quantity
        :param order_type: order type
        :param kwargs: order details
        :return: exchange order ID
        """
        order = {'product_type': product_type,
                 'symbol': symbol,
                 'state': 'LIVE',
                 'buy_sell': buy_sell,
                 'quantity': quantity,
                 'order_type': order_type,
                 'fill_quantity': None,
                 'fill_price': None}
        order.update(kwargs)
        order['order_id'] = self._order_id
        order['replaces'] = [Exchange_Replace(quantity=quantity, details=kwargs)]
        self._order_id += 1

        # make this a fifo queue and push into it
        self._open_orders[order['order_id']] = order

        log.info('order received, exchange order id: %d' % order['order_id'])
        return order['order_id']

    def receive_cancel(self, order_id):
        """
        Receive a cancel order request from a Broker

        :param order_id: Exchange order ID to cancel
        :return: nothing
        """
        log.info('cancel request received for order id: %d' % order_id)
        order = self.get_order(order_id)
        if order_id in self.open_orders_df.index:  # if the order is still open
            order['state'] = 'CANCEL_SENT'

    def receive_replace(self, order_id, quantity, details):
        """
        Receive a replace order request from a Broker

        :param order_id: Exchange order ID to replace
        :param quantity: new quantity
        :param details: dict of new details
        :return: nothing
        """
        log.info(f'replace request received for order id: {order_id}, quantity: {quantity}, details: {details}')
        order = self.get_order(order_id)
        if order_id in self.open_orders_df.index:  # if the order is still open
            order['state'] = 'REPLACE_SENT'
            order['replaces'].append(Exchange_Replace(quantity=quantity, details=details))

    def fill_quantity(self, order, bar):
        """
        Given an order and bar returns the quantity of the fill.

        :param order: exchange order
        :param bar: market bar
        :return:
        """
        fills_quantity = order['fill_quantity'] if order['fill_quantity'] is not None else 0
        remain_quantity = order['quantity'] - fills_quantity
        return int(min([remain_quantity, bar['volume'] * self._parameters['fill_multiplier']]))

    def _make_filled(self, order, timestamp):
        """
        Put order into FILLED state and process properly.

        :param order: exchange order
        :param timestamp: timestamp of the close
        :return: nothing
        """
        order['close_bar_timestamp'] = timestamp
        order['state'] = 'FILLED'
        self._closed_orders[order['order_id']] = self._open_orders.pop(order['order_id'])

    def fill_order(self, order, fill_quantity, timestamp):
        """
        Change an order into a PARTIALLY_FILLED or FILLED state and add the associated items to the order, including
        the fill qty and px.

        :param order: exchange order
        :param fill_quantity: quantity of the fill
        :param timestamp: timestamp of the fill
        :return: nothing
        """
        # confirm there is a timezone on the timestamp
        if timestamp.tz is None:
            raise ValueError('timestamp must have a time zone attached')

        # Add the fill to the fill list
        fill = Exchange_Fill(id=self._fill_id, timestamp=timestamp, quantity=fill_quantity, price=order['price'])
        self._fill_id += 1
        if 'fills' in order:
            order['fills'].append(fill)
        else:
            order['fills'] = [fill]

        # update the total fill_quantity and average fill price
        if order['fill_quantity']:  # if this is not the first fill
            order['fill_price'] = (order['fill_price'] * order['fill_quantity'] + order['price'] * fill_quantity) / \
                                  (order['fill_quantity'] + fill_quantity)
            order['fill_quantity'] += fill_quantity
        else:
            order['fill_quantity'] = fill_quantity
            order['fill_price'] = order['price']

        if order['fill_quantity'] >= order['quantity']:  # order is complete
            self._make_filled(order, timestamp)
        else:  # only partially filled
            order['state'] = 'PARTIALLY_FILLED'

        log.info('order fill: %d', order['order_id'])

    def cancel_order(self, order, timestamp):
        """
        Change an order to CANCELED state

        :param order: exchange order
        :param timestamp: timestamp of the cancel
        :return: nothing
        """
        order['state'] = 'CANCELED'
        order['close_bar_timestamp'] = timestamp
        self._closed_orders[order['order_id']] = self._open_orders.pop(order['order_id'])
        log.info('order canceled: %d', order['order_id'])

    def replace_order(self, order, timestamp):
        """
        Replace an order. If the new quantity is <= filled quantity then put into FILLED state

        :param order: exchange order
        :param timestamp: timestamp of the replace
        :return: nothing
        """
        replacement_values = order['replaces'][-1]
        order['quantity'] = replacement_values.quantity
        for key, value in replacement_values.details.items():
            order[key] = value

        if order['fill_quantity'] and (order['fill_quantity'] >= order['quantity']):
            self._make_filled(order, timestamp)
        else:
            order['state'] = 'LIVE'
        log.info('order replaced: %d', order['order_id'])

    def process_order(self, order, market_data_manager):
        """
        Run the matching engine process an order. If it meets a fill condition will be sent to the fill order method.

        :param order: exchange order
        :param market_data_manager: Market data manager for the market data
        :return: nothing
        """
        log.info('processing order:', order)
        bar = market_data_manager.current_bar(order['product_type'], order['symbol'], self._live_frequency)

        if order['state'] == 'CANCEL_SENT':
            self.cancel_order(order, market_data_manager.bartime)
        elif order['state'] == 'REPLACE_SENT':
            self.replace_order(order, market_data_manager.bartime)

        if order['state'] in ['LIVE', 'PARTIALLY_FILLED']:
            if order['order_type'] == 'LIMIT':
                if order['buy_sell'] == 'buy':
                    if bar['low']:
                        if bar['low'] < order['price']:
                            quantity = self.fill_quantity(order, bar)
                            self.fill_order(order, quantity, market_data_manager.bartime)
                    else:
                        log.info('bar low price is None so buy order cannot be processed')
                if order['buy_sell'] == 'sell':
                    if bar['high']:
                        if bar['high'] > order['price']:
                            quantity = self.fill_quantity(order, bar)
                            self.fill_order(order, quantity, market_data_manager.bartime)
                    else:
                        log.info('bar high price is None so sell order cannot be processed')
            else:
                raise RuntimeError('only order type LIMIT supported')

    def process_orders(self, market_data_manager):
        """
        Run the matching engine process an all live orders.

        :param market_data_manager: Market data manager for the market data
        :return: nothing
        """
        log.info('processing orders')
        for order_id, order in self._open_orders.copy().items():
            log.info('processing order: %d', order_id)
            self.process_order(order, market_data_manager)

    def open_order(self, order_id):
        return self._open_orders[order_id]

    @property
    def open_orders_df(self):
        values = self._open_orders.values()
        if values:
            return rc.DataFrame({k: [x[k] for x in values] for k in
                                 self._open_orders[list(self._open_orders.keys())[0]]},
                                index=list(self._open_orders.keys()), sort=False)
        else:
            return rc.DataFrame()

    @property
    def open_orders_list(self):
        return list(self._open_orders.values())

    @property
    def closed_orders_df(self):
        values = self._closed_orders.values()
        if values:
            return rc.DataFrame({k: [x[k] for x in values] for k in
                                 self._closed_orders[list(self._closed_orders.keys())[0]]},
                                index=list(self._closed_orders.keys()), sort=False)
        else:
            return rc.DataFrame()

    @property
    def closed_orders_list(self):
        return list(self._closed_orders.values())

    def get_order(self, order_id):
        """
        Given an exchange order_id returns the order dict

        :param order_id: exchange order ID
        :return: order dict
        """
        if order_id in self._open_orders:
            return self.open_order(order_id)
        else:
            return self._closed_orders[order_id]

    def market_close(self, timestamp):
        """
        Run the end of day process for the exchange by canceling all outstanding orders.

        :param timestamp: timestamp of the market close
        :return: nothing
        """
        log.info('canceling orders in the exchange')
        for order in self.open_orders_list:
            self.cancel_order(order, timestamp)
