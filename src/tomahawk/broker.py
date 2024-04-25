"""
Broker connections classes
"""

import datetime
import logging
import uuid
from abc import ABCMeta

import pandas as pd

log = logging.getLogger(__name__)


class Broker(metaclass=ABCMeta):
    def __init__(self, broker_id, order_manager):
        self.__uuid = str(uuid.uuid4())
        self._broker_id = broker_id
        self._order_manager = order_manager

    @property
    def uuid(self):
        return self.__uuid

    @property
    def broker_id(self):
        return self._broker_id

    @property
    def order_manager(self):
        return self._order_manager

    def order_id(self):
        raise NotImplementedError

    def send_order_to_exchange(self, order):
        """
        Send an order to the attached exchange

        :param order: Order object
        :return: nothing
        """
        raise NotImplementedError

    def get_exchange_order(self, order):
        """
        Get the current order state. Returns a dict of the state

        :param order: Order object
        :return: returns a dict of {'state', 'fill_quantity', 'fill_price'} If there is no fill those keys are not
                 included
        """
        raise NotImplementedError

    def get_order_response(self, order):
        raise NotImplementedError

    def source_symbol(self, symbol):
        raise NotImplementedError


# noinspection PyAbstractClass
class PaperBroker(Broker):
    """
    Paper Broker to be used in simulations. Requires a connection to the Paper Exchange
    """

    def __init__(self, broker_id, order_manager, exchange, parameters=None):
        """
        Setup PaperBroker. The parameters used dictionary can be used to override the defaults.

        :param broker_id: unique string ID for the Broker
        :param order_manager: OrderManager object
        :param exchange: PaperExchange object
        :param parameters: dictionary of parameter values
        """
        self._exchange = exchange
        self._order_id = int(datetime.datetime.now().strftime('%y%m%d%H%M%S')) * 1000000 + 100
        super().__init__(broker_id, order_manager)
        self._parameters = {}
        self._set_parameters(parameters)
        log.info(f"PaperBroker initialized : {self} : attached to exchange: {self._exchange}")

    def _set_parameters(self, parameters):
        # set default values
        self._parameters['stock_fee_per_share'] = -0.01

        if parameters:
            if any(x not in self._parameters for x in parameters):
                raise ValueError('invalid keys in the parameters dict.')
            for key, value in parameters.items():
                self._parameters[key] = value

    @property
    def parameters(self):
        return self._parameters

    def order_id(self):
        """
        Returns the order ID from the broker and increments the ID.

        :return: int
        """
        hold = self._order_id
        self._order_id += 1
        return hold

    def send_order_to_exchange(self, order):
        """
        Sends order to PaperExchange

        :param order: Order object
        :return: nothing
        """
        log.info(f"sending order to exchange: {order}")
        order.exchange_order_id = self._exchange.receive_order(product_type=order.product_type,
                                                               symbol=order.symbol,
                                                               buy_sell=order.buy_sell,
                                                               quantity=order.quantity,
                                                               order_type=order.type,
                                                               **order.details)

    def send_order(self, order):
        """
        Check that the order is RISK_ACCEPTED and if so then send to the exchange.

        :param order: Order object
        :return: nothing
        """
        if order.state != 'RISK_ACCEPTED':
            raise RuntimeError('order not in RISK_ACCEPTED state, cannot send:', order)

        order.broker_order_id = self.order_id()
        log.info('broker order id: %d' % order.broker_order_id)
        self.order_manager.change_state(order, 'SENT')
        self.send_order_to_exchange(order)

    def send_risk_accepted(self):
        """
        Sends all the orders in the RISK_ACCEPTED list to the exchange

        :return: nothing
        """
        log.info(f'sending orders to exchange: {self._exchange}')
        orders = self.order_manager.orders_list({'state': 'RISK_ACCEPTED'})
        for order in orders:
            self.send_order(order)

    def send_cancel_to_exchange(self, order):
        """
        Send cancel request to exchange

        :param order: Order object
        :return: nothing
        """
        log.info(f"sending cancel request to exchange for order: {order}")
        self._exchange.receive_cancel(order.exchange_order_id)

    def send_cancel(self, order):
        """
        Process a cancel request

        :param order: Order object
        :return: nothing
        """
        self.order_manager.change_state(order, 'CANCEL_SENT')
        self.send_cancel_to_exchange(order)

    def send_cancel_requested(self):
        """
        Get all orders in the state CANCEL_REQUESTED and send that cancel request to the Exchange

        :return: nothing
        """
        log.info(f'sending cancel requests to exchange: {self._exchange}')
        orders = self.order_manager.orders_list({'state': 'CANCEL_REQUESTED'})
        for order in orders:
            if order.exchange_order_id:  # has been sent to exchange already
                self.send_cancel(order)
            else:  # not sent to exchange yet so can just cancel
                self.order_manager.change_state(order, 'CANCELED')

    def send_replace_to_exchange(self, order):
        """
        Send replace request to exchange

        :param order: Order object
        :return: nothing
        """
        log.info(f"sending replace request to exchange for order: {order}")
        self._exchange.receive_replace(order.exchange_order_id, order.quantity, order.details)

    def send_replace(self, order):
        """
        Process a replace request

        :param order: Order object
        :return: nothing
        """
        self.order_manager.change_state(order, 'REPLACE_SENT')
        self.send_replace_to_exchange(order)

    def send_replace_requested(self):
        """
        Get all orders in the state REPLACE_REQUESTED and send that replace request to the Exchange. If the order has
        not yet reached the exchange and does not have an exchange order ID it will raise an error.

        :return: nothing
        """
        log.info(f'sending replace requests to exchange: {self._exchange}')
        orders = self.order_manager.orders_list({'state': 'REPLACE_REQUESTED'})
        for order in orders:
            if order.exchange_order_id:  # has been sent to exchange already
                self.send_replace(order)
            else:  # not sent to exchange yet so bork on this
                raise RuntimeError('cannot request REPLACE on an order that has not made it to the exchange yet.')

    def send_orders(self):
        """
        Sends all the orders in the RISK_ACCEPTED and CANCEL_REQUESTED state to the exchange

        :return: nothing
        """
        self.send_cancel_requested()
        self.send_replace_requested()
        self.send_risk_accepted()

    def get_exchange_order(self, order):
        """
        Get order dict from the exchange

        :param order: Order object
        :return: exchange order dict
        """
        return self._exchange.get_order(order.exchange_order_id)

    def commission(self, order, exchange_fill):
        """
        Calculates the commission for an order

        :param order: Order object
        :param exchange_fill: exchange fill named tuple
        :return: commission amount
        """
        if order.product_type == 'stock':
            return exchange_fill.quantity * self._parameters['stock_fee_per_share']
        else:
            raise ValueError('only orders for product_type stock can calculate commissions.')

    def process_fills(self, order, exchange_order):
        """
        Process the fills on an exchange order and put them into the Order object. If the order has no un-booked child
        fills and is in the FILLED state, then the Order will have closed set to True. This captures orders where the
        replace request quantity is less than the quantity already filled.

        :param order: Order object
        :param exchange_order: exchange order dict{}
        :return: nothing
        """
        exchange_fills = exchange_order['fills']
        order_fills = order.fills
        existing_fill_ids = order_fills.index

        new_trades = False
        for fill in exchange_fills:
            if fill.id not in existing_fill_ids:
                order.add_fill(fill.id, pd.Timestamp.now(tz='UTC'), fill.timestamp, fill.quantity, fill.price,
                               self.commission(order, fill))
                self._order_manager.set_booked(order, False)
                new_trades = True

        if (not new_trades) and (order.state == 'FILLED'):
            self.order_manager.close_order(order)

    def update_order_state(self, order):
        """
        Get the order state from the exchange and process the result in the order_manager. If the order has fills,
        process those.

        :param order: Order object
        :return: nothing
        """
        exchange_order = self.get_exchange_order(order)
        if exchange_order['state'] != order.state:  # If the state has changed
            self.order_manager.change_state(order, exchange_order['state'])  # Then update the state
        if exchange_order['state'] in ['PARTIALLY_FILLED', 'FILLED']:
            self.process_fills(order, exchange_order)

    def update_order_states(self):
        """
        Update the order state for all open active orders.

        :return: nothing
        """
        log.info(f'getting order states from exchange: {self._exchange}')
        orders = self.order_manager.orders_list({'state': ['LIVE', 'SENT', 'CANCEL_SENT', 'REPLACE_SENT',
                                                           'PARTIALLY_FILLED']})
        for order in orders:
            self.update_order_state(order)
