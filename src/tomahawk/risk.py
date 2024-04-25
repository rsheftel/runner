"""
Risk class
"""

import logging
import uuid

log = logging.getLogger(__name__)


class Risk:
    def __init__(self, order_manager):
        self.__uuid = str(uuid.uuid4())
        self._order_manager = order_manager
        log.info(f'Risk initialized : {self}')

    @property
    def order_manager(self):
        return self._order_manager

    # TODO: This is a dummy stub function, replace with real
    def check_order(self, order):
        """
        Checks the order and based on the math will return either RISK_ACCEPTED or RISK_REJECTED

        :param order: Order object
        :return: status
        """
        # market open check
        if not self._order_manager.market_state(order.product_type):
            return 'RISK_REJECTED'

        # quantity check
        if order.quantity <= 100:
            return 'RISK_ACCEPTED'
        else:
            return 'RISK_REJECTED'

    def reverse_replacement(self, order):
        """
        Revert a replacement back to the prior quantity and details

        :param order: Order object
        :return: nothing
        """
        prior_quantity = order.replaces[len(order.replaces) - 2, 'quantity']
        prior_details = order.replaces[len(order.replaces) - 2, 'details']
        self.order_manager.replace_order(order, prior_quantity, **prior_details)

    def process_order(self, order):
        """
        Processes an order by performing the risk check and then changing its state in the OrderManager. If the order
        is put into RISK_REJECTED then close the Order as well.

        :param order: Order object
        :return: nothing
        """
        status = self.check_order(order)
        if order.state == 'REPLACE_REQUESTED':
            if status == 'RISK_REJECTED':
                self.order_manager.change_state(order, 'REPLACE_REJECTED')
                self.reverse_replacement(order)
        else:
            self.order_manager.change_state(order, status)
            if status == 'RISK_REJECTED':
                self.order_manager.close_order(order)

    def process_portfolio_orders(self, portfolio):
        """
        Process the orders in the OrdderManager for a given Portfolio that are in STAGED or REPLACE_REQUESTED state.

        :param portfolio: Portfolio object
        :return: nothing
        """
        log.info(f'processing orders for portfolio: {portfolio}')
        orders = self.order_manager.orders_list({'portfolio_id': portfolio.id,
                                                 'state': ['STAGED', 'REPLACE_REQUESTED']})
        for order in orders:
            self.process_order(order)
