"""
Portfolio class
"""

import logging
import uuid

import raccoon as rc

from puma.order import Order

log = logging.getLogger(__name__)


class Portfolio:
    """
    Portfolio object. Takes orders and intents from strategies and transforms them into orders
    """

    def __init__(self, portfolio_id, order_manager, position_manager):
        """
        :param portfolio_id: Unique string ID
        :param order_manager: OrderManager object
        """
        self.__uuid = str(uuid.uuid4())
        self._portfolio_id = portfolio_id
        self._order_manager = order_manager
        self._position_manager = position_manager
        self._market_data_manager = None
        self._live_frequency = None
        self._strategies = {}
        self._intents = rc.DataFrame(columns=['target', 'order'], sort=True)
        log.info(f'Portfolio initialized : {self}')

    @property
    def uuid(self):
        return self.__uuid

    @property
    def id(self):
        return self._portfolio_id

    @property
    def order_manager(self):
        return self._order_manager

    @property
    def intents(self):
        return self._intents

    @property
    def position_manager(self):
        return self._position_manager

    @property
    def market_data_manager(self):
        return self._market_data_manager

    def setup_market_data(self, market_data_manager, live_frequency='1min'):
        """
        Connect to the MarketDataManager for intent calculations.

        :param market_data_manager: MarketDataManager object
        :param live_frequency: frequency to use for getting the live price for intent order
        :return: nothing
        """
        log.info(f'setting up market data manager: {market_data_manager}')
        self._market_data_manager = market_data_manager
        self._live_frequency = live_frequency

    @property
    def strategy_ids(self):
        """
        Returns a list of the strategy_ids that are attached to the portfolio

        :return: list of strategy ids
        """
        return [strategy.strategy_id for strategy in self._strategies.values()]

    def add_strategy(self, strategy):
        """
        Attach a Strategy to the portfolio

        :param strategy: Strategy object
        :return: nothing
        """
        self._strategies[strategy.strategy_id] = strategy
        strategy.attach_portfolio(self)

    def set_intent(self, strategy_id, product_type, symbol, target):
        """
        Set the intent for a give (strategy_id, product_type, symbol)
        :param strategy_id: strategy ID
        :param product_type: product type
        :param symbol: symbol
        :param target: target quantity for the intent
        :return: nothing
        """
        log.info(f'setting intent ({strategy_id}, {product_type}, {symbol}) : {target}')
        self._intents.set_cell((strategy_id, product_type, symbol), 'target', target)

    def get_intent(self, strategy_id, product_type, symbol):
        """
        Get the intent for a give (strategy_id, product_type, symbol)
        :param strategy_id: strategy ID
        :param product_type: product type
        :param symbol: symbol
        :return: {target, order} dict
        """
        return self._intents.get_columns(index=(strategy_id, product_type, symbol), columns=['target', 'order'],
                                         as_dict=True)

    def _new_order(self, strategy_id, product_type, symbol, trade_to_do):
        """
        Create an order in the OrderManager that will later be picked up by the Portfolio

        :param strategy_id: strategy ID
        :param product_type: product_type
        :param symbol: symbol
        :param trade_to_do: trade to do, positive numbers for buy, negative numbers for sell
        :return: Order object
        """
        if product_type not in self._strategies[strategy_id].symbols:
            raise RuntimeError(f'Cannot create order for product_type not added to strategy: {product_type}')
        if symbol not in self._strategies[strategy_id].symbols[product_type]:
            raise RuntimeError(f'Cannot create order for symbol not added to strategy : {product_type} : {symbol}')

        quantity = abs(trade_to_do)
        buy_sell = 'B' if trade_to_do > 0 else 'S'
        order_type = 'LIMIT'
        price = self._market_data_manager.last_valid_bar(product_type, symbol, self._live_frequency)['close']

        log.info(f'creating new order from intent: {symbol} : {buy_sell} : {quantity} : {order_type} : {price}')
        order = Order(self.uuid, 'portfolio.' + self.id, self._strategies[strategy_id].uuid, strategy_id,
                      product_type, symbol, buy_sell, quantity, order_type, price=price)
        self.order_manager.new_order(order)
        return order

    def _cancel_order(self, order):
        """
        Cancel order in OrderManager by submitting cancel request

        :param order: Order object
        :return: nothing
        """
        log.info(f'cancelling intent order: {order}')
        if order.closed:
            log.info('attempting to cancel order already in closed state, request ignored.')
        else:
            self.order_manager.change_state(order, 'CANCEL_REQUESTED')

    def _modify_order(self, order, trade_to_do):
        """
        Modifies an existing intent order price and/or quantity. If the order is closed then create a new order.

        :param order: Order object
        :param trade_to_do: trade to do, positive numbers for buy, negative numbers for sell
        :return: Order object, either the existing order modified or
        """
        if order.closed:
            log.info('attempting to replace order already in closed state, request ignored.')
            return self._new_order(order.strategy_id, order.product_type, order.symbol, trade_to_do)
        else:
            filled_quantity = order.fill_quantity if order.fill_quantity is not None else 0
            quantity = abs(trade_to_do) + filled_quantity
            price = self._market_data_manager.last_valid_bar(order.product_type, order.symbol,
                                                             self._live_frequency)['close']
            log.info(f'modifying intent order : price : quantity :: {order} : {price} : {quantity}')
            self.order_manager.replace_order(order, quantity, price=price)
            return order

    def process_intent(self, intent):
        """
        Process intent by calculating the trades to do and updating the order object. The originator of the orders
        will be this portfolio.

        :param intent: intent dictionary
        :return: nothing
        """
        strategy_id = intent['index'][0]
        product_type = intent['index'][1]
        symbol = intent['index'][2]
        log.info(f'processing intent ({strategy_id}, {product_type}, {symbol})')

        target = intent['target']
        order = intent['order']

        if target is not None:  # there is an intent
            actual = self._position_manager.get_value(strategy_id, product_type, symbol, 'current_position')
            ttd = target - actual if actual is not None else target
            if order:  # there is already an order
                if ttd == 0:  # there is no trade to do, cancel order
                    self._cancel_order(order)
                    self._intents.set_cell((strategy_id, product_type, symbol), 'order', None)
                else:
                    order_direction = order.buy_sell
                    if ((ttd < 0) and (order_direction == 'sell')) or ((ttd > 0) and (order_direction == 'buy')):
                        return_order = self._modify_order(order, ttd)
                        if return_order != order:  # if the order is new, then update the intents
                            self._intents.set_cell((strategy_id, product_type, symbol), 'order', return_order)
                    else:  # existing order wrong direction for TTD, so cancel and send new order
                        self._cancel_order(order)
                        new_order = self._new_order(strategy_id, product_type, symbol, ttd)
                        self._intents.set_cell((strategy_id, product_type, symbol), 'order', new_order)
            else:  # new order
                if ttd != 0:  # only create new order if there is a trade to do
                    new_order = self._new_order(strategy_id, product_type, symbol, ttd)
                    self._intents.set_cell((strategy_id, product_type, symbol), 'order', new_order)
        else:  # no intent
            if order:
                self._cancel_order(order)
                self._intents.set_cell((strategy_id, product_type, symbol), 'order', None)

    def process_intents(self):
        """
        Process all intents by updating the order on each.

        :return: nothing
        """
        log.info("processing intents.")
        for intent in self._intents.iterrows():
            self.process_intent(intent)
        self._intents['target'] = None

    def process_orders(self):
        """
        First processes the intents in the portfolio to turn them into Orders, then process the orders in the
        OrderManager that have been created by the attached strategies or the portfolio.

        :return: nothing
        """
        self.process_intents()
        for strategy in self._strategies.values():
            log.info('processing orders for strategy: ' + strategy.strategy_id)
            orders = self.order_manager.orders_list({'strategy_uuid': strategy.uuid, 'state': 'CREATED'})
            for order in orders:
                self.order_manager.add_portfolio(order, self)
                self.order_manager.change_state(order, 'STAGED')
