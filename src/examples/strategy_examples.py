"""
Examples strategies used in unit tests
"""

import logging

import numpy as np
import pandas as pd

import montauk.metric as metric
from config.datetime import default_time_zone
from montauk.data.structures import Bar
from montauk.tomahawk.strategy import Strategy

log = logging.getLogger(__name__)


# noinspection PyAttributeOutsideInit
class UnitTest_01(Strategy):
    """
    Unit test strategy that tests:
    - bartime
    - originating basic orders
    - cancel orders
    - partial fills
    """

    def on_initialize(self):
        self.barcount = None

    def on_start(self):
        self.barcount = 0

    def on_bar(self, bartime):
        if bartime == pd.Timestamp('2010-01-04 09:30:00', tz=default_time_zone):
            # Order to be RISK_REJECTED
            self.order('stock', 'test.sym.9', 'b', 1000, 'LIMIT', 55.5)
            # Order to be SENT and FILLED on next bar
            self.order('stock', 'test.sym.9', 'b', 25, 'LIMIT', 51.75)
            # Orders to be SENT but not FILLED on next bar
            self.id3 = self.order('stock', 'test.sym.9', 's', 25, 'LIMIT', 52.1)
        if bartime == pd.Timestamp('2010-01-04 09:31:00', tz=default_time_zone):
            # get outstanding orders
            order_to_cancel = self.order_manager.order(self.id3)
            self.cancel_order(order_to_cancel)
        if bartime == pd.Timestamp('2010-01-04 09:32:00', tz=default_time_zone):
            self.id6 = self.order('stock', 'test.sym.9', 'buy', 100, 'LIMIT', 50.5)
        if bartime == pd.Timestamp('2010-01-04 09:33:00', tz=default_time_zone):
            self.cancel_order(self.order_manager.order(self.id6))
        if bartime == pd.Timestamp('2010-01-04 09:35:00', tz=default_time_zone):
            self.id7 = self.order('stock', 'test.sym.9', 'sell', 105, 'LIMIT', 52.5)
            self.cancel_order(self.order_manager.order(self.id7))
        if bartime == pd.Timestamp('2010-01-04 09:37:00', tz=default_time_zone):
            self.id9 = self.order('stock', 'test.sym.9', 'B', 100, 'LIMIT', 51.6)
            self.id10 = self.order('stock', 'test.sym.9', 'S', 100, 'LIMIT', 52.02)

    def on_fills(self, bartime, fills):
        for order in fills:
            if order.uuid == getattr(self, 'id4', None):
                self.order('stock', 'test.sym.9', 'buy', 50, 'LIMIT', 51.5)
            if order.uuid == getattr(self, 'id10', None):
                self.cancel_order(self.order_manager.order(self.id10))

    def on_cancels(self, bartime, cancels):
        if getattr(self, 'id3', None) in [x.uuid for x in cancels]:
            self.id4 = self.order('stock', 'test.sym.9', 'sell', 25, 'LIMIT', 52.25)
        if getattr(self, 'id7', None) in [x.uuid for x in cancels]:
            self.order('stock', 'test.sym.9', 'sell', 85, 'LIMIT', 52.5)
        if len(cancels) > 2:
            raise RuntimeError


# noinspection PyAttributeOutsideInit
class UnitTest_02(Strategy):
    """
    Unit test strategy to test:
    - initialization and startup
    - parameters
    - Replace order
    - Access to PositionManager and PnL
    """

    def on_initialize(self):
        # setup a counter on initialization
        self.barcount = None

    def on_start(self):
        # set the starting value of the barcount using parameters
        self.barcount = self.parameters['start_bar']

    def on_bar(self, bartime):
        if self.barcount == 1:
            # create an order 3 on the first bar, which is 1 because that is the parameter
            self.id_3 = self.order('stock', 'test.sym.10', 'sell', 50, 'LIMIT', 44.8)
            self.order_3 = self.orders_list()[0]
        if self.market_data_manager.current_bar('stock', 'test.sym.10', '1min') == \
                Bar(pd.Timestamp('2010-04-01 09:31:00', tz=default_time_zone), 44.4, 44.5, 44, 44, 900):
            # attempt to replace the order 3 on the next bar with a size too big, the replace will be rejected
            self.replace_order(self.order_3, 500, price=54.8)
        if self.barcount == 3:
            # originate an order that will be replaced several times later
            self.id_1 = self.order('stock', 'test.sym.9', 'buy', 100, 'LIMIT', 50.6)
            # use the orders list to get active orders for this strategy
            self.order_1 = self.orders_list()[1]
            # create an order 4
            self.id_4 = self.order('stock', 'test.sym.10', 'sell', 100, 'LIMIT', 45.5)
            self.order_4 = self.order_manager.order(self.id_4)
        if self.barcount == 4:
            # replace the order with a new price
            self.replace_order(self.order_1, price=50.8)
            # multiple replaces, the last one takes
            self.replace_order(self.order_4, 50, price=41.5)
            self.replace_order(self.order_4, 75, price=45.3)
        if self.barcount == 5:
            # replace order with new price
            self.replace_order(self.order_1, price=51.7)
        if self.barcount == 6:
            # replace order with new price
            self.replace_order(self.order_1, price=51.75)
        if self.barcount == 8:
            # this replace will be ignored because it happens after the order is filled
            self.replace_order(self.order_2, 100)
        if bartime <= pd.Timestamp('2010-01-04 09:35:00', tz=default_time_zone):
            # get the gross pnl using the access to the PositionManager
            gross_pnl = self.position_manager.get_value(self.strategy_id, 'stock', 'test.sym.10', 'gross_pnl')
            if gross_pnl:
                if gross_pnl < 19:
                    # create a new order when a PnL condition is met
                    self.id_5 = self.order('stock', 'test.sym.9', 'buy', 100, 'LIMIT', 51)
                    self.order_5 = self.order_manager.order(self.id_5)
        # increment the bar counter
        self.barcount += 1

    def on_fills(self, bartime, fills):
        for order in fills:
            if order.uuid == getattr(self, 'id_1', None):
                if not order.closed:  # is the order is only partially filled
                    # once the order 1 is filled, replace the size with 20 + fill size to make the next fill 20
                    self.replace_order(self.order_1, quantity=(20 + self.order_1.fill_quantity), price=51.5)
                    # on the first partial fill of order 1, create a new order 2 on another symbol
                    self.id_2 = self.order('stock', 'test.sym.10', 'S', 50, 'LIMIT', 44.5)
                    self.order_2 = self.order_manager.order(self.id_2)
            if order.uuid == getattr(self, 'id_3', None):
                # attempt to cancel an order after it has been filled. This cancel request will be ignored
                self.cancel_order(self.order_3)
            if order.uuid == getattr(self, 'id_5', None):
                # order 5 is partially filled, but then the new replace quantity is < partial fills, so becomes FILLED
                self.replace_order(self.order_5, 50)


# noinspection PyAttributeOutsideInit
class UnitTest_03(Strategy):
    """
    Unit test strategy to test:
    - intents
    - intent and order interaction, or lack thereof
    - position() method in strategy
    """

    def on_initialize(self):
        self.filled_orders = None
        self.barcount = None

    def on_start(self):
        # set the starting value of the barcount using parameters
        self.barcount = 1

    def on_bar(self, bartime):
        # for 3 bars keep the same intent
        if self.barcount <= 3:
            self.intent('stock', 'test.sym.9', 25)
        # modify a buy order up in quantity
        elif self.barcount == 5:
            self.intent('stock', 'test.sym.9', 10 + self.position('stock', 'test.sym.9'))
            self.qty = self.get_intent('stock', 'test.sym.9')
        elif self.barcount in [6, 7]:
            self.qty += 10
            self.intent('stock', 'test.sym.9', self.qty)
        # modify a sell order down in quantity
        elif self.barcount == 8:
            self.intent('stock', 'test.sym.9', -50 + self.position('stock', 'test.sym.9'))
            self.qty = self.get_intent('stock', 'test.sym.9')
        elif self.barcount in [9, 10]:
            self.qty += 10
            self.intent('stock', 'test.sym.9', self.qty)
        # flip sign on an intent
        elif self.barcount == 11:
            self.intent('stock', 'test.sym.9', self.position('stock', 'test.sym.9') + 10)
        elif self.barcount in [12, 13]:
            self.intent('stock', 'test.sym.9', self.position('stock', 'test.sym.9') - 10)
        elif self.barcount == 14:
            self.intent('stock', 'test.sym.9', self.position('stock', 'test.sym.9') + 5)
        elif self.barcount in [15, 16]:
            self.intent('stock', 'test.sym.9', 100)
        elif self.barcount == 17:
            self.intent('stock', 'test.sym.9', 60)
        elif self.barcount == 18:
            self.intent('stock', 'test.sym.9', 160)
            self.order_1 = self.order('stock', 'test.sym.9', 'sell', 50, 'LIMIT', 54.5)
        elif self.barcount == 19:
            self.intent('stock', 'test.sym.9', 100)
            self.order_2 = self.order('stock', 'test.sym.9', 'buy', 50, 'LIMIT', 51.5)
        elif self.barcount == 20:
            self.intent('stock', 'test.sym.9', 600)
            self.cancel_order(self.get_order(self.order_2))

        # increment the bar counter
        self.barcount += 1

    def on_fills(self, bartime, fills):
        self.filled_orders = fills

    def on_cancels(self, bartime, cancels):
        self.canceled_orders = cancels


# noinspection PyAttributeOutsideInit
class UnitTest_04(Strategy):
    """
    Unit test strategy to test:
    - market_open & market_close
    - begin_of_day & end_of_day
    - 1D frequency strategies
    """

    def on_initialize(self):
        self.daycount = None
        self.sum_of_count = None

    def on_start(self):
        self.daycount = 0
        self.sum_of_count = 0

    def on_begin_of_day(self, bartime):
        self.daycount += 1

    def on_end_of_day(self, bartime):
        self.sum_of_count += self.daycount

    def on_market_open(self, bartime):
        # skip the first bar
        if self.sum_of_count == 1:
            # create an order that will be filled
            self.order('stock', 'test.sym.9', 'buy', 50, 'LIMIT', 49.5)
            # create an order that will not be filled on this bar, so canceled
            self.order('stock', 'test.sym.9', 'sell', 50, 'LIMIT', 70.25)
        elif self.sum_of_count == 3:
            # buy and sell order that both get filled, so position flat at the end
            self.order('stock', 'test.sym.10', 'buy', 25, 'LIMIT', 46.6)
            self.order('stock', 'test.sym.10', 'sell', 25, 'LIMIT', 65.25)
        elif self.sum_of_count == 6:
            # submit an order, then immediately cancel
            ord1 = self.order('stock', 'test.sym.9', 'buy', 10, 'LIMIT', 70.5)
            self.cancel_order(self.get_order(ord1))

    def on_market_close(self, bartime):
        if self.daycount == 4:
            # try to submit order and get rejected as market closed
            self.order('stock', 'test.sym.10', 'sell', 10, 'LIMIT', 50.5)

    def on_fills(self, bartime, fills):
        self.filled_orders = fills

    def on_cancels(self, bartime, cancels):
        self.canceled_orders = cancels


# noinspection PyAttributeOutsideInit
class UnitTest_05(Strategy):
    """
    Unit test strategy to test the complete process: intents, positions overnight, metrics
    """

    def on_start(self):
        self.ewma = {}
        half_life = self.parameters['half_life']
        for symbol in self.symbol_tuples:
            self.ewma[symbol.symbol] = \
                metric.ExponentialWeightedMA(self.market_data_manager, (symbol, 'close'), half_life)

    def on_bar(self, bartime):
        for symbol in self.symbols['stock']:
            diff = self.ewma[symbol][0] - self.market_data_manager.current_bar('stock', symbol, '1min')['close']
            target = round(diff * 10, -1)
            intent = np.sign(target) * min([100, abs(target)])
            self.intent('stock', symbol, intent)
