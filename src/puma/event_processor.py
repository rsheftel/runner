"""
Event engine
"""

import logging
import uuid

from puma import order as tw_order

log = logging.getLogger(__name__)


class EventProcessor:
    """
    Event loop processor for Strategy objects
    """

    def __init__(self, strategies, portfolios, risk, order_manager, position_manager, broker, market_data_manager,
                 exchange=None):
        """
        Initialize the object with required puma objects.

        :param strategies: list of Strategy objects
        :param portfolios: list of Portfolio objects
        :param risk: Risk object
        :param order_manager: OrderManager object
        :param position_manager: PositionManager object
        :param broker: Broker object
        :param market_data_manager: MarketDataManager object
        :param exchange: None if running live, or Exchange object for simulation
        """
        log.info('initializing Strategy event processor')
        self._strategies = strategies
        self._portfolios = portfolios
        self._risk = risk
        self._order_manager = order_manager
        self._position_manager = position_manager
        self._broker = broker
        self._market_data_manager = market_data_manager
        self._exchange = exchange

    def process_cancels(self):
        """
        Process cancels by getting the CANCELED orders that are not in closed=True and call the on_cancels on the
        strategies

        :return: nothing
        """
        log.info('processing cancels')
        orders = self._order_manager.cancels_to_process()
        order_dict = tw_order.list_to_dict(orders, 'originator_id')
        for strategy in self._strategies:
            strat_id = 'strategy.' + strategy.strategy_id
            if strat_id in order_dict:
                log.info(f'calling on_cancels method on strategy: {strat_id}')
                strategy.on_cancels(self._market_data_manager.bartime, order_dict[strat_id])
        # mark all the CANCELED orders as closed
        for order in orders:
            self._order_manager.close_order(order)

    def process_fills(self):
        """
        Process fills by getting FILLED and PARTIALLY_FILLED closed orders from the OMS and first booking them in
        the position_manager, then calling the on_fills of the strategies

        :return: nothing
        """
        log.info('processing fills')
        booked_orders = self._position_manager.book_fills()
        for strategy in self._strategies:
            strat_id = 'strategy.' + strategy.strategy_id
            if strat_id in booked_orders:
                log.info(f'calling on_fills method on strategy: {strat_id}')
                strategy.on_fills(self._market_data_manager.bartime, booked_orders[strat_id])

    def check_stuck_orders(self):
        """
        Check if there are any orders stuck in an object where there should be no orders. For example if the Risk
        object has orders that have not been passed to the OrderManager, etc. If there is a stuck order will raise a
        RunTimeError and halt.

        :return: nothing
        """
        log.info("Checking for stuck orders.")
        states = tw_order.states()['open']
        states = states[:states.index('SENT')]
        for state in states:
            if self._order_manager.orders_list({'state': state}):
                raise RuntimeError(f'Stuck order in {state} state')

    def process_bar(self, product_types, frequency):
        """
        Process bar for the current bartime in the MarketDataManager. Update market data, process orders in the
        exchange, call strategy on_X methods, process the new strategy orders out to the exchange via the broker.

        :param product_types: list of product_types
        :param frequency: frequency in standard format
        :return: nothing
        """
        # update MarketDataManager data
        bartime = self._market_data_manager.bartime
        log.info(f"Processing bar for bartime: {bartime} : products : {product_types} : frequency : {frequency}")
        for product_type in product_types:
            self._market_data_manager.update(product_type, frequency)

        # calculate PnL as the strategy may want to know the PnL
        self._position_manager.update_pnl()

        # If running in simulation, have exchange process orders. In prod this is automatic
        if self._exchange:
            log.info('Exchange object exists, calling process_orders')
            self._exchange.process_orders(self._market_data_manager)

        # Get order states from broker
        self._broker.update_order_states()

        # Process cancels
        self.process_cancels()

        # process fills & recalc PnL
        self.process_fills()
        self._position_manager.update_pnl()

        # call on_bar for each strategy
        for strategy in self._strategies:
            strategy.on_bar(bartime)

        # process the strategy orders to the OMS
        for portfolio in self._portfolios:
            portfolio.process_orders()
            self._risk.process_portfolio_orders(portfolio)

        # send the orders to market
        self._broker.send_orders()

        # confirm there are no stuck orders anywhere
        self.check_stuck_orders()

    def market_open(self, product_types):
        """
        Run the market open process. The datetime is the current bartime in the MarketDataManager

        :param product_types: list of product_types
        :return: nothing
        """
        log.info('running market_open process')
        # Set the market state to open (True)
        for product_type in product_types:
            self._order_manager.market_state(product_type, True)

        # Call strategies on_market_open
        for strategy in self._strategies:
            strategy.on_market_open(self._market_data_manager.bartime)

    def market_close(self, product_types):
        """
        Run the market close process. The datetime is the current bartime in the MarketDataManager

        :param product_types: list of product_types
        :return: nothing
        """
        # Set the market state to closed (False)
        log.info('running market_close process')
        for product_type in product_types:
            self._order_manager.market_state(product_type, False)

        # If running in simulation
        if self._exchange:
            self._exchange.market_close(self._market_data_manager.bartime)

        # Get order states from broker
        self._broker.update_order_states()

        # Process cancels
        self.process_cancels()

        # Call strategy on_market_close()
        # This should not be able to create any new orders because market_state is False
        for strategy in self._strategies:
            strategy.on_market_close(self._market_data_manager.bartime)

        # confirm there are no open orders anywhere
        if self._order_manager.orders_list({'state': tw_order.states()['open']}):
            raise RuntimeError('open orders in OrderManager after market_close is run.')

    def begin_of_day(self):
        """
        Run the begin of day (BOD) processes. The date is the current bartime in the MarketDataManager

        :return: nothing
        """
        log.info("Running BOD process")
        self._position_manager.begin_of_day()

        # Call strategy on_begin_of_day
        for strategy in self._strategies:
            strategy.on_begin_of_day(self._market_data_manager.bartime)

    def end_of_day(self, product_types):
        """
        Run the end of day (EOD) processes. The date is the current bartime in the MarketDataManager

        :param product_types: list of product_types
        :return: nothing
        """
        # update MarketDataManager data
        log.info("Running EOD process")
        log.info(f"Updating 1D market data for products : {product_types} : frequency : 1D")
        for product_type in product_types:
            self._market_data_manager.extend(product_type, '1D')

        # run strategy EOD
        for strategy in self._strategies:
            strategy.on_end_of_day(self._market_data_manager.bartime)

        # run position manager EOD
        self._position_manager.end_of_day()

        # run the order manager EOD
        self._order_manager.end_of_day(self._market_data_manager.bartime)

    def stop(self):
        """
        Run the stop processes. The date is the current bartime in the MarketDataManager

        :return: nothing
        """
        # update MarketDataManager data
        log.info("Running stop process")
        # run strategy on_stop
        for strategy in self._strategies:
            strategy.on_stop(self._market_data_manager.bartime)

        log.info('Running stop processes')
        # run position manager EOD
        self._position_manager.stop()

        # run the order manager EOD
        self._order_manager.stop(self._market_data_manager.bartime)


class MetricProcessor:
    """
    Event processor for MetricContainer objects
    """

    def __init__(self, metric_containers, market_data_manager):
        """
        Initialize with the Metric and market data objects

        :param metric_containers: single MetricContainer or a list of MetricContainer objects
        :param market_data_manager: MarketDataManager object
        """
        log.info(f'Initializing MetricProcessor event looper: {self}')
        self.__uuid = str(uuid.uuid4())
        self._metric_containers = metric_containers if isinstance(metric_containers, list) else list(metric_containers)
        self._market_data_manager = market_data_manager

    @property
    def uuid(self):
        return self.__uuid

    @property
    def metric_containers(self):
        return self._metric_containers

    def start(self):
        """
        Run the start() method on all attached MetricContainers

        :return: nothing
        """
        log.info('Running start for all attached metric_containers')
        for metric_container in self._metric_containers:
            metric_container.start()

    def stop(self):
        """
        Run the stop() method on all attached MetricContainers

        :return: nothing
        """
        log.info('Running stop for all attached metric_containers')
        for metric_container in self._metric_containers:
            metric_container.stop()

    def process_bar(self, frequencies):
        """
        Process the bar by updating the market data for the supplied frequencies and then run the calculate() method on
        all attached MetricContainers

        :param frequencies: string or list of frequency strings to update market data
        :return: nothing
        """
        log.info('Updating market data')
        frequencies = frequencies if isinstance(frequencies, list) else [frequencies]
        for product_type in self._market_data_manager.product_types():
            for frequency in frequencies:
                self._market_data_manager.update(product_type, frequency)

        log.info('Running calculate for all attached metric_containers')
        for metric_container in self._metric_containers:
            metric_container.calculate()
