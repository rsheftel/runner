"""
The outermost class that runs the entire system
"""

import importlib
import logging
from abc import ABCMeta, abstractmethod
from collections import OrderedDict

import pandas as pd

import database.utils as dbutils
import data.datetime as mdatetime
import puma as tw
import data.data_manager as dutils
import utils.pandas as pdutils
from utils.datetime import default_time_zone
from database import tapdb, strategydb, metadb

log = logging.getLogger(__name__)


class RunnerBase(metaclass=ABCMeta):
    """
    Runner Abstract Base Class
    """

    def __init__(self, host, runner_id=None):
        """
        Initialization method.

        :param runner_id: unique ID for the runner that will be saved in logs and files
        :param host: database host
        """
        log.info(f"initializing Runner: {self}")
        self._runner_id = runner_id
        self._db_host = host

        # Holding objects for future initialization
        self._time_zone = None
        self._market_data_manager = None
        self._event_looper = None

    @property
    def id(self):
        return self._runner_id

    def setup_market_data(self, data_feed="CsvDataFeed", time_zone=None, **kwargs):
        """
        Sets up the market data by initializing the DataFeed, LiveDataManager, HistoricalDataManager and
        MarketDataManager. To use this MarketDataManager in other processes request a pointer with market_data_manager()

        :param data_feed: data_feed class name
        :param time_zone: time zone for the market data and the event loop datetimes. If None use default
        :param kwargs: additional arguments required for the data_feed
        :return: nothing
        """
        log.info(f"setting up market data for data_feed: {data_feed}")
        time_zone = default_time_zone if time_zone is None else time_zone
        kwargs["time_zone"] = time_zone
        self._market_data_manager = dutils.market_data_manager(data_feed, self._db_host, **kwargs)
        self._time_zone = time_zone

    @property
    def time_zone(self):
        return self._time_zone

    @property
    def market_data_manager(self):
        return self._market_data_manager

    def product_types(self):
        """
        List of product types that are registered in the market data manager

        :return: list of product_types
        """
        return self._market_data_manager.product_types()

    def frequencies(self):
        """
        Returns a list of frequencies that are registered in the market data manager

        :return: list of frequencies in string form
        """
        frequencies = []
        for product in self._market_data_manager.product_types():
            frequencies.extend(self._market_data_manager.frequencies(product))
        return frequencies

    def min_frequency(self):
        """
        Get the shortest frequency. Used to determine the increment of the event loop.

        :return: minimum frequency as string
        """
        frequencies = self.frequencies()
        frequencies = [pd.Timedelta(x) for x in frequencies]
        return pdutils.timedelta_to_str(min(frequencies))

    def bartimes(self, start_datetime, end_datetime, include_open=True, default_close=False):
        """
        Given a start and end DateTime will return an iterator of DateTimes at the min frequency. Uses the
        pandas_market_calendars to incorporate the trading day times so that the proper times are returned
        and dates are adjusted for holidays. If default_close is True then uses the default close time for 1D data and
        ignores the early close time in the pandas_market_calendars.

        :param start_datetime: start DateTime
        :param end_datetime: end DateTime
        :param include_open: if True then include the open time as a bar
        :param default_close: if True then use the default close time for 1D data, if False use actual close time
        :return: iterator of DateTimes
        """
        return mdatetime.bartimes(
            self.product_types(), self.min_frequency(), start_datetime, end_datetime, include_open, default_close
        )

    @abstractmethod
    def run(self, bartimes):
        """
        The main method of the class that runs the Runner over the bartimes

        :param bartimes: iterator of pandas Timestamps
        :return: nothing
        """
        raise NotImplementedError


class SimRunner(RunnerBase):
    """
    Strategy Simulation Runner class
    """

    def __init__(self, host, runner_id=None):
        """
        Initialization method.

        :param runner_id: unique ID for the runner that will be saved in logs and files
        :param host: database host
        """
        id_name = runner_id if runner_id is not None else "simulation"
        super().__init__(host, id_name)

        # setup TAPDB
        self._tapdb = self._setup_tapdb(id_name)

        # Setup required objects
        self._order_manager = tw.order_manager.OrderManager(id_name, self._tapdb)
        self._risk = tw.Risk(self._order_manager)
        self._position_manager = tw.PositionManager(id_name, self._order_manager, self._tapdb)

        # setup simulation exchange and paper broker
        self._exchange = tw.exchange.PaperExchange()
        self._broker = tw.PaperBroker("paper_broker", self._order_manager, self._exchange)

        # Holding objects for future initialization
        self._live_frequency = None
        self._strategies = OrderedDict()
        self._portfolios = OrderedDict()
        self._product_types = []

    def exit(self):
        if hasattr(self, "_tapdb"):
            self._tapdb.dispose()

    def __del__(self):
        self.exit()

    @property
    def portfolios(self):
        return self._portfolios

    @property
    def risk(self):
        return self._risk

    @property
    def order_manager(self):
        return self._order_manager

    @property
    def position_manager(self):
        return self._position_manager

    @property
    def tapdb_engine(self):
        return self._tapdb

    def _setup_tapdb(self, name):
        """
        Setup the tapdb for runner

        :param name: name of the position manager source
        :return: sqlalchemy engine for TAPDB
        """
        # get production databases
        prod_tapdb = tapdb.engine(self._db_host)
        prod_strategydb = strategydb.engine(self._db_host)
        prod_stockdb = metadb.engine(self._db_host, "stock")

        # create the DB for this run
        runner_db_name = f"tapdb_{self._runner_id}"
        dbutils.delete_db(self._db_host, runner_db_name)
        dbutils.create_db(self._db_host, runner_db_name)
        runner_tapdb = dbutils.make_engine(runner_db_name, host=self._db_host)

        # reflect the schema
        dbutils.copy_table_schema(prod_tapdb, runner_tapdb)

        # attach the other DBs
        dbutils.attach_schema(runner_tapdb, "strategy", self._db_host)
        dbutils.attach_schema(runner_tapdb, "stock", self._db_host)

        # copy the data
        dbutils.copy_table_data(prod_tapdb, runner_tapdb, include_tables=["source"])

        # dispose of unneeded engines
        prod_stockdb.dispose()
        prod_strategydb.dispose()
        prod_tapdb.dispose()

        if not dbutils.name_exists(runner_tapdb, "source", name):
            dbutils.upload_name(runner_tapdb, "source", name)
        return runner_tapdb

    def setup_market_data(self, data_feed="CsvDataFeed", live_frequency="1min", time_zone=None, **kwargs):
        """
        Sets up the market data by initializing the DataFeed, LiveDataManager, HistoricalDataManager and
        MarketDataManager. To use this MarketDataManager in other processes request a pointer with market_data_manager()

        :param data_feed: data_feed class name
        :param live_frequency: the frequency for live data for PositionManager and Portfolio
        :param time_zone: time zone for the market data and the event loop datetimes. If None use default
        :param kwargs: additional arguments required for the data_feed
        :return: nothing
        """
        super().setup_market_data(data_feed, time_zone, **kwargs)
        log.info(f"setting frequency on position_manager: {live_frequency}")
        self._position_manager.setup_market_data(self._market_data_manager, live_frequency=live_frequency)

        log.info(f"setting frequency on exchange: {live_frequency}")
        self._exchange.live_frequency = live_frequency
        self._live_frequency = live_frequency

    def add_eod_metrics(self, metrics):
        """
        Add an OrderedDict of end of day metrics. The any metric that is dependant on another metric must be after,
        to the right, of the metric it depends on. Metrics are added in order.

        :param metrics: OrderedDict of metric_id: Metric object
        :return: nothing
        """
        for metric in metrics:
            self._position_manager.add_eod_metric(metrics[metric], metric)

    def add_portfolio(self, portfolio_id):
        """
        Add a portfolio to the runner.

        :param portfolio_id: portfolio ID
        :return: nothing
        """
        if portfolio_id not in self._portfolios:
            portfolio = tw.Portfolio(portfolio_id, self._order_manager, self._position_manager)
            self._portfolios[portfolio_id] = portfolio
            portfolio.setup_market_data(self._market_data_manager, live_frequency=self._live_frequency)

    def add_strategy(self, module_name, class_name, strategy_id, portfolio_id):
        """
        Add a strategy to the runner.

        :param module_name: Module name for the strategy class
        :param class_name: Strategy class name
        :param strategy_id: strategy_id
        :param portfolio_id: portfolio id to attach the strategy to
        :return: nothing
        """
        log.info(f"adding strategy: {(module_name + '.' + class_name)}")
        strategy_module = importlib.import_module(module_name)
        strategy = getattr(strategy_module, class_name)(strategy_id, self)
        self._strategies[strategy_id] = strategy
        self.portfolios[portfolio_id].add_strategy(strategy)  # Must attach the strategy to the Portfolio object

    def add_strategies(self, strategies):
        """
        Add a DataFrame of strategies to the runner. The DataFrame needs the columns: module_name, class_name,
        strategy_id, portfolio_id

        :param strategies: DataFrame of strategies. Can be either pandas or raccoon
        :return: nothing
        """
        if not self._market_data_manager:
            raise RuntimeError("Must setup market data before adding strategies.")

        for row in strategies.itertuples(index=False):
            self.add_portfolio(row.portfolio_id)
            self.add_strategy(row.module_name, row.class_name, row.strategy_id, row.portfolio_id)

    @property
    def strategies(self):
        return self._strategies

    def add_symbols(self, symbols_df):
        """
        Adds symbols to strategies. This is required so that the strategies can register with the MarketDataManager.

        :param symbols_df: DataFrame with columns (strategy_id, product_type, symbol_name, frequency)
        :return: nothing
        """
        log.info("adding symbols to strategies.")
        symbols_df = symbols_df[["strategy_id", "product_type", "symbol_name", "frequency"]]
        for strategy_id, product_type, symbol_name, frequency in symbols_df.itertuples(index=False):
            self._strategies[strategy_id].add_symbol(product_type, symbol_name, frequency)

    def product_types(self):
        """
        List of product types that the strategies have registered for.

        :return: list of product_types
        """
        product_types = set()
        for strategy in self._strategies.values():
            product_types = product_types.union(strategy.product_types)
        return list(product_types)

    def frequencies(self):
        """
        Returns a list of frequencies that have been registered by the strategies.

        :return: list of frequencies in string form
        """
        frequencies = set()
        for strategy in self._strategies.values():
            frequencies = frequencies.union(strategy.frequencies)
        return list(frequencies)

    def set_parameters(self, parameters):
        """
        Set the parameters for the attached strategies.

        :param parameters: dict of parameter dicts. Outer dict keys are strategy IDs, the inner dict are the parameters
        :return: nothing
        """
        for strategy_id in parameters:
            self.strategies[strategy_id].set_parameters(parameters[strategy_id])

    def run(self, bartimes):
        """
        The main method of the class that runs the Runner over the bartimes

        :param bartimes: iterator of pandas Timestamps
        :return: nothing
        """
        self._event_looper = tw.EventProcessor(
            list(self._strategies.values()),
            list(self._portfolios.values()),
            self._risk,
            self._order_manager,
            self._position_manager,
            self._broker,
            self._market_data_manager,
            self._exchange,
        )

        frequency = self.min_frequency()
        log.info("starting strategies.")
        for strategy in self.strategies.values():
            log.info(f"starting strategy: {strategy.strategy_id}")
            strategy.start()

        product_types = self.product_types()
        log.info("beginning run from {} to {} at frequency {}".format(bartimes[0], bartimes[-1], frequency))
        prior_bar = None
        for bartime in bartimes:
            log.info(f"running bar: {bartime}")
            if prior_bar is None:  # first bar in the run
                self._market_data_manager.bartime = bartime
                self._event_looper.begin_of_day()
                self._event_looper.market_open(product_types)
            elif bartime.date() > prior_bar.date():  # first bar of a new day
                self._event_looper.market_close(product_types)
                self._event_looper.end_of_day(product_types)
                self._market_data_manager.bartime = bartime
                self._event_looper.begin_of_day()
                self._event_looper.market_open(product_types)
            else:
                self._market_data_manager.bartime = bartime
            self._event_looper.process_bar(product_types, frequency)
            prior_bar = bartime
        # After all bars have been run execute stop
        self._event_looper.stop()
