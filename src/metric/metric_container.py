"""
MetricContainer
"""

import logging
import os
import uuid
from abc import ABCMeta, abstractmethod
from typing import List

import raccoon as rc

import utils.pandas as pd_utils
import utils.time_series as tsutils
from data.structures import SymbolTuple
from metric.total_return_index import TotalReturnIndex

log = logging.getLogger(__name__)


class MetricContainer(metaclass=ABCMeta):
    """
    MetricContainer Abstract Base Class
    """

    def __init__(self, market_data_manager, metric, arguments: List, **kwargs):
        """
        :param market_data_manager: MarketDataManager object
        :param metric: Metric object
        :param arguments: list of arguments to pass to the metric. These are the items that are different for each
            of the underlying metrics. For example a list of SymbolTuples or a list of data frames
        :param kwargs: additional arguments that will be passed to the metric at construction. These are the same
            values used for all of the underlying metrics
        """
        self.__uuid = str(uuid.uuid4())
        log.info(f'Initializing MetricContainer: {self.__uuid} : Metric : {metric}')
        self._market_data_manager = market_data_manager
        self._metrics = rc.DataFrame(columns=['metric'], sort=False, index_name=self.index_name)
        for argument in arguments:
            log.info(f'Adding metric with argument: {str(argument)}')
            self._metrics.append_row(self.metric_index_name(argument),
                                     {'metric': self.initialize_metric(metric, argument, **kwargs)},
                                     new_cols=False)
        log.info(f'MetricContainer initialized : {self}')

    @property
    @abstractmethod
    def index_name(self):
        """
        Returns the index_name for the raccoon DataFrame. This must be consistent with the format of the
        metric_index_name

        :return: valid string or tuple
        """
        raise NotImplementedError

    @abstractmethod
    def metric_index_name(self, argument):
        """
        For a given argument return the index name for the DataFrame. Can be any valid single value or tuple for a
        raccoon DataFrame index

        :param argument: argument for metric
        :return: valid value for raccoon index
        """
        raise NotImplementedError

    @abstractmethod
    def initialize_metric(self, metric, argument, **kwargs):
        """
        This is the abstract method that must be overwritten in the concrete implementation that takes initializes
        the Metric object for a given argument. Because the parameters for the initialization of every metric is
        different this must be a unique implementation for each Metric type.

        :param metric: Metric class
        :param argument: variable argument for the metric
        :param kwargs: additional arguments for the metric
        :return: metric object
        """
        raise NotImplementedError

    @property
    def uuid(self):
        return self.__uuid

    @property
    def metrics(self):
        return self._metrics

    def start(self):
        pass

    def stop(self):
        pass

    def calculate(self, datetime=None):
        """
        Calculate all of the underlying metrics for the given datetime, or the bartime from the MarketDataManager.
        The order of evaluation will be the order they were entered but this is not assured.

        :param datetime: datetime if None then use bartime from MarketDataManager
        :return: nothing
        """
        log.info(f'Calculating metrics in container: {self.__uuid}')
        datetime = datetime or self._market_data_manager.bartime
        for metric in self._metrics.get_entire_column('metric', as_list=True):
            metric.calculate(datetime)


class SignalContainer(MetricContainer):
    """
    Metric container for signals. The argument parameter list must be in the form of [(SymbolTuple, component), ...]
    """

    @property
    def index_name(self):
        # noinspection PyProtectedMember
        return SymbolTuple._fields

    def metric_index_name(self, argument):
        """
        The index name is the SymbolTuple

        :param argument: (SymbolTuple, component) tuple
        :return: SymbolTuple
        """
        return argument[0]

    def initialize_metric(self, metric, symbol_tuple, **kwargs):
        """
        Initializes the signal metric for the symbol_tuple and adds the symbol to the MarketDataManager. It does not
        check symbols are valid SymbolDB because it is assumed to be used mostly in research.

        :param metric: Metric class
        :param symbol_tuple: tuple of (SymbolTuple, component)
        :param kwargs: any additional arguments for the Metric
        :return: Metric object
        """
        symbol = symbol_tuple[0]
        self._market_data_manager.add_symbols(product_type=symbol.product_type, frequency=symbol.frequency,
                                              symbols=[symbol.symbol], check_symbols=False)
        return metric(self._market_data_manager, symbol_tuple, **kwargs)

    def start(self):
        log.info("SignalContainer start.")

    def stop(self):
        log.info("SignalContainer stop.")


class TRIContainer(MetricContainer):
    """
    Metric container for TRI Generation. The argument parameter list must be in the form of
    [PairsTuple, ...]. Each element in the list is a ParisTuple which itself is a tuple of two SymbolComponent tuples,
    which itself is a SymbolTuple and component. The first of the PairsTuple it the dependant data (the one that is
    driven by the other) and the second is the independent data (the driver).
    """

    def __init__(self, market_data_manager, regression_metric, arguments: List, lag_bars, output_dir=None,
                 slurp_style=False, **kwargs):
        """
        :param market_data_manager: MarketDataManager object
        :param arguments: list of PairsTuple where the first symbol in each tuple is the dependent data and the second
            is the independent
        :param regression_metric: regression Metric class to use for the beta
        :param lag_bars: lag_bars for the regression_metric
        :param output_dir: If not None then the TRI output will be saved to this location. Ignored if None
        :param slurp_style: If True then will output files in the format for slurping, if False then in the format for
            reading with functions like CsvDataFeed. Only has an effect if output_dir is True
        :param kwargs: additional arguments that will be passed to the regression_metric at construction. These are the
            same values used for all of the underlying metrics
        """
        self.regression_metric = regression_metric
        self.lag_days = lag_bars
        self.output_dir = os.path.normpath(output_dir) if output_dir else output_dir
        self.slurp_style = slurp_style
        super().__init__(market_data_manager, TotalReturnIndex, arguments, **kwargs)

    @property
    def index_name(self):
        return 'dependant_data', 'independent_data'

    def metric_index_name(self, argument):
        """
        The index name is a PairsTuple of SymbolTuples

        :param argument: PairsTuple
        :return: (SymbolTuple, SymbolTuple)
        """
        return argument.dependent.symbol_tuple, argument.independent.symbol_tuple

    def initialize_metric(self, tri_metric, pairs, **kwargs):
        """
        Initializes the signal metric for the symbol_tuple and adds the symbol to the MarketDataManager.

        :param tri_metric: TRI Metric class
        :param pairs: PairsTuple
        :param kwargs: any additional arguments for the regression_metric
        :return: Metric object
        """
        for symbol_tuple in pairs:
            symbol = symbol_tuple.symbol_tuple
            self._market_data_manager.add_symbols(product_type=symbol.product_type, frequency=symbol.frequency,
                                                  symbols=[symbol.symbol])
        return tri_metric(self._market_data_manager, pairs.dependent, pairs.independent, self.regression_metric,
                          self.lag_days, **kwargs)

    def start(self):
        log.info(f"TRIContainer start: {self}")

    def stop(self):
        """
        When the stop() is called will output to csv files the DataFrame of the TRI for each pair. This is ignored if
        output_dir=None. The filename is dependant_independent_TRI. Inside the DF has column name 'close' so that it
        may be consumed by other functions.

        :return: nothing
        """
        log.info(f"TRIContainer stop: {self}")
        if self.output_dir:
            filenames_symboldb = []
            filenames_tsdb = []
            for row in self.metrics.iterrows():
                index = row[self.index_name]
                ticker = index[0].symbol + '_' + index[1].symbol + '_TRI'  # construct the ticker
                frequency = index[0].frequency  # use the frequency of the index[0] for output

                # TRI data
                tri_data = row['metric'].data
                tri_df = pd_utils.rc_to_pd(tri_data)  # put the data into pandas DF and relabel properly
                tri_df.columns = ['close']

                # regressor data
                reg_df = pd_utils.namedtuple_to_df(row['metric'].regressor.data)
                reg_df = pd_utils.rc_to_pd(reg_df)

                if self.slurp_style:
                    source = self._market_data_manager.source('live')  # Use the LDM for the source
                    symbol_file = tsutils.symbol_to_csv(tri_df, self.output_dir, ticker, 'index', frequency, source,
                                                        slurp_style=True)
                    column_names = [ticker + '_' + 'index' + '_' + frequency + '_' + x + ':' + source
                                    for x in reg_df.columns]
                    reg_df.columns = column_names
                    tsdb_file = tsutils.tsdb_to_csv(reg_df, self.output_dir, ticker, 'index', frequency, source=source,
                                                    slurp_style=True)
                else:
                    symbol_file = tsutils.symbol_to_csv(tri_df, self.output_dir, ticker, 'index', frequency)
                    tsdb_file = tsutils.tsdb_to_csv(reg_df, self.output_dir, ticker, 'index', frequency, 'regression')
                filenames_symboldb.append(symbol_file)
                filenames_tsdb.append(tsdb_file)

            # add filenames to DataFrame
            self.metrics['filename_symboldb'] = filenames_symboldb
            self.metrics['filename_tsdb'] = filenames_tsdb
