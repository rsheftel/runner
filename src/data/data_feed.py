"""
Data feeds including Abstract Base Class and the concrete implementations
"""

import logging
import numpy as np
import os
from abc import ABCMeta, abstractmethod

import database.components as complib
import utils.pandas as pdutils
from utils.datetime import DAY, MIDNIGHT, NANOSECOND, default_time_zone
from utils.collections import sorted_in

log = logging.getLogger(__name__)


class DataFeed(metaclass=ABCMeta):
    def __init__(self, source, feed_name, time_zone):
        """
        Abstract base class for DataFeed

        :param source: source name to apply
        :param feed_name: name of the concrete implementation of DataFeed
        :param time_zone: time zone to convert the observations data to
        """
        self._source = source
        self._feed_name = feed_name
        self._time_zone = time_zone
        self._bar_data = {}

    @property
    def source(self):
        return self._source

    @property
    def feed(self):
        return self._feed_name

    @property
    def time_zone(self):
        return self._time_zone

    @abstractmethod
    def load_data(self, product_type, symbol, frequency):
        """
        Loads the data from into memory.

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard form
        :return: nothing
        """
        raise NotImplementedError

    def add_data(self, data, product_type, symbol, frequency):
        """
        Adds data from data provider into internal structure for later retrieval with bar or bars.

        :param data: DataFrame in standard format
        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard format
        :return: nothing
        """
        # replace NaN with None
        standard_components = complib.standard_components(product_type)
        for component in standard_components:
            if component not in data.columns:
                data[component] = None
        if product_type not in self._bar_data:
            self._bar_data[product_type] = {}
        if frequency not in self._bar_data[product_type]:
            self._bar_data[product_type][frequency] = {}
        self._bar_data[product_type][frequency][symbol] = pdutils.pd_to_rc(data[standard_components], sort=True)

    def lazy_load(self, product_type, symbol, frequency):
        """
        If the data has not been loaded yet, load the data. If the data is already loaded then do nothing.

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :return: nothing
        """
        # check if the data has been loaded, if not then load
        exists = False
        if (
            product_type in self._bar_data
            and frequency in self._bar_data[product_type]
            and symbol in self._bar_data[product_type][frequency]
        ):
            exists = True
        if not exists:
            self.load_data(product_type, symbol, frequency)

    def bars(self, product_type, symbol, frequency, start_datetime, end_datetime):
        """
        Returns DataFrame the symbol over the given date range. Will do lazy loading of the data from the source if
        it has not already been loaded. If it has been loaded will not reload, so no refresh.

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard format
        :param start_datetime: start datetime in pandas Timestamp
        :param end_datetime: end datetime in pandas Timestamp
        :return: raccoon DataFrame
        """
        self.lazy_load(product_type, symbol, frequency)

        if end_datetime.time() == MIDNIGHT:
            end_datetime = end_datetime + DAY - NANOSECOND
        return self._bar_data[product_type][frequency][symbol].get_slice(start_datetime, end_datetime)

    def bar(self, product_type, symbol, frequency, datetime):
        """
        Returns a single Bar dict for the symbol for the datetime. If the datetime get no data from the DataFeed then
        it will return a bar with the datetime and all components of the bar set to None.

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard format
        :param datetime: datetime in pandas Timestamp
        :return: Bar dict
        """
        self.lazy_load(product_type, symbol, frequency)
        bar_data = self._bar_data[product_type][frequency][symbol]
        if sorted_in(bar_data.index, datetime):
            return bar_data.get_columns(datetime, as_dict=True)
        else:  # if the bar is not in the data, return a None bar
            standard_components = complib.standard_components(product_type)
            bar = {'datetime': datetime}
            bar.update({k: None for k in standard_components})
            return bar


class CsvDataFeed(DataFeed):
    def __init__(self, directory, source_name='csv', time_zone=None):
        """
        DataFeed concrete implementation for CSV fies

        :param directory: directory location of the csv files
        :param source_name: source name to apply
        :param time_zone: time zone to convert the observations data to. If None use default.
        """
        time_zone = default_time_zone if time_zone is None else time_zone
        super().__init__(source_name, 'csv', time_zone)
        self._directory = directory

    def load_data(self, product_type, symbol, frequency):
        """
        Loads the data from the csv file into memory. The filename is self.directory + symbol + '.csv'

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard form
        :return: nothing
        """
        filename = os.path.join(self._directory, symbol + '_' + product_type + '_' + frequency + '.csv')
        log.info(f'CsvDataFeed: reading file: {filename}')
        data = pdutils.read_csv_time_series(filename, 'datetime', pdutils.strict_parser)
        data = data.replace(np.nan, None)
        data.index.name = 'datetime'
        self.add_data(data, product_type, symbol, frequency)
