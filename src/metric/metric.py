"""
Metric class. This is the Abstract Base Class for all metric class implementations.
"""

import logging
import uuid
from abc import ABCMeta, abstractmethod

import pandas as pd
import raccoon as rc

from data.structures import SymbolTuple

log = logging.getLogger(__name__)


class Metric(metaclass=ABCMeta):
    """
    The abstract base class for all metric classes.
    """

    def __init__(self, market_data_manager):
        """
        Concrete implementations should have an __init__ method and call this via Super().__init__(mdm)
        :param market_data_manager: MarketDataManager object
        """

        self.__uuid = str(uuid.uuid4())
        self._mdm = market_data_manager
        self._data = rc.Series(data_name='value', index_name='datetime', sort=True)
        log.info(f'Metric initialized : {self}')

    @property
    def uuid(self):
        return self.__uuid

    @property
    def data(self):
        return self._data

    def series_wrap(self, object_in):
        """
        Returns either a ViewSeries or Metric from input based on what the input is

        :param object_in: either Metric, Series, ViewSeries, (DataFrame, column) or (SymbolTuple, component)
        :return: Metric or ViewSeries
        """
        if isinstance(object_in, Metric):
            return object_in
        elif isinstance(object_in, rc.ViewSeries):
            return object_in
        elif isinstance(object_in, rc.Series):
            return rc.ViewSeries.from_series(object_in, offset=1)
        elif isinstance(object_in, tuple):
            structure = object_in[0]
            column = object_in[1]
            if isinstance(structure, SymbolTuple):
                bars = self._mdm.view(structure.product_type, structure.symbol, structure.frequency)
                return rc.ViewSeries.from_dataframe(bars, column, offset=1)
            elif isinstance(structure, rc.DataFrame):
                return rc.ViewSeries.from_dataframe(structure, column, offset=1)
        raise ValueError('not valid object_in')

    @abstractmethod
    def _calculate(self, datetime):
        """
        Calculates the metric value. All concrete implementations must have this method defined and return a single
        value.

        :param datetime: datetime
        :return: value
        """
        raise NotImplementedError

    def calculate(self, datetime, force_recalc=False):
        """
        The main calculation method. This will determine if the value for a datetime needs to be calculated and if so
        will calculate, or if force_recalc is True will recalc the metric for that datetime regardless.

        This method calls the concrete implementation of _calculate() where the metric specific calc is performed.

        :param datetime: datetime
        :param force_recalc: if True then recalc the metric even if it already exists
        :return: nothing
        """
        log.info(f'Calculating metric - {self} - {self.__uuid}')
        if (len(self._data) == 0) \
                or ((len(self._data) > 0) and (datetime > self._data.index[-1])) \
                or force_recalc:
            self._data[datetime] = self._calculate(datetime)

    def _value(self, index):
        """
        Returns either a single value or list of values based on the index argument. Importantly this method does NOT
        perform a calculation. This method is intended to be used in the _calculation() method when getting values
        for the metric itself.

        Usage...
        _value(-1)  - get value at -1
        _value((-5, -1)) - get list of values from -5 to -1 inclusive

        :param index: integer index, datetime, integer slice or datetime slice
        :return: value or list of values
        """
        if isinstance(index, int):
            if index > 0:
                raise IndexError('Index must be <= 0')
            if self._data.index[-1] == self._mdm.bartime:  # if it is a recalc offset the index
                index -= 1
            return self._data.data[index]
        elif isinstance(index, pd.Timestamp):
            return self._data[index]
        elif isinstance(index, tuple):
            recalc = self._data.index[-1] == self._mdm.bartime  # if it is a recalc offset the index
            start = index[0] - (1 if recalc else 0)
            stop = index[1] - (1 if recalc else 0)
            if stop >= -1:
                return self._data.data[start:]
            else:
                return self._data.data[start:stop]
        else:
            raise ValueError('Index not a valid type.')

    def value(self, index):
        """
        Returns either a single value or list of values based on the x argument. Performs lazy calculation of the
        metric.

        Usage...
        value(5)  -- get value at location=5
        value(pd.Timestamp('2010-01-01 09:30', tz=NYC')) -- get value for datetime\
        value(slice(-5, 0))  -- get locations at slices

        :param index: integer index, datetime, integer slice or datetime slice
        :return: value or list of values
        """
        self.calculate(self._mdm.bartime)

        if isinstance(index, int):
            if index > 0:
                raise IndexError('Index must be <= 0')
            return self._data.data[index - 1]

        elif isinstance(index, pd.Timestamp):
            return self._data[index]

        elif isinstance(index, slice):
            start = index.start - 1
            if index.stop == 0:
                return self._data.data[start:]
            else:
                return self._data.data[start:index.stop]

        else:
            raise ValueError('Index not a valid type.')

    def __getitem__(self, index):
        """
        Convenience wrapper around the value() method for using metric[].

        :param index: any of the parameters above
        :return: DataFrame of the subset slice
        """
        return self.value(index)
