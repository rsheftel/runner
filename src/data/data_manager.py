import logging
import uuid
from abc import ABCMeta
from data.structures import Bar
from database import symboldb
import utils.datetime as dtutils
from utils.datetime import NANOSECOND, default_time_zone
import raccoon as rc
import database.components as complib


log = logging.getLogger(__name__)


class MarketDataManager:
    """
    MarketDataManager if the top level class for market data and is the sole recommended access point to the information
    in the classes underneath (DataManager and DataFeed).
    """

    def __init__(self, historical_data_manager, live_data_manager, time_zone=None):
        """

        :param historical_data_manager: HistoricalDataManager object
        :param live_data_manager: LiveDataManager object
        :param time_zone: time zone for the data. If None then will use default
        """
        self.__uuid = str(uuid.uuid4())
        self._hdm = historical_data_manager
        self._ldm = live_data_manager

        self._time_zone = default_time_zone if time_zone is None else time_zone

        self._start_live_bartime = None
        self._current_bartime = None
        self._bar_data = {}

    @property
    def time_zone(self):
        return self._time_zone

    def _validate_time_zones(self):
        """
        Confirm that the time zone in the MarketDataManager matches the time zone in the underlying DataFeed objects
        if they exist. If there is not a match then raise an error. If the underlying DataManager or DataFeed objects
        are None then assume this is in unit testing and do not check time zone.

        :return: nothing
        """
        for manager in [self._hdm, self._ldm]:
            if manager and manager.data_feed:  # to support unit tests that do not have data manager or feeds
                if manager.data_feed.time_zone != self._time_zone:
                    raise AttributeError('Time zone of the market data manager does not match data feed:', manager)

    def database_info(self):
        """
        Returns the database info (username, password, host) for either of the underlying Data Managers

        :return: dictionary of database info
        """
        if self._hdm:
            db_info = self._hdm.db_info
        elif self._ldm:
            db_info = self._ldm.db_info
        else:
            raise RuntimeError('Either live or historical data_feed must be attached')
        return db_info

    def source(self, data_manager: str) -> str:
        """
        Returns the source for the given data manager name

        :param data_manager: either 'live' or 'historical'
        :return: source name
        """
        if data_manager == 'live':
            return self._ldm.data_feed.source
        elif data_manager == 'historical':
            return self._hdm.data_feed.source
        else:
            raise ValueError('Only valid values for data_manager are {live, historical}')

    def check_symbols(self, product_type, symbols):
        """
        Checks all symbols of a given product type to make sure they are in the database. If any symbol is not this
        method will raise an error.

        :param product_type: single product type
        :param symbols: list of symbols
        :return: nothing
        """
        db_info = self.database_info()

        engine = symboldb.symbol_engine(product_type, db_info['username'], db_info['password'], db_info['db_host'])
        db_symbols = symboldb.get_symbols(engine)
        for symbol in symbols:
            if symbol not in db_symbols:
                raise AttributeError(f'cannot add symbol, not in database symbol table: {symbol}')

    def add_symbols(self, product_type, symbols, frequency):
        """
        Add symbol(s) to the MarketDataManager

        :param product_type: product type. only one product_type allowed for all symbols per function call
        :param symbols: either a single symbol or list of symbols
        :param frequency: frequency. only one frequency type per function call
        :return: nothing
        """
        if product_type not in self._bar_data:
            self._bar_data[product_type] = {}

        if frequency not in self._bar_data[product_type]:
            self._bar_data[product_type][frequency] = {}

        symbols = [symbols] if isinstance(symbols, str) else symbols
        for symbol in symbols:
            if symbol not in self._bar_data[product_type][frequency]:
                self._bar_data[product_type][frequency][symbol] = \
                    rc.DataFrame(index_name='datetime', sort=True, columns=complib.standard_components(product_type))
                log.info(f"adding new symbol: {symbol}")

    def product_types(self):
        """
        Returns a list of product types that are currently loaded
        :return: list of product types
        """
        return list(self._bar_data.keys())

    def frequencies(self, product_type):
        """
        Returns a list of frequencies for a given product type

        :param product_type: product type
        :return: list of frequencies in standard string format
        """
        if product_type in self._bar_data:
            return list(self._bar_data[product_type].keys())
        return []

    def symbols(self, product_type, frequency):
        """
        Returns a list of symbols for the given product_type and frequency

        :param product_type: product type
        :param frequency: frequency in standard form
        :return: list of symbols
        """
        if product_type in self._bar_data and frequency in self._bar_data[product_type]:
            return list(self._bar_data[product_type][frequency].keys())
        return []

    def _get_symbols(self, product_type, symbols, frequency):
        if symbols:  # if symbols argument was passed
            symbols = [symbols] if isinstance(symbols, str) else symbols
        else:  # if not then set to all symbols
            symbols = self.symbols(product_type, frequency)
        return symbols

    @property
    def bartime(self):
        return self._current_bartime

    @bartime.setter
    def bartime(self, datetime):
        """
        Set the current bartime. The bartime can only increment to a later time than the current

        :param datetime: datetime in any form that can convert to pandas Timestamp
        :return: nothing
        """
        datetime_ts = dtutils.set_datetime(datetime, self._time_zone, self._time_zone)
        if self._current_bartime and datetime_ts <= self._current_bartime:
            raise AttributeError(f'datetime : {datetime} : is not later than current bartime: '
                                 f'{self._current_bartime}')
        self._current_bartime = datetime_ts

        # if this is the first setting of the current bartime, then save that as the start of live
        self._start_live_bartime = datetime_ts if not self._start_live_bartime else self._start_live_bartime

    def update(self, product_type, frequency, symbols=None):
        """
        Main method to update the live information for symbols. Given the product_type, frequency and a list of symbols
        this will call the underlying LiveDataManager and snapshot the data for each symbol for the current bartime
        and add that Bar() data to the data structure.

        :param product_type: product type
        :param frequency: frequency in standard form
        :param symbols: a single symbol, a list of symbols, or if None then this will update all symbols that have been
                        added with the add_symbols() method
        :return: nothing
        """
        # if the last datetime is not bartime, then load from live_data_manager (LiveDataManager)
        log.info('updating symbols')
        symbols = self._get_symbols(product_type, symbols, frequency)
        bartime = self.bartime
        for symbol in symbols:
            # if there is data already in the bar_data structure
            if self._bar_data[product_type][frequency][symbol]:
                max_bartime = self._bar_data[product_type][frequency][symbol].index[-1]
                # if the new bartime is newer then append
                if bartime > max_bartime:
                    bar = self._ldm.bar(product_type, symbol, frequency, bartime)
                    datetime = bar.pop('datetime')
                    self._bar_data[product_type][frequency][symbol].append_row(datetime, bar)
                # else if the bartime is the same, overwrite
                elif bartime == max_bartime:
                    bar = self._ldm.bar(product_type, symbol, frequency, bartime)
                    bar.pop('datetime')
                    self._bar_data[product_type][frequency][symbol].set_location(-1, bar, missing_to_none=True)
                else:
                    raise RuntimeError('bartime is earlier than last datetime in the bars data.')
            # if this is the first bar in the list
            else:
                bar = self._ldm.bar(product_type, symbol, frequency, bartime)
                datetime = bar.pop('datetime')
                self._bar_data[product_type][frequency][symbol].append_row(datetime, bar)

    def load_history(self, product_type, frequency, symbols=None, start_datetime=None):
        """
        Add historical Bar data for the symbols by calling the underlying HistoricalDataManager. This method will load
        all the data between the start_datetime and the current bartime at the time of the method call. This method
        will work prior to any live updates or after there are existing bar updates. If there is already live data
        then the historical data will load up to prior to the first Bar datetime that already exists, in other words
        the historical will NOT overwrite the current data.

        :param product_type: product type
        :param frequency: frequency in standard form
        :param symbols: symbol or list of symbols, if None then all symbols
        :param start_datetime: pandas Timestamp of datetime
        :return: nothing
        """
        start_datetime = dtutils.set_datetime(start_datetime, self._time_zone, self._time_zone)
        symbols = self._get_symbols(product_type, symbols, frequency)
        for symbol in symbols:
            log.info(f'loading historical bars for: {symbol}')
            current_bars = self.bars(product_type, symbol, frequency)
            # if there is something currently in the bars
            if current_bars:
                # set the end_datetime to one smallest unit prior to the first datetime in the current bars
                end_datetime = current_bars.index[0] - NANOSECOND
                historical_bars = self._hdm.bars(product_type, symbol, frequency, start_datetime, end_datetime)
                historical_bars.append(current_bars)
                self._bar_data[product_type][frequency][symbol] = historical_bars
            # if there are no data for the symbol, then just load from history
            else:
                if not self.bartime:
                    raise ValueError('cannot update history if the current bartime is not set.')
                if not start_datetime:
                    raise AttributeError('start_datetime must be a valid pandas Timestamp, cannot be None.')
                # set the end date to one unit prior to the current bartime
                end_datetime = self.bartime - NANOSECOND
                self._bar_data[product_type][frequency][symbol] = self._hdm.bars(product_type, symbol, frequency,
                                                                                 start_datetime, end_datetime)

    def extend(self, product_type, frequency, symbols=None):
        """
        Extends the bar data from the previous last bar to the current bar time and fills in any data between the
        previous last bar and the current bartime with historical data. It acts as a combination of update and
        load_history.
        Add historical Bar data for the symbols by calling the underlying HistoricalDataManager. This method will load
        all the data between the last datetime in the current bar data and and the current bartime. Then calls the
        update method to get the most recent Live data, but importantly will only add the live data if it is not empty,
        otherwise it is ignored.
        If there is no existing data then will only call the update for the live data.

        :param product_type: product type
        :param frequency: frequency in standard form
        :param symbols: symbol or list of symbols, if None then all symbols
        :return: nothing
        """
        symbols = self._get_symbols(product_type, symbols, frequency)
        bartime = self.bartime
        for symbol in symbols:
            log.info(f'loading historical bars for: {symbol}')
            current_bars = self._bar_data[product_type][frequency][symbol]
            # if there is something currently in the bars
            if current_bars:
                # only add history if the bartime is beyond the current last bar
                if bartime > current_bars.index[-1]:
                    # set the start_datetime to one smallest unit after the last datetime in the current bars
                    start_datetime = current_bars.index[-1] + NANOSECOND
                    historical_bars = self._hdm.bars(product_type, symbol, frequency, start_datetime, self.bartime)
                    current_bars.append(historical_bars)
            log.info(f'adding current bar for: {symbol}')
            live_bar = self._ldm.bar(product_type, symbol, frequency, bartime)
            datetime = live_bar.pop('datetime')
            if live_bar['close'] is not None:
                # first bar in the set
                if not current_bars:
                    self._bar_data[product_type][frequency][symbol].append_row(datetime, live_bar)
                elif bartime > current_bars.index[-1]:
                    self._bar_data[product_type][frequency][symbol].append_row(datetime, live_bar)
                # else if the bartime is the same, overwrite
                elif bartime == current_bars.index[-1]:
                    current_bars.set_location(-1, live_bar, missing_to_none=True)
                else:
                    raise ValueError('bartime cannot be before existing last bar.')

    @property
    def bar_data(self):
        return self._bar_data

    def view(self, product_type, symbol, frequency):
        """
        Returns a view of the underlying data structure. Be careful not to modify or will corrupt the object.

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :return: DataFrame
        """
        return self._bar_data[product_type][frequency][symbol]

    def bars(self, product_type, symbol, frequency, start_datetime=None, end_datetime=None):
        """
        For a given symbol return a raccoon DataFrame of the bars

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :param start_datetime: start datetime as pandas Timestamp
        :param end_datetime: end datetime as pandas Timestamp
        :return: raccoon DataFrame
        """
        return self._bar_data[product_type][frequency][symbol].get_slice(start_datetime, end_datetime)

    def current_bar(self, product_type, symbol, frequency):
        """
        Return the Bar dict for the current datetime

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :return: Bar dict
        """
        return Bar(**self._bar_data[product_type][frequency][symbol].get_location(-1, as_dict=True))

    def bar(self, product_type, symbol, frequency, datetime=None):
        """
        Return the Bar dict for a specific datetime

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :param datetime: specific datetime as pandas Timestamp, if None then the current datetime is returned
        :return: Bar dict
        """
        if datetime is None:
            return self.current_bar(product_type, symbol, frequency)
        else:
            return Bar(**self._bar_data[product_type][frequency][symbol].get_columns(datetime, as_dict=True))

    def last_valid_bar(self, product_type, symbol, frequency):
        """
        Returns the last Bar dict where the close is not None

        :param product_type: product type
        :param symbol: symbol
        :param frequency: frequency
        :return: Bar dict
        """
        bars = self.view(product_type, symbol, frequency)
        for i in range(len(bars)):
            bar = bars.get_location(-(i + 1), as_dict=True)
            if bar['close'] is not None:
                return Bar(**bar)
        return None


class DataManager(metaclass=ABCMeta):
    def __init__(self, data_feed, host):
        """
        Live or Historical Data Manager meta class

        :param data_feed: DataFeed object for live data
        :param host: database host machine name
        """
        self.__uuid = str(uuid.uuid4())
        self._data_feed = data_feed
        self._db_info = {'host': host}
        self._engines = {}
        self._source_symbols = {}

    @property
    def data_feed(self):
        return self._data_feed

    @property
    def db_info(self):
        return self._db_info

    def bars(self, product_type, symbol, frequency, start_datetime, end_datetime):
        """
        Returns a raccoon DataFrame for the symbol over the given date range.

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard format
        :param start_datetime: start datetime in pandas Timestamp
        :param end_datetime: end datetime in pandas Timestamp
        :return: raccoon DataFrame
        """
        return self._data_feed.bars(product_type, symbol, frequency, start_datetime, end_datetime)

    def bar(self, product_type, symbol, frequency, datetime):
        """
        Returns a single Bar dict for the symbol for the datetime. If the datetime get no data from the DataFeed then
        it will return a bar with the datetime and all components of the bar set to None.

        :param product_type: product type
        :param symbol: symbol name
        :param frequency: frequency in standard form
        :param datetime: datetime in pandas Timestamp
        :return: Bar dict
        """
        return self._data_feed.bar(product_type, symbol, frequency, datetime)


class LiveDataManager(DataManager):
    def __init__(self, data_feed, host):
        super().__init__(data_feed, host)


class HistoricalDataManager(DataManager):
    def __init__(self, data_feed, host):
        super().__init__(data_feed, host)


def market_data_manager(data_feed='SymbolDBDataFeed', host='localhost', time_zone=None, **kwargs):
    """
    Construct the MarketDataManager by initializing the DataFeed, LiveDataManager, HistoricalDataManager and
    then MarketDataManager and return it.

    :param data_feed: data_feed class name
    :param host: database host
    :param time_zone: time zone for the market data and the event loop datetimes. If None use default
    :param kwargs: additional arguments required for the data_feed
    :return: nothing
    """
    time_zone = default_time_zone if time_zone is None else time_zone
    kwargs['time_zone'] = time_zone
    hist_data_manager = HistoricalDataManager(data_feed, host)
    live_data_manager = LiveDataManager(data_feed, host)
    return MarketDataManager(hist_data_manager, live_data_manager, time_zone=time_zone)
