import logging
import uuid
from abc import ABCMeta
from utils.datetime import default_time_zone

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


class DataManager(metaclass=ABCMeta):
    def __init__(self, data_feed, db_username, db_password, db_host):
        """
        Live or Historical Data Manager meta class

        :param data_feed: DataFeed object for live data
        :param db_username: database username
        :param db_password: database password
        :param db_host: database host machine name
        """
        self.__uuid = str(uuid.uuid4())
        self._data_feed = data_feed
        self._db_info = {'username': db_username, 'password': db_password, 'db_host': db_host}
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
    def __init__(self, data_feed, db_username, db_password, db_host):
        super().__init__(data_feed, db_username, db_password, db_host)


class HistoricalDataManager(DataManager):
    def __init__(self, data_feed, db_username, db_password, db_host):
        super().__init__(data_feed, db_username, db_password, db_host)


def market_data_manager(data_feed='SymbolDBDataFeed', db_username=None, db_password=None, db_host='localhost',
                        time_zone=None, **kwargs):
    """
    Construct the MarketDataManager by initializing the DataFeed, LiveDataManager, HistoricalDataManager and
    then MarketDataManager and return it.

    :param data_feed: data_feed class name
    :param db_username: database username
    :param db_password: database password
    :param db_host: database host
    :param time_zone: time zone for the market data and the event loop datetimes. If None use default
    :param kwargs: additional arguments required for the data_feed
    :return: nothing
    """
    time_zone = default_time_zone if time_zone is None else time_zone
    kwargs['time_zone'] = time_zone
    hist_data_manager = HistoricalDataManager(data_feed, db_username, db_password, db_host)
    live_data_manager = LiveDataManager(data_feed, db_username, db_password, db_host)
    return MarketDataManager(hist_data_manager, live_data_manager, time_zone=time_zone)
