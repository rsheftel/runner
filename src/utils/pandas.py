import pandas as pd
import raccoon as rc
import pytz
from utils.datetime import default_time_zone, end_of_day


def rc_to_pd(raccoon_structure):
    """
    Convert a raccoon dataframe or series to pandas dataframe

    :param raccoon_structure: raccoon DataFrame or Series
    :return: pandas DataFrame
    """
    if isinstance(raccoon_structure, rc.DataFrame):
        data_dict = raccoon_structure.to_dict(index=False)
        return pd.DataFrame(data_dict, columns=raccoon_structure.columns, index=raccoon_structure.index)
    if isinstance(raccoon_structure, rc.Series):
        return pd.DataFrame({raccoon_structure.data_name: raccoon_structure.data}, index=raccoon_structure.index)


def pd_to_rc(pandas_dataframe, sort=None):
    """
    Convert a pandas dataframe to raccoon dataframe

    :param pandas_dataframe: pandas DataFrame
    :param sort: sort parameter to pass to raccoon DataFrame construction
    :return: raccoon DataFrame
    """
    columns = pandas_dataframe.columns.tolist()
    pandas_data = pandas_dataframe.to_numpy().T.tolist()
    data = {columns[i]: pandas_data[i] for i in range(len(columns))}
    index = pandas_dataframe.index.tolist()
    index_name = pandas_dataframe.index.name
    index_name = "index" if not index_name else index_name
    return rc.DataFrame(data=data, columns=columns, index=index, index_name=index_name, sort=sort)


def timedelta_to_str(timedelta):
    """
    Convert pandas Timedelta to string

    :param timedelta: pandas Timedelta
    :return: string of frequency in standard form
    """
    seconds = int(timedelta.total_seconds())
    periods = [("M", 60 * 60 * 24 * 30), ("D", 60 * 60 * 24), ("H", 60 * 60), ("min", 60), ("S", 1)]

    strings = []
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            strings.append(f"{period_value}{period_name}")
    return ", ".join(strings)


def from_daily(item, time=end_of_day, time_zone=None):
    """
    Converts a Timestamp, DateTimeIndex or DataFrame in daily format (date only, no time and no time zone) into full
    date and time with time zone.

    :param item: Timestamp, DateTimeIndex or DataFrame
    :param time: datetime.time object for the end of day time
    :param time_zone: time zone. If None use default time zone
    :return: transformed item
    """

    def zero_time(x):
        return x.replace(hour=time.hour, minute=time.minute, second=time.second, microsecond=time.microsecond)

    if time_zone is None:
        time_zone = default_time_zone
    elif isinstance(time_zone, str):
        time_zone = pytz.timezone(time_zone)

    item = item.tz_localize(None).tz_localize(time_zone)
    if isinstance(item, pd.Timestamp):
        item = zero_time(item)
    elif isinstance(item, pd.DatetimeIndex):
        items = [zero_time(x) for x in item]
        item = pd.DatetimeIndex(items, tz=item.tz, freq=item.freq, name=item.name)
    elif isinstance(item, pd.DataFrame):
        items = [zero_time(x) for x in item.index]
        item.index = pd.DatetimeIndex(items, tz=item.index.tz, freq=item.index.freq, name=item.index.name)
    return item
