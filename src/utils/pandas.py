import pandas as pd
import datetime
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


def _get_items(raccoon_structure):
    """
    Private method that returns the data items from a raccoon Series or 1-column DataFrame

    :param raccoon_structure: either raccoon Series or 1-column DataFrame
    :return: list
    """
    if isinstance(raccoon_structure, rc.Series):
        items = raccoon_structure.data
    elif isinstance(raccoon_structure, rc.DataFrame):
        if len(raccoon_structure.columns) > 1:
            raise ValueError("If input is DataFrame can only have one column")
        items = raccoon_structure.data[0]
    else:
        raise ValueError("Only series or one-column DataFrame allowed as input")
    return items


def namedtuple_to_df(raccoon_structure):
    """
    Converts a Series or a 1-column DataFrame where the column is all named tuples and converts it to a dataframe
    where each field in the named tuple is a column in the new DataFrame.

    :param raccoon_structure: raccoon Series or one column DataFrame
    :return: DataFrame
    """
    items = _get_items(raccoon_structure)
    # noinspection PyProtectedMember
    columns = items[0]._fields
    zipped = zip(*items)
    data = {k: list(v) for (k, v) in zip(columns, zipped)}
    return rc.DataFrame(
        data, list(columns), raccoon_structure.index, raccoon_structure.index_name, sort=raccoon_structure.sort
    )


def read_csv_time_series(filename, datetime_col, parser):
    """
    Implements the pandas read_csv with a datetime parser now that that functionality is depricated from the pandas
    pacakge in v2.0.0. This will read a time series csv file and use the parser function to convert the dateime_col
    to datetime and make that the index

    :param filename: filename
    :param datetime_col: column name for the datetimes for column number as integer
    :param parser: parser function to apply to the datetime string values to convert to datetimes
    :return: pandas DataFrame
    """
    df = pd.read_csv(filename)
    datetime_col = df.columns[datetime_col] if isinstance(datetime_col, int) else datetime_col
    df[datetime_col] = parser(df[datetime_col].values)
    return df.set_index(datetime_col)


def datetime_parser(dates):
    """
    This function is to be used in pandas read_csv as the datetime parser. It will accept either full datetimes with a
    UTC offset, or just a date.

    :param dates: list of string dates or datetimes
    :return: DateTimeIndex
    """
    try:
        datetimes = [datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z") for date in dates]
        return pd.to_datetime(datetimes, utc=True)
    except ValueError:
        try:
            datetimes = [pd.Timestamp(datetime.datetime.strptime(date, "%Y-%m-%d"), tz=None) for date in dates]
            return pd.DatetimeIndex(datetimes)
        except ValueError:
            raise ValueError(
                "Unable to parse datetimes because the format were not either YYYY-MM-DD or " "YYYY-MM-DD HH:MM:SS+/-Z"
            )


def strict_parser(dates):
    """
    This function is to be used in pandas read_csv as the datetime parser. It is a strict version that will only accept
    full datetimes with UTC offset.

    :param dates: list of string of datetimes
    :return: DateTimeIndex
    """
    datetimes = [datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z") for date in dates]
    return pd.to_datetime(datetimes, utc=True)


def set_datetime(input_datetime, time_zone_input=None, time_zone_output=None):
    """
    Takes in a value and returns either None if the input is None, or the datetime as a pandas Timestamp

    :param input_datetime: input argument as either None, pandas Timestamp or something that can coerce to a pandas
        Timestamp
    :param time_zone_input: time zone of the input_datetime, ignored if input_datetime already has a timezone attached
    :param time_zone_output: time zone to convert the output to, if None then input timezone preserved
    :return: either None or pandas Timestamp
    """
    if input_datetime is not None:
        if isinstance(input_datetime, pd.Timestamp):
            if input_datetime.tz is None:
                input_datetime = input_datetime.tz_localize(time_zone_input)
        else:
            input_datetime = pd.Timestamp(input_datetime, tz=time_zone_input)
        if time_zone_output:
            if input_datetime.tz is None:
                raise ValueError('Ambiguous conversion attempt: cannot set time_zone_output if input_datetime has '
                                 'time zone None and time_zone_input is None.')
            else:
                input_datetime = input_datetime.tz_convert(time_zone_output)
    return input_datetime
