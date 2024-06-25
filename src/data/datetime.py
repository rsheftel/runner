"""
General market datetime functions
"""

import datetime
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import utils.pandas as pdutils


def bartimes(markets, frequency, start_datetime, end_datetime, include_open=True, default_close=False):
    """
    Given a start and end DateTime will return an iterator of DateTimes for the given markets at the frequency.
    Uses the pandas_market_calendars to incorporate the trading day times so that the proper times are returned
    and dates are adjusted for holidays. If default_close is True then uses the default close time for 1D data and
    ignores the early close time in the pandas_market_calendars.

    :param markets: single or list of markets (ie: NYSE, stock, CME)
    :param frequency: frequency in standard string format
    :param start_datetime: start DateTime
    :param end_datetime: end DateTime
    :param include_open: if True then include the open time as a bar
    :param default_close: if True then use the default close time for 1D data, if False use actual close time
    :return: iterator of DateTimes
    """
    schedules = []
    markets = markets if isinstance(markets, list) else [markets]
    for market in markets:
        calendar = mcal.get_calendar(market)
        schedule = calendar.schedule(start_datetime, end_datetime)
        schedules.append(schedule)
    merged_schedule = mcal.merge_schedules(schedules, how="outer")
    closed = None if include_open else "right"
    datetimes = mcal.date_range(merged_schedule, frequency, closed)

    # if the datetimes have time associated, cut by that
    if start_datetime.time() != datetime.time(0, 0):
        if not start_datetime.tz:
            raise ValueError("start_datetime must have a time zone")
        datetimes = datetimes[datetimes >= start_datetime]
    if end_datetime.time() != datetime.time(0, 0):
        if not end_datetime.tz:
            raise ValueError("end_datetime must have a time_zone")
        datetimes = datetimes[datetimes <= end_datetime]

    # if default_close for 1D data then overwrite actual early close time with default close
    if default_close & (frequency == "1D"):
        if len(markets) > 1:
            raise RuntimeError("default_close cannot handle multiple product types")
        market = mcal.get_calendar(markets[0])
        datetimes = pdutils.from_daily(datetimes, market.regular_market_times["market_close"][-1][-1], market.tz)

    return datetimes


def align_datetimes(
        x: pd.DataFrame | pd.Series,
        markets: str | list[str],
        frequency: str,
        fill_value=np.nan,
        include_open: bool = False,
        default_close: bool = False,
        strip_times: bool = False
) -> pd.DataFrame | pd.Series:
    """
    Given a pd.DataFrame or pd.Series and a list of markets will expand the DataFrame/Series so that all valid
    dates in the markets are in the index of the series. If a date/datetime is missing in the X series, it will be
    filled with the fill_value. If the X series has a date or datetime not in the values of datetimes for the markets
    it will remain in the return series.

    If default_close is True then uses the default close time for 1D data and ignores the early close time in the
    pandas_market_calendars.

    :param x: pd.DataFrame of pd.Series of time series data with the index datetimes
    :param markets: single or list of markets (ie: NYSE, stock, CME)
    :param frequency: frequency in standard string format
    :param fill_value: value to fill for missing dates
    :param include_open: if True then include the open time as a bar
    :param default_close: if True then use the default close time for 1D data, if False use actual close time
    :param strip_times: if True strip the times and only use the dates
    :return:
    """
    target_index = bartimes(markets, frequency, x.index.min(), x.index.max(), include_open, default_close)
    if strip_times:
        target_index = target_index.date
    if isinstance(x, pd.DataFrame):
        target_df = pd.DataFrame(index=target_index)
    else:
        target_df = pd.Series(index=target_index)
    res, _ = x.align(target_df, fill_value=fill_value)
    return res
