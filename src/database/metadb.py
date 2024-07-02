"""
Meta information for symbols and other items
"""

import functools

import pandas as pd
import pandas_market_calendars as mcal
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

import database.utils as dbutils

product_types = ('stock', 'future')


def metadb_engine(product_type, username, password, db_host='linuxdb'):
    """
    Returns a sqlalchemy engine object for the metadb database schema

    :param product_type: name of the product schema (stock, future, etc)
    :param username: username
    :param password: password
    :param db_host: host machine
    :return: sqlalchemy engine object
    """
    return dbutils.make_engine(product_type, username, password, db_host)


@functools.lru_cache()
def holidays(product_type):
    """
    For a given product_type return a holiday calendar. These are fully closed market days only, no early close.

    :param product_type: product type
    :return: pandas CustomBusinessDay calendar object
    """
    if product_type == 'future':
        return pd.tseries.offsets.CustomBusinessDay(calendar=USFederalHolidayCalendar())
    elif product_type == 'stock':
        cal = mcal.get_calendar(product_type)
        return CustomBusinessDay(
            holidays=cal.adhoc_holidays,
            calendar=cal.regular_holidays,
            weekmask="Mon Tue Wed Thu Fri"
        )
    return mcal.get_calendar(product_type).holidays()


def prior_business_day(product_type, symbol, date_time, days_back):
    """
    Returns the pandas Timestamp datetime a given number of business days prior to the datetime.

    :param product_type: product type. If None then no holiday calendar is used
    :param symbol: Symbol. If None then no holiday calendar is used
    :param date_time: the starting datetime
    :param days_back: number of days back
    :return: Timestamp ndays holiday adjusted business days prior to datetime
    """
    if (product_type is None) and (symbol is None):
        return pd.Timestamp(date_time) - pd.tseries.offsets.BDay(days_back)
    elif product_type == 'stock':
        return pd.Timestamp(date_time) - days_back * holidays(product_type)
    elif product_type == 'future':
        return pd.Timestamp(date_time) - days_back * holidays(product_type)
    else:
        raise ValueError(f'product_type {product_type} not supported.')


@functools.lru_cache()
def _stock_default_close_time():
    cal = mcal.get_calendar('stock')
    close_time = cal.regular_market_times['market_close'][-1][-1]
    return close_time.replace(tzinfo=cal.tz)


def end_of_day_time(product_type, symbol):
    """
    Returns a datetime.time object with the closing/settlement time and time zone for a (product type, symbol)

    :param product_type: product type
    :param symbol: symbol
    :return: datetime.time object
    """
    if product_type == 'stock':
        return _stock_default_close_time()
    else:
        raise ValueError(f'product_type {product_type} not supported.')


def end_of_day(product_type, symbol, date_time):
    """
    Returns the pandas Timestamp with the time set to the end of day time for the (product type, symbol).

    :param product_type: product type
    :param symbol: symbol
    :param date_time: datetime to be modified
    :return: pandas Timestamp
    """
    eod_time = end_of_day_time(product_type, symbol)
    time_zone = date_time.tz
    eod = date_time.tz_convert(eod_time.tzinfo)
    eod = eod.normalize().replace(hour=eod_time.hour, minute=eod_time.minute)
    return eod.tz_convert(time_zone)


def prior_end_of_day(product_type, symbol, date_time, days_back):
    """
    Returns the pandas Timestamp datetime a given number of business days prior to the datetime with the time set to
    the end of day time for that (product type, symbol)

    :param product_type: product type. If None then no holiday calendar is used
    :param symbol: Symbol. If None then no holiday calendar is used
    :param date_time: the starting datetime
    :param days_back: number of days back
    :return: Timestamp ndays holiday adjusted business days prior to datetime
    """
    prior_eod = prior_business_day(product_type, symbol, date_time, days_back)
    return end_of_day(product_type, symbol, prior_eod)
