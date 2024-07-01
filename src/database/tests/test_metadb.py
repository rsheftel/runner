"""
Unit test for MetaDB
"""

import datetime

import pandas as pd
import pytest
import pytz

from utils.datetime import CHI, NYC
from database import metadb


def test_prior_business_day():
    # No holidays
    assert pd.Timestamp('2016-06-23') == metadb.prior_business_day(None, None, '2016-06-24', 1)
    assert pd.Timestamp('2016-06-23 12:00') == metadb.prior_business_day(None, None, '2016-06-24 12:00', 1)
    assert pd.Timestamp('2016-06-22 09:00', tz='EST') == \
           metadb.prior_business_day(None, None, pd.Timestamp('2016-06-24 09:00', tz='EST'), 2)
    assert pd.Timestamp('2016-06-17') == metadb.prior_business_day(None, None, '2016-06-24', 5)
    assert pd.Timestamp('2010-01-01') == metadb.prior_business_day(None, None, '2010-01-02', 1)

    # Stock product
    assert pd.Timestamp('2009-12-31 15:00', tz=NYC) == \
           metadb.prior_business_day('stock', 'TEST', pd.Timestamp('2010-01-02 15:00', tz=NYC), 1)
    assert pd.Timestamp('2009-12-31') == metadb.prior_business_day('stock', None, '2010-01-02', 1)
    assert pd.Timestamp('2016-07-04', tz='UTC') == \
           metadb.prior_business_day(None, None, pd.Timestamp('2016-07-05', tz='UTC'), 1)
    assert pd.Timestamp('2016-07-01', tz='America/New_York') == \
           metadb.prior_business_day('stock', None, pd.Timestamp('2016-07-05', tz='America/New_York'), 1)
    # regular holiday & adhoc Sandy holiday
    assert pd.Timestamp('2012-07-03') == metadb.prior_business_day('stock', None, '2012-07-05', 1)
    assert pd.Timestamp('2012-10-26') == metadb.prior_business_day('stock', None, '2012-10-31', 1)

    # Future product
    assert pd.Timestamp('2009-12-31 15:00', tz=NYC) == \
           metadb.prior_business_day('future', 'TEST', pd.Timestamp('2010-01-02 15:00', tz=NYC), 1)
    assert pd.Timestamp('2010-01-01') == metadb.prior_business_day(None, None, '2010-01-02', 1)
    assert pd.Timestamp('2009-12-31') == metadb.prior_business_day('stock', None, '2010-01-02', 1)
    assert pd.Timestamp('2016-07-04', tz='UTC') == \
           metadb.prior_business_day(None, None, pd.Timestamp('2016-07-05', tz='UTC'), 1)
    assert pd.Timestamp('2016-07-01', tz='America/New_York') == \
           metadb.prior_business_day('stock', None, pd.Timestamp('2016-07-05', tz='America/New_York'), 1)

    with pytest.raises(ValueError):
        metadb.prior_business_day('BAD', None, '2016-07-05', 1)


def test_end_of_day_time():
    assert metadb.end_of_day_time('stock', None) == datetime.time(16, 00, tzinfo=pytz.timezone('America/New_York'))

    with pytest.raises(ValueError):
        metadb.end_of_day_time('BAD', 'TEST')


def test_end_of_day():
    # stock
    assert pd.Timestamp('2010-02-02 16:00', tz='America/New_York') == \
           metadb.end_of_day('stock', None, pd.Timestamp('2010-02-02', tz='America/New_York'))

    assert pd.Timestamp('2010-02-02 15:00', tz='America/Chicago') == \
           metadb.end_of_day('stock', None, pd.Timestamp('2010-02-02 09:00', tz='America/Chicago'))

    # fail when input datetime has no time zone
    with pytest.raises(TypeError):
        metadb.end_of_day('stock', None, pd.Timestamp('2010-02-02 09:00'))


def test_prior_end_of_day():
    # No holidays
    assert pd.Timestamp('2016-06-23 16:00', tz=NYC) == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-06-24', tz=NYC), 1)
    assert pd.Timestamp('2016-06-23 15:00', tz=CHI) == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-06-24', tz=CHI), 1)
    assert pd.Timestamp('2016-06-23 16:00', tz=NYC) == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-06-24 12:00', tz=NYC), 1)
    assert pd.Timestamp('2016-06-22 15:00', tz='EST') == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-06-24 09:00', tz='EST'), 2)
    assert pd.Timestamp('2016-06-17 16:00', tz=NYC) == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-06-24', tz=NYC), 5)

    # Stock product
    assert pd.Timestamp('2009-12-31 16:00', tz=NYC) == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2010-01-02', tz=NYC), 1)
    assert pd.Timestamp('2016-06-30 20:00', tz='UTC') == \
           metadb.prior_end_of_day('stock', None, pd.Timestamp('2016-07-05', tz='UTC'), 1)

    with pytest.raises(ValueError):
        metadb.prior_end_of_day(None, None, '2016-06-24', 1)
