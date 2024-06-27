import pytest
from collections import namedtuple
import pandas as pd
import raccoon as rc
import datetime
from pandas.testing import assert_frame_equal, assert_index_equal
import utils.pandas as pd_utils
from raccoon.utils import assert_frame_equal as rc_assert_frame_equal


def test_rc_to_pd():
    # DataFrame input
    rc_df = rc.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"], index=[7, 8, 9])
    expected = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"], index=[7, 8, 9])
    actual = pd_utils.rc_to_pd(rc_df)
    assert_frame_equal(actual, expected)

    # DataFrame series
    rc_srs = rc.Series([4, 5, 6], data_name="b", index=[7, 8, 9])
    expected = pd.DataFrame({"b": [4, 5, 6]}, index=[7, 8, 9])
    actual = pd_utils.rc_to_pd(rc_srs)
    assert_frame_equal(actual, expected)


def test_pd_to_rc():
    pd_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[5, 6, 7], columns=["a", "b"])
    expected = rc.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[5, 6, 7], columns=["a", "b"])
    actual = pd_utils.pd_to_rc(pd_df)
    rc_assert_frame_equal(actual, expected)

    pd_df = pd.DataFrame({"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"])
    pd_df.index.name = "hasName"
    expected = rc.DataFrame(
        {"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"], index_name="hasName", sort=False
    )
    actual = pd_utils.pd_to_rc(pd_df)
    rc_assert_frame_equal(actual, expected)

    expected = rc.DataFrame(
        {"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"], index_name="hasName", sort=True
    )
    actual = pd_utils.pd_to_rc(pd_df, sort=True)
    rc_assert_frame_equal(actual, expected)


def test_timedelta_to_str():
    assert pd_utils.timedelta_to_str(pd.Timedelta("1D")) == "1D"
    assert pd_utils.timedelta_to_str(pd.Timedelta("3H")) == "3H"
    assert pd_utils.timedelta_to_str(pd.Timedelta("1min")) == "1min"
    assert pd_utils.timedelta_to_str(pd.Timedelta("5S")) == "5S"


def test_from_daily():
    expected = pd.Timestamp("2010-11-11 17:00", tz="America/New_York")
    assert pd_utils.from_daily(pd.Timestamp("2010-11-11")) == expected

    expected = pd.Timestamp("2010-11-11 12:00", tz="America/New_York")
    assert pd_utils.from_daily(pd.Timestamp("2010-11-11"), time=datetime.time(12, 00)) == expected

    expected = pd.Timestamp("2010-11-11 12:00", tz="UTC")
    assert pd_utils.from_daily(pd.Timestamp("2010-11-11"), time=datetime.time(12, 00), time_zone="UTC") == expected

    expected = pd.date_range("2010-10-01 12:00:00", periods=10, freq="1D", tz="America/Chicago")
    expected.name = "TESTNAME"
    actual = pd_utils.from_daily(
        pd.date_range("2010-10-01", periods=10, freq="1D", name="TESTNAME"),
        time=datetime.time(12, 00),
        time_zone="America/Chicago",
    )
    assert_index_equal(actual, expected)

    actual = pd_utils.from_daily(
        pd.date_range("2010-10-01 09:00", periods=10, freq="1D", tz="UTC", name="TESTNAME"),
        time=datetime.time(12, 00),
        time_zone="America/Chicago",
    )
    assert_index_equal(actual, expected)

    start = pd.DataFrame(
        {"a": list(range(10))},
        index=pd.date_range("2010-10-01 12:00:00", periods=10, freq="1D", tz="UTC", name="TESTNAME"),
    )
    end = pd.DataFrame(
        {"a": list(range(10))}, index=pd.date_range("2010-10-01", periods=10, freq="1D", name="TESTNAME")
    )
    assert_frame_equal(pd_utils.from_daily(end, datetime.time(12, 00), time_zone="UTC"), start)


def test_namedtuple_convert():
    Name = namedtuple("Name", ["first", "second", "third"])
    test_data = [Name(x, x * 2, x * 3) for x in [1, 4, 5]]
    expected_data = {"first": [1, 4, 5], "second": [2, 8, 10], "third": [3, 12, 15]}

    srs = rc.Series(test_data)
    expected = rc.DataFrame(expected_data)
    actual = pd_utils.namedtuple_to_df(srs)
    rc_assert_frame_equal(actual, expected)

    srs = rc.Series(test_data, index=[8, 9, 10], index_name="indy")
    expected = rc.DataFrame(expected_data, index=[8, 9, 10], index_name="indy")
    actual = pd_utils.namedtuple_to_df(srs)
    rc_assert_frame_equal(actual, expected)

    df = rc.DataFrame({"a": test_data}, sort=True)
    expected = rc.DataFrame(expected_data, sort=True)
    actual = pd_utils.namedtuple_to_df(df)
    rc_assert_frame_equal(actual, expected)

    df = rc.DataFrame({"a": test_data, "b": test_data})
    with pytest.raises(ValueError):
        pd_utils.namedtuple_to_df(df)
