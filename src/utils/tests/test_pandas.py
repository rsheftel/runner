import pytest
from utils.datetime import UTC
from collections import namedtuple
import pandas as pd
import raccoon as rc
import datetime
from pandas.testing import assert_frame_equal, assert_index_equal
import utils.pandas as pd_utils
from raccoon.utils import assert_frame_equal as rc_assert_frame_equal
from pathlib import Path

inst_dir = None


def setup_module():
    global inst_dir
    inst_dir = Path(__file__).parent / "inst/pandas/"


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


def test_datetime_parser():
    # works for date only
    expected = pd.DatetimeIndex(["2015-05-05", "2015-05-06", "2015-05-07"], tz=None)
    actual = pd_utils.datetime_parser(["2015-05-05", "2015-05-06", "2015-05-07"])
    assert_index_equal(actual, expected)
    assert actual.tz is None

    # works for full datetime with time zone
    actual = pd_utils.datetime_parser(
        ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"]
    )
    expected = pd.to_datetime(
        ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"], utc=True
    )
    assert_index_equal(actual, expected)
    assert actual.tz == UTC

    # this fails because first item is date only
    with pytest.raises(ValueError):
        pd_utils.datetime_parser(["2010-05-06", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0500"])

    # fails because no UTC offset on first item
    with pytest.raises(ValueError):
        pd_utils.datetime_parser(["2010-05-03 09:30:00", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0500"])

    # fails because no UTC offset on any items
    with pytest.raises(ValueError):
        pd_utils.datetime_parser(["2010-05-03 09:30:00", "2010-05-05 09:35:00", "2010-05-05 09:40:00"])


def test_strict_parser():
    # works for full datetime with time zone only
    actual = pd_utils.strict_parser(
        ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"]
    )
    expected = pd.to_datetime(
        ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"], utc=True
    )
    assert_index_equal(actual, expected)
    assert actual.tz == UTC

    # fails for date only
    with pytest.raises(ValueError):
        pd_utils.strict_parser(["2015-05-05", "2015-05-06", "2015-05-07"])

    # this fails because first item is date only
    with pytest.raises(ValueError):
        pd_utils.strict_parser(["2010-05-06", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0500"])

    # fails because no UTC offset on first item
    with pytest.raises(ValueError):
        pd_utils.strict_parser(["2010-05-03 09:30:00", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0500"])

    # fails because no UTC offset on any items
    with pytest.raises(ValueError):
        pd_utils.strict_parser(["2010-05-03 09:30:00", "2010-05-05 09:35:00", "2010-05-05 09:40:00"])


def test_read_csv_datetime_parser():
    expected = pd.DataFrame(
        {"close": [1.1, 2.2, 3.3]},
        index=pd.DatetimeIndex(["2015-05-05", "2015-05-06", "2015-05-07"], tz=None, name="date"),
    )
    actual = pd_utils.read_csv_time_series(f"{inst_dir}/date_good.csv", "date", pd_utils.datetime_parser)
    assert_frame_equal(actual, expected)

    # works for full datetime with time zone
    expected = pd.DataFrame(
        {"close": [4.4, 5.5, 6.6]},
        index=pd.DatetimeIndex(
            pd.to_datetime(
                ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"], utc=True
            ),
            name="datetime",
        ),
    )
    actual = pd_utils.read_csv_time_series(
        f"{inst_dir}/datetime_good.csv", datetime_col=0, parser=pd_utils.datetime_parser
    )
    assert_frame_equal(actual, expected)

    # this fails because first item is date only
    with pytest.raises(ValueError):
        actual = pd_utils.read_csv_time_series(f"{inst_dir}/date_bad.csv", "datetime", pd_utils.datetime_parser)

    # fails because no UTC offset on first item
    with pytest.raises(ValueError):
        actual = pd_utils.read_csv_time_series(f"{inst_dir}/datetime_bad.csv", "datetime", pd_utils.datetime_parser)


def test_read_csv_strict_parser():
    # works for full datetime with time zone
    expected = pd.DataFrame(
        {"close": [4.4, 5.5, 6.6]},
        index=pd.DatetimeIndex(
            pd.to_datetime(
                ["2010-05-05 09:30:00-0500", "2010-05-05 09:35:00-0500", "2010-05-05 09:40:00-0400"], utc=True
            ),
            name="datetime",
        ),
    )
    actual = pd_utils.read_csv_time_series(f"{inst_dir}/datetime_good.csv", "datetime", pd_utils.strict_parser)
    assert_frame_equal(actual, expected)

    # this fails because they are all dates not datetime
    with pytest.raises(ValueError):
        actual = pd_utils.read_csv_time_series(f"{inst_dir}/date_good.csv", "date", pd_utils.strict_parser)

    # this fails because first item is date only
    with pytest.raises(ValueError):
        actual = pd_utils.read_csv_time_series(f"{inst_dir}/date_bad.csv", "datetime", pd_utils.strict_parser)

    # fails because no UTC offset on first item
    with pytest.raises(ValueError):
        actual = pd_utils.read_csv_time_series(f"{inst_dir}/datetime_bad.csv", "datetime", pd_utils.strict_parser)
