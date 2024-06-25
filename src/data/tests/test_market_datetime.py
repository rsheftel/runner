"""
Unit tests
"""

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_index_equal, assert_series_equal, assert_frame_equal

import data.datetime as mdatetime


def test_bartimes():
    # Test daily, note that the 1st is missing for the holiday, as are the 5th and 6th for weekend
    expected = pd.DatetimeIndex(
        [
            pd.Timestamp(x + " 16:00", tz="America/New_York").tz_convert("UTC")
            for x in ["1991-01-02", "1991-01-03", "1991-01-04", "1991-01-07"]
        ],
        freq="B",
    )
    actual = mdatetime.bartimes(
        "stock", "1D", pd.Timestamp("1991-01-01"), pd.Timestamp("1991-01-07"), include_open=False
    )
    assert_index_equal(actual, expected)

    # Test minute inside one day including the open bar
    expected = pd.DatetimeIndex(
        [
            pd.Timestamp(x, tz="America/New_York").tz_convert("UTC")
            for x in ["1991-01-02 09:30:00", "1991-01-02 09:31:00", "1991-01-02 09:32:00"]
        ],
        freq="T",
    )
    actual = mdatetime.bartimes(
        ["NYSE"],
        "1min",
        pd.Timestamp("1991-01-02 09:30:00", tz="America/New_York"),
        pd.Timestamp("1991-01-02 09:32:00", tz="America/New_York"),
    )
    assert_index_equal(actual, expected)

    # Test daily with default_close False gets early close end time for 2017-07-03
    actual = mdatetime.bartimes(
        "stock", "1D", pd.Timestamp("2017-07-01"), pd.Timestamp("2017-07-07"), include_open=False
    )
    assert actual[0] == pd.Timestamp("2017-07-03 13:00", tz="America/New_York")

    # Test daily with default_close True the early close for 2017-07-03 overwritten with default end time
    actual = mdatetime.bartimes(
        "stock", "1D", pd.Timestamp("2017-07-01"), pd.Timestamp("2017-07-07"), include_open=False, default_close=True
    )
    assert actual[0] == pd.Timestamp("2017-07-03 16:00", tz="America/New_York")

    # default_close True ignored for sub daily time
    # early close date
    actual = mdatetime.bartimes(
        "stock",
        "60min",
        pd.Timestamp("2017-07-03 09:00", tz="America/New_York"),
        pd.Timestamp("2017-07-03 16:00", tz="America/New_York"),
        default_close=True,
    )
    assert len(actual) == 5
    assert actual[-1] == pd.Timestamp("2017-07-03 13:00", tz="America/New_York")

    # regular close date
    actual = mdatetime.bartimes(
        "stock",
        "60min",
        pd.Timestamp("2017-07-05 09:00", tz="America/New_York"),
        pd.Timestamp("2017-07-05 16:00", tz="America/New_York"),
        default_close=True,
    )
    assert len(actual) == 8
    assert actual[-1] == pd.Timestamp("2017-07-05 16:00", tz="America/New_York")

    # only works with one product_type for default close
    with pytest.raises(RuntimeError):
        mdatetime.bartimes(
            ["stock", "stock"],
            "1D",
            pd.Timestamp("2017-07-05 09:00", tz="America/New_York"),
            pd.Timestamp("2017-07-05 16:00", tz="America/New_York"),
            default_close=True,
        )


def test_align_datetimes():
    # series
    ser = pd.Series(
        [1, 2, 3, 4, 5],
        pd.to_datetime(["2024-03-18", "2024-03-19", "2024-03-21", "2024-03-22", "2024-03-23"]),
    )
    expected = pd.Series(
        [1, 2, np.nan, 3, 4, 5],
        pd.to_datetime(["2024-03-18", "2024-03-19", "2024-03-20", "2024-03-21", "2024-03-22", "2024-03-23"]),
    )
    actual = mdatetime.align_datetimes(ser, "stock", "1D", strip_times=True)
    assert_series_equal(actual, expected)

    # dataframe
    df = pd.DataFrame({'1': ser, '2': ser * 2})
    expected = pd.DataFrame({'1': expected, '2': expected * 2})
    actual = mdatetime.align_datetimes(df, "stock", "1D", strip_times=True)
    assert_frame_equal(actual, expected)

    # with times
    df = df.tz_localize('UTC')
    df.index = [d.replace(hour=20) for d in df.index]

    expected = expected.tz_localize('UTC')
    expected.index = [d.replace(hour=20) for d in expected.index]

    actual = mdatetime.align_datetimes(df, "stock", "1D")
    assert_frame_equal(actual, expected)
