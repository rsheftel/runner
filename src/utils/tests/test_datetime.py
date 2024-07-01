"""
Unit tests for datetime utils.
"""

import datetime
import pandas as pd
import pytest

import utils.datetime as dtutils


def test_set_datetime():
    assert dtutils.set_datetime('2015-01-01') == pd.Timestamp('2015-01-01')

    in_date = pd.Timestamp('2020-12-04 15:00:01')
    assert dtutils.set_datetime(in_date) == in_date

    in_date = datetime.datetime(1990, 6, 5)
    assert dtutils.set_datetime(in_date) == pd.Timestamp(in_date)

    assert dtutils.set_datetime(None) is None

    with pytest.raises(ValueError):
        dtutils.set_datetime('BAD')

    # with time zones
    # the input time zone is None
    in_date = pd.Timestamp('2020-12-04 15:00:01')
    assert dtutils.set_datetime(in_date, time_zone_input=None) == in_date
    assert dtutils.set_datetime('2020-12-04 15:00:01', time_zone_input=None) == in_date

    # input time zone is not None, output is None
    out_date = pd.Timestamp('2020-12-04 15:00:01', tz='UTC')
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 15:00:01'), time_zone_input='UTC') == out_date
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 15:00:01', tz='UTC')) == out_date
    assert dtutils.set_datetime('2020-12-04 15:00:01', time_zone_input='UTC') == out_date

    # EST in, America/Chicago out
    out_date = pd.Timestamp('2020-12-04 14:00:01', tz='America/Chicago')
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 15:00:01', tz='EST'),
                                time_zone_output='America/Chicago') == out_date
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 15:00:01'),
                                time_zone_input='EST', time_zone_output='America/Chicago') == out_date
    assert dtutils.set_datetime('2020-12-04 15:00:01', time_zone_input='EST',
                                time_zone_output='America/Chicago') == out_date

    # None in, America/Chicago out raises error
    with pytest.raises(ValueError):
        dtutils.set_datetime('2020-12-04 14:00:01', time_zone_input=None, time_zone_output='America/Chicago')

    with pytest.raises(ValueError):
        dtutils.set_datetime(pd.Timestamp('2020-12-04 14:00:01'), time_zone_input=None,
                             time_zone_output='America/Chicago')

    # EST in, None out. The None on the output is ignored, time_zone_input retained
    out_date = pd.Timestamp('2020-12-04 14:00:01', tz='EST')
    assert dtutils.set_datetime('2020-12-04 14:00:01', time_zone_input='EST', time_zone_output=None) == out_date
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 14:00:01', tz='EST'), time_zone_input=None,
                                time_zone_output=None) == out_date

    # tz_input ignored when the input Timestamp has a tz
    out_date = pd.Timestamp('2020-12-04 14:00:01', tz='America/Chicago')
    assert dtutils.set_datetime(pd.Timestamp('2020-12-04 15:00:01', tz='EST'), time_zone_input='UTC',
                                time_zone_output='America/Chicago') == out_date
