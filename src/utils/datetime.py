"""
time zone related config
"""

import datetime

import pandas as pd
import pytz

default_time_zone = pytz.timezone('America/New_York')
NYC = pytz.timezone('America/New_York')
CHI = pytz.timezone('America/Chicago')
UTC = datetime.timezone.utc

end_of_day = datetime.time(17, 00, tzinfo=pytz.timezone('America/New_York'))
MIDNIGHT = datetime.time(0, 0)

DAY = pd.tseries.offsets.Day()
NANOSECOND = pd.tseries.offsets.Nano()


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
