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
