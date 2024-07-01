"""
unit tests for CsvDataFeed classes
"""

from pathlib import Path

import pandas as pd
import pytest
import pytz
import raccoon as rc

from utils.datetime import NYC, default_time_zone
from data import data_feed, structures

# Global variables
inst_dir = None


def setup_module():
    global inst_dir
    inst_dir = Path(__file__).parent / "inst"


def test_time_zone():
    csvdf = data_feed.CsvDataFeed(f'{inst_dir}/csv_data_feed')
    assert csvdf.time_zone == pytz.timezone('America/New_York')

    csvdf = data_feed.CsvDataFeed(f'{inst_dir}/csv_data_feed', time_zone='UTC')
    assert csvdf.time_zone == 'UTC'

    with pytest.raises(AttributeError):
        # noinspection PyPropertyAccess
        csvdf.time_zone = default_time_zone


def test_load_data():
    csvdf = data_feed.CsvDataFeed(f'{inst_dir}/csv_data_feed')
    csvdf.load_data('stock', 'test.sym.1', '1min')
    actual = csvdf._bar_data['stock']['1min']['test.sym.1']

    assert sum(actual['close'].to_list()) == 20930.0
    assert min(actual.index) == pd.Timestamp('2000-01-01 09:30:00', tz=NYC)
    assert max(actual.index) == pd.Timestamp('2000-01-02 11:00:00', tz=NYC)


def test_bars():
    csvdf = data_feed.CsvDataFeed(f'{inst_dir}/csv_data_feed')

    # cut start to inside date
    actual = csvdf.bars('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 09:30:00', tz=NYC),
                        pd.Timestamp('2000-01-01 09:40:00', tz=NYC))
    assert isinstance(actual, rc.DataFrame)
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2000-01-01 09:30:00', tz=NYC),
                                                    'open': 100.0, 'high': 101.5, 'low': 98.5, 'close': 100.5,
                                                    'volume': 100}
    # Note that dict or Bar data type can be used
    assert actual.get_location(-1, as_dict=True) == structures.Bar(pd.Timestamp('2000-01-01 09:40:00', tz=NYC),
                                                                   105.0, 106.5, 103.5, 105.5, 150)

    # cut inside start to exact end
    actual = csvdf.bars('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 10:30:00', tz=NYC),
                        pd.Timestamp('2000-01-02 11:00:00', tz=NYC))
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2000-01-01 10:30:00', tz=NYC),
                                                    'open': 130.0, 'high': 131.5, 'low': 128.5, 'close': 130.5,
                                                    'volume': 400}

    assert actual.get_location(-1, as_dict=True) == {'datetime': pd.Timestamp('2000-01-02 11:00:00', tz=NYC),
                                                     'open': 85.0, 'high': 86.0, 'low': 84.0, 'close': 84.5,
                                                     'volume': 1000}

    # cut before first date to after end date
    actual = csvdf.bars('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 10:45:00', tz=NYC),
                        pd.Timestamp('2000-01-02 10:05:00', tz=NYC))
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2000-01-01 10:45:00', tz=NYC),
                                                    'open': 137.5, 'high': 139, 'low': 136, 'close': 138, 'volume': 475}

    assert actual.get_location(-1, as_dict=True) == {'datetime': pd.Timestamp('2000-01-02 10:05:00', tz=NYC),
                                                     'open': 112.5, 'high': 113.5, 'low': 111.5, 'close': 112,
                                                     'volume': 450}

    # test with missing high, low columns
    actual = csvdf.bars('stock', 'test.sym.2', '1D', pd.Timestamp('2010-01-01', tz=NYC),
                        pd.Timestamp('2010-01-30', tz=NYC))
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2010-01-01', tz=NYC), 'open': None,
                                                    'high': None, 'low': None, 'close': 10.0, 'volume': None}
    assert actual.get_location(-1, as_dict=True) == {'datetime': pd.Timestamp('2010-01-30', tz=NYC), 'open': None,
                                                     'high': None, 'low': None, 'close': 24.5, 'volume': None}

    # test with missing gaps that get NaN
    actual = csvdf.bars('stock', 'test.sym.2', '1D', pd.Timestamp('2010-01-05', tz=NYC),
                        pd.Timestamp('2010-01-12', tz=NYC))
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2010-01-05', tz=NYC), 'open': None,
                                                    'high': None, 'low': None, 'close': None, 'volume': None}
    assert actual.get_location(1, as_dict=True) == {'datetime': pd.Timestamp('2010-01-06', tz=NYC), 'open': None,
                                                    'high': None, 'low': None, 'close': 12.5, 'volume': None}
    assert actual.get_location(-2, as_dict=True) == {'datetime': pd.Timestamp('2010-01-11', tz=NYC), 'open': None,
                                                     'high': None, 'low': None, 'close': 15.0, 'volume': None}
    assert actual.get_location(-1, as_dict=True) == structures.Bar(pd.Timestamp('2010-01-12', tz=NYC), None, None, None,
                                                                   None, None)

    # test outside range
    actual = csvdf.bars('stock', 'test.sym.2', '1D', pd.Timestamp('2000-01-05', tz=NYC),
                        pd.Timestamp('2000-01-02', tz=NYC))
    assert len(actual) == 0


def test_bar():
    csvdf = data_feed.CsvDataFeed(f'{inst_dir}/csv_data_feed', time_zone='America/Chicago')

    actual = csvdf.bar('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 09:30:00', tz='America/Chicago'))
    assert isinstance(actual, dict)
    assert actual == {'datetime': pd.Timestamp('2000-01-01 09:30:00', tz='America/Chicago'),
                      'open': 130.0, 'high': 131.5, 'low': 128.5, 'close': 130.5, 'volume': 400}

    actual = csvdf.bar('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 09:40:00', tz=NYC))
    assert actual == {'datetime': pd.Timestamp('2000-01-01 09:40:00', tz=NYC), 'open': 105.0, 'high': 106.5,
                      'low': 103.5, 'close': 105.5, 'volume': 150}

    actual = csvdf.bar('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 09:55:55', tz=NYC))
    assert actual == {'datetime': pd.Timestamp('2000-01-01 09:55:55', tz=NYC), 'open': None, 'high': None,
                      'low': None, 'close': None, 'volume': None}
