"""
unit tests for LiveDataManager and related functions
"""

from pathlib import Path

import pandas as pd

import data as datalib
from utils.datetime import NYC
from data import data_manager, structures

# Global variables
inst_dir = Path()


def setup_module():
    global inst_dir
    inst_dir = Path(__file__).parent / "inst"


def test_bars():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', source_name='test_csv_data_feed')
    ldm = data_manager.LiveDataManager(csvdf, host="temp")

    actuals = ldm.bars('stock', 'test.sym.1', '1min', pd.Timestamp('2000-01-01 09:45:00', tz=NYC),
                       pd.Timestamp('2000-01-01 09:50:00', tz=NYC))
    assert len(actuals) == 6

    # can either use Bar or standard dict
    assert actuals.get_location(0, as_dict=True) == structures.Bar(pd.Timestamp('2000-01-01 09:45:00', tz=NYC),
                                                                   107.5,109.0,106.0,108.0,175)
    assert actuals.get_location(2, as_dict=True) == {'datetime': pd.Timestamp('2000-01-01 09:47:00', tz=NYC),
                                                     'open': 108.5, 'high': 110.0, 'low': 107.0, 'close': 109.0,
                                                     'volume': 185}


def test_bar():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', source_name='test_csv_data_feed')
    ldm = data_manager.LiveDataManager(csvdf, host="temp")

    assert ldm.bar('stock', 'MSFT', '1min', pd.Timestamp('2010-01-01 14:00:00', tz=NYC)) == \
           {'datetime': pd.Timestamp('2010-01-01 14:00:00', tz=NYC), 'open': 41.38, 'high': 42.29, 'low': 40.67,
            'close': 41.27, 'volume': 824}

    assert ldm.bar('stock', 'TEST', '1min', pd.Timestamp('2010-01-01 14:00:00', tz=NYC)) == \
           structures.Bar(pd.Timestamp('2010-01-01 14:00:00', tz=NYC), 94.11, 94.6, 92.77, 93.7, 123)

    # try to get a bar that is not in the data
    assert ldm.bar('stock', 'TEST', '1min', pd.Timestamp('2100-12-31 12:00:00', tz=NYC)) == \
           {'datetime': pd.Timestamp('2100-12-31 12:00:00', tz=NYC), 'open': None, 'high': None, 'low': None,
               'close': None, 'volume': None}
