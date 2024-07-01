"""
unit tests for the MarketDataManager and associated functions
"""

from pathlib import Path

import pandas as pd
import pytest
import pytz
import raccoon as rc
from raccoon.utils import assert_frame_equal

import data as datalib
from utils.datetime import NYC, default_time_zone
from data import data_manager, structures
from database import symboldb

# Global variables
inst_dir = Path()
test_login = {}
global_ldm = None


def setup_module():
    global inst_dir, global_ldm, test_login
    inst_dir = Path(__file__).parent / "inst"
    global_ldm = datalib.LiveDataManager(None, host='temp')


def test_time_zone():
    # if the data managers are None, skip test
    mdm = data_manager.MarketDataManager(None, None)
    assert mdm.time_zone == pytz.timezone('America/New_York')

    # data managers, but no data feed
    hdm = data_manager.HistoricalDataManager(None, host='temp')
    ldm = data_manager.LiveDataManager(None, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm, time_zone=pytz.utc)
    assert mdm.time_zone == pytz.utc

    # time zones match
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', time_zone='UTC')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm, time_zone='UTC')
    assert mdm.time_zone == 'UTC'

    # time zones do not match
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', time_zone='UTC')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    with pytest.raises(AttributeError):
        data_manager.MarketDataManager(hdm, ldm, time_zone=NYC)


def test_db_info():
    ldm = datalib.LiveDataManager(None, host='temp')
    hdm = datalib.HistoricalDataManager(None, host='temp')

    expected = {'host': 'temp'}
    mdm = data_manager.MarketDataManager(hdm, None)
    assert mdm.database_info() == expected

    mdm = data_manager.MarketDataManager(None, ldm)
    assert mdm.database_info() == expected

    mdm = data_manager.MarketDataManager(None, None)
    with pytest.raises(RuntimeError):
        mdm.database_info()


def test_source():
    csvdf_hist = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', 'hist_source')
    csvdf_live = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', 'live_source')
    hdm = data_manager.HistoricalDataManager(csvdf_hist, host='temp')
    ldm = data_manager.LiveDataManager(csvdf_live, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)

    assert mdm.source('live') == 'live_source'
    assert mdm.source('historical') == 'hist_source'

    with pytest.raises(ValueError):
        mdm.source('bad')


def test_bartime():
    mdm = data_manager.MarketDataManager(None, None)

    assert mdm.bartime is None

    mdm.bartime = '2010-01-01 09:30:00'
    assert mdm.bartime == pd.Timestamp('2010-01-01 09:30:00', tz=NYC)

    mdm.bartime = '2010-01-01 09:40:00'
    assert mdm.bartime == pd.Timestamp('2010-01-01 09:40:00', tz=NYC)

    with pytest.raises(AttributeError):
        mdm.bartime = '2010-01-01 09:40:00'

    with pytest.raises(AttributeError):
        mdm.bartime = '2010-01-01 09:39:00'

    mdm.bartime = pd.Timestamp('2010-01-01 08:50:00', tz='America/Chicago')
    assert mdm.bartime == pd.Timestamp('2010-01-01 09:50:00', tz=NYC)

    mdm = data_manager.MarketDataManager(None, None, time_zone='UTC')
    mdm.bartime = '2010-01-01 09:30:00'
    assert mdm.bartime == pd.Timestamp('2010-01-01 09:30:00', tz='UTC')


def test_add_symbols():
    mdm = data_manager.MarketDataManager(None, global_ldm)
    assert mdm.symbols('stock', '1min') == []

    mdm.add_symbols('stock', 'test.sym.3', '1min')
    assert mdm.product_types() == ['stock']
    assert mdm.frequencies('stock') == ['1min']
    assert mdm.symbols('stock', '1min') == ['test.sym.3']

    mdm.add_symbols('stock', ['test.sym.1'], '1min')
    assert sorted(mdm.symbols('stock', '1min')) == ['test.sym.1', 'test.sym.3']

    mdm.add_symbols('stock', ['test.sym.2', 'test.sym.5'], '1min')
    assert sorted(mdm.symbols('stock', '1min')) == ['test.sym.1', 'test.sym.2', 'test.sym.3', 'test.sym.5']

    mdm.add_symbols('stock', ['test.sym.2', 'test.sym.5'], '1D')
    assert sorted(mdm.frequencies('stock')) == ['1D', '1min']
    assert sorted(mdm.symbols('stock', '1D')) == ['test.sym.2', 'test.sym.5']

    # test redundant entry
    mdm.add_symbols('stock', ['test.sym.3', 'test.sym.5'], '1min')
    assert sorted(mdm.symbols('stock', '1min')) == ['test.sym.1', 'test.sym.2', 'test.sym.3', 'test.sym.5']

    # test empty frequency
    assert mdm.symbols('stock', '5min') == []

    # test empty product_type
    assert mdm.frequencies('future') == []
    assert mdm.symbols('future', '1D') == []


def test_load_history_1min():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', 'test.sym.1', '1min')
    mdm.add_symbols('stock', 'test.sym.2', '1D')

    # call here of empty
    actual = mdm.bars('stock', 'test.sym.1', '1min')
    assert_frame_equal(actual, rc.DataFrame(index_name='datetime', sort=True,
                                            columns=['open', 'high', 'low', 'close', 'volume']))

    # populate live data and test
    for date in pd.date_range('2000-01-01 10:01:00', periods=10, freq='1min'):
        mdm.bartime = date
        mdm.update('stock', '1min')
    actual = mdm.bars('stock', 'test.sym.1', '1min')
    assert len(actual) == 10
    assert actual.index[0] == pd.Timestamp('2000-01-01 10:01:00', tz=NYC)
    assert actual.index[-1] == pd.Timestamp('2000-01-01 10:10:00', tz=NYC)

    # now populate historical data with start_datetime
    mdm.load_history('stock', '1min', None, pd.Timestamp('2000-01-01 09:45:00'))
    actual = mdm.bars('stock', 'test.sym.1', '1min')
    assert len(actual) == 26
    assert actual.index[0] == pd.Timestamp('2000-01-01 09:45:00', tz=NYC)
    assert actual.index[-1] == pd.Timestamp('2000-01-01 10:10:00', tz=NYC)

    # test view is a view
    assert mdm.bar_data['stock']['1min']['test.sym.1'] is mdm.view('stock', 'test.sym.1', '1min')

    # add some more live data
    mdm.bartime = pd.Timestamp('2000-01-01 10:11:00')
    mdm.update('stock', '1min')
    actual = mdm.bars('stock', 'test.sym.1', '1min')
    assert len(actual) == 27
    assert actual.index[0] == pd.Timestamp('2000-01-01 09:45:00', tz=NYC)
    assert actual.index[-1] == pd.Timestamp('2000-01-01 10:11:00', tz=NYC)

    # assert that none of the 1min loading added 1D data
    assert_frame_equal(mdm.bars('stock', 'test.sym.2', '1D'),
                       rc.DataFrame(index_name='datetime', sort=True,
                                    columns=['open', 'high', 'low', 'close', 'volume']))


def test_load_history_1d():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', 'test.sym.2', '1D')

    # populate prior to live load
    mdm.bartime = pd.Timestamp('2010-01-05', tz=NYC)
    mdm.load_history('stock', '1D', None, pd.Timestamp('2010-01-02'))
    actual = mdm.bars('stock', 'test.sym.2', '1D')
    assert len(actual) == 3
    assert actual.index[0] == pd.Timestamp('2010-01-02', tz=default_time_zone)
    assert actual.index[-1] == pd.Timestamp('2010-01-04', tz=default_time_zone)

    # add live update
    mdm.bartime = pd.Timestamp('2010-01-06', tz=NYC)
    mdm.update('stock', '1D')
    actual = mdm.bars('stock', 'test.sym.2', '1D')
    assert len(actual) == 4
    assert actual.index[0] == pd.Timestamp('2010-01-02', tz=default_time_zone)
    assert actual.index[-1] == pd.Timestamp('2010-01-06', tz=default_time_zone)

    # reload history with same start date and nothing should happen
    mdm.load_history('stock', '1D', None, pd.Timestamp('2010-01-02'))
    actual = mdm.bars('stock', 'test.sym.2', '1D')
    assert len(actual) == 4
    assert actual.index[0] == pd.Timestamp('2010-01-02', tz=default_time_zone)
    assert actual.index[-1] == pd.Timestamp('2010-01-06', tz=default_time_zone)


def test_load_history_1d_eod():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', 'test.sym.9', '1D')

    # populate prior to live load
    mdm.bartime = pd.Timestamp('2010-01-05 09:30', tz=NYC)
    mdm.load_history('stock', '1D', None, pd.Timestamp('2010-01-04 16:00'))
    actual = mdm.bars('stock', 'test.sym.9', '1D')
    assert len(actual) == 1
    assert actual.index[0] == pd.Timestamp('2010-01-04 16:00', tz=default_time_zone)

    # add live update
    mdm.bartime = pd.Timestamp('2010-01-05 16:00', tz=NYC)
    mdm.update('stock', '1D')
    actual = mdm.bars('stock', 'test.sym.9', '1D')
    assert len(actual) == 2
    assert actual.index[0] == pd.Timestamp('2010-01-04 16:00', tz=default_time_zone)
    assert actual.index[1] == pd.Timestamp('2010-01-05 16:00', tz=default_time_zone)

    # reload history with same start date and nothing should happen
    mdm.load_history('stock', '1D', None, pd.Timestamp('2010-01-04 16:00'))
    actual = mdm.bars('stock', 'test.sym.9', '1D')
    assert len(actual) == 2
    assert actual.index[0] == pd.Timestamp('2010-01-04 16:00', tz=default_time_zone)
    assert actual.index[1] == pd.Timestamp('2010-01-05 16:00', tz=default_time_zone)


def test_load_history_fails():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', 'test.sym.2', '1D')

    # no bartime set
    with pytest.raises(ValueError):
        mdm.load_history('stock', '1D', None, pd.Timestamp('2010-01-02'))

    mdm.bartime = pd.Timestamp('2010-01-05')
    # No start datetime
    with pytest.raises(AttributeError):
        mdm.load_history('stock', '1D', None, start_datetime=None)


def test_update():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(None, ldm)

    mdm.add_symbols('stock', ['test.sym.3', 'AAPL', 'MSFT'], '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min', ['test.sym.3'])

    aapl = rc.DataFrame(index_name='datetime', sort=True, columns=['open', 'high', 'low', 'close', 'volume'])
    assert_frame_equal(mdm.view('stock', 'AAPL', '1min'), aapl)

    msft = rc.DataFrame(index_name='datetime', sort=True, columns=['open', 'high', 'low', 'close', 'volume'])
    assert_frame_equal(mdm.view('stock', 'MSFT', '1min'), msft)

    test_sym_3 = rc.DataFrame(index=[pd.Timestamp('2010-01-01 09:31:00', tz=NYC)], index_name='datetime',
                              data={'open': 100, 'high': 100.5, 'low': 99.5, 'close': 100.25, 'volume': 100}, sort=True)
    assert_frame_equal(mdm.view('stock', 'test.sym.3', '1min'), test_sym_3)

    # test update all
    mdm.update('stock', '1min')

    aapl.append_row(pd.Timestamp('2010-01-01 09:31:00', tz=NYC),
                    {'open': 52.0, 'high': 52.1, 'low': 51.5, 'close': 51.75, 'volume': 50})
    assert_frame_equal(mdm.view('stock', 'AAPL', '1min'), aapl)

    msft.append_row(pd.Timestamp('2010-01-01 09:31:00', tz=NYC),
                    {'open': 44.4, 'high': 44.5, 'low': 44.0, 'close': 44.0, 'volume': 900})
    assert_frame_equal(mdm.view('stock', 'MSFT', '1min'), msft)

    # test.sym.3 already updated
    assert_frame_equal(mdm.view('stock', 'test.sym.3', '1min'), test_sym_3)

    # test update only some
    mdm.bartime = '2010-01-01 09:32:00'
    mdm.update('stock', '1min', ['AAPL', 'MSFT'])

    aapl.append_row(pd.Timestamp('2010-01-01 09:32:00', tz=NYC),
                    {'open': 51.9, 'high': 52.71, 'low': 50.86, 'close': 51.62, 'volume': 125})
    assert_frame_equal(mdm.view('stock', 'AAPL', '1min'), aapl)

    msft.append_row(pd.Timestamp('2010-01-01 09:32:00', tz=NYC),
                    {'open': 43.9, 'high': 44.89, 'low': 43.04, 'close': 43.93, 'volume': 551})
    assert_frame_equal(mdm.view('stock', 'MSFT', '1min'), msft)

    assert_frame_equal(mdm.view('stock', 'test.sym.3', '1min'), test_sym_3)

    # update all with the same bartime, some will be overwritten some will add new data
    mdm.update('stock', '1min')

    assert_frame_equal(mdm.view('stock', 'AAPL', '1min'), aapl)
    assert_frame_equal(mdm.view('stock', 'MSFT', '1min'), msft)

    test_sym_3.append_row(pd.Timestamp('2010-01-01 09:32:00', tz=NYC),
                          {'open': 100.11, 'high': 100.5, 'low': 98.76, 'close': 99.33, 'volume': 161})
    assert_frame_equal(mdm.view('stock', 'test.sym.3', '1min'), test_sym_3)

    # test with bartime prior to the last datetime, this shouldn't happen so raise error if it does
    mdm._current_bartime = pd.Timestamp('2010-01-01 09:31:00', tz=NYC)  # this should never be called this way
    with pytest.raises(RuntimeError):
        mdm.update('stock', '1min')


def test_no_data():
    seng = symboldb.symbol_engine('stock', host="temp")
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    ldm = data_manager.LiveDataManager(symboldf, host='temp')
    mdm = data_manager.MarketDataManager(None, ldm)

    mdm.add_symbols('stock', 'test.sym.9', '1min')

    # load data before any data in database
    mdm.bartime = '2010-01-04 09:30:00'
    mdm.update('stock', '1min')
    expected_bars = rc.DataFrame(index=[pd.Timestamp('2010-01-04 09:30:00', tz='America/New_York')],
                                 index_name='datetime', sort=True,
                                 data={'open': None, 'high': None, 'low': None, 'close': None, 'volume': None})

    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1min'), expected_bars)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-1, as_dict=True)
    assert mdm.current_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') is None

    # load data for a datetime that exists
    mdm.bartime = '2010-01-04 16:00:00'
    mdm.update('stock', '1min')
    expected_bars.append_row(pd.Timestamp('2010-01-04 16:00:00', tz='America/New_York'),
                             {'open': 67.920000000000002, 'high': 68.739999999999995, 'low': 67.430000000000007,
                              'close': 67.980000000000004, 'volume': 104.0})

    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1min'), expected_bars)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-2, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 16:00:00', tz=NYC)) == \
           expected_bars.get_location(-1, as_dict=True)
    assert mdm.current_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)

    # load data for a datetime that does NOT exist
    mdm.bartime = '2010-01-05 09:30:00'
    mdm.update('stock', '1min')
    expected_bars.append_row(pd.Timestamp('2010-01-05 09:30:00', tz='America/New_York'),
                             {'open': None, 'high': None, 'low': None, 'close': None, 'volume': None})
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1min'), expected_bars)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-3, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 16:00:00', tz=NYC)) == \
           expected_bars.get_location(-2, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-05 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-1, as_dict=True)
    assert mdm.current_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-2, as_dict=True)

    # load another bar that does exist
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')
    expected_bars.append_row(pd.Timestamp('2010-01-05 09:31:00', tz='America/New_York'),
                             {'open': 67.890000000000001, 'high': 70.079999999999998, 'low': 67.859999999999999,
                              'close': 69.219999999999999, 'volume': 124})

    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1min'), expected_bars)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-4, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-04 16:00:00', tz=NYC)) == \
           expected_bars.get_location(-3, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-05 09:30:00', tz=NYC)) == \
           expected_bars.get_location(-2, as_dict=True)
    assert mdm.bar('stock', 'test.sym.9', '1min', pd.Timestamp('2010-01-05 09:31:00', tz=NYC)) == \
           expected_bars.get_location(-1, as_dict=True)
    assert mdm.current_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == expected_bars.get_location(-1, as_dict=True)


def test_extend_1d():
    seng = symboldb.symbol_engine('stock', host="temp")
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    hdm = data_manager.HistoricalDataManager(symboldf, host='temp')
    ldm = data_manager.LiveDataManager(symboldf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', 'test.sym.9', '1D')

    # extend with no current data and a blank live data
    mdm.bartime = pd.Timestamp('2010-01-04 09:30', tz=NYC)
    mdm.extend('stock', '1D')
    expected = rc.DataFrame(index_name='datetime', sort=True, columns=['open', 'high', 'low', 'close', 'volume'])
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)

    # extend with a bartime that has data
    mdm.bartime = pd.Timestamp('2010-01-04 16:00', tz=NYC)
    mdm.extend('stock', '1D')
    expected.append_row(pd.Timestamp('2010-01-04 16:00', tz=NYC),
                        {'open': 52.0, 'high': 68.95, 'low': 49.08, 'close': 67.98, 'volume': 48667})
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)

    # extend with a new end date that has data
    mdm.bartime = pd.Timestamp('2010-01-05 16:00', tz=NYC)
    mdm.extend('stock', '1D')
    expected.append_row(pd.Timestamp('2010-01-05 16:00', tz=NYC),
                        {'open': 67.89, 'high': 74.12, 'low': 52.16, 'close': 52.97, 'volume': 58671})
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)

    # extend to a new date that skips historical data, see that it is filled
    mdm.bartime = pd.Timestamp('2010-01-07 16:00', tz=NYC)
    mdm.extend('stock', '1D')
    expected.append_row(pd.Timestamp('2010-01-06 16:00', tz=NYC),
                        {'open': 53.28, 'high': 59.4, 'low': 35.27, 'close': 44.49, 'volume': 59182})
    expected.append_row(pd.Timestamp('2010-01-07 16:00', tz=NYC),
                        {'open': 44.37, 'high': 54.5, 'low': 7.83, 'close': 27.96, 'volume': 56950})
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)

    # extend to a new date that skips historical data, end no live data for that end date
    mdm.bartime = pd.Timestamp('2010-01-08 20:00', tz=NYC)
    mdm.extend('stock', '1D')
    expected.append_row(pd.Timestamp('2010-01-08 16:00', tz=NYC),
                        {'open': 27.79, 'high': 40.86, 'low': 20.65, 'close': 34.72, 'volume': 58076})
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)

    # no new data
    mdm.bartime = pd.Timestamp('2010-01-08 21:00', tz=NYC)
    mdm.extend('stock', '1D')
    assert_frame_equal(mdm.bars('stock', 'test.sym.9', '1D'), expected)


def test_bars():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed', time_zone='UTC')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm, time_zone='UTC')
    mdm.add_symbols('stock', ['test.sym.3', 'test.sym.1'], '1min')

    # call here of empty
    actual = mdm.bars('stock', 'test.sym.3', '1min')
    assert_frame_equal(actual, rc.DataFrame(index_name='datetime',
                                            sort=True, columns=['open', 'high', 'low', 'close', 'volume']))

    # populate some data and test
    for date in pd.date_range('2010-01-01 15:01:00', periods=10, freq='1min'):
        mdm.bartime = date
        mdm.update('stock', '1min')

    actual = mdm.bars('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 09:01:00', tz='America/Chicago'),
                      pd.Timestamp('2010-01-01 09:10:00', tz='America/Chicago'))
    assert len(actual) == 10

    # Can use Bar or standard python dict
    assert actual.get_location(4, as_dict=True) == structures.Bar(
        pd.Timestamp('2010-01-01 09:05:00', tz='America/Chicago'),
        93.04, 93.46, 91.81, 91.95, 154)

    actual = mdm.bars('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 9:31:00', tz=NYC),
                      pd.Timestamp('2010-01-01 10:05:00', tz=NYC))
    assert len(actual) == 5
    assert actual.get_location(0, as_dict=True) == {'datetime': pd.Timestamp('2010-01-01 10:01:00', tz=NYC),
                                                    'open': 94.35, 'high': 95.27, 'low': 94.24, 'close': 95.07,
                                                    'volume': 121}

    actual = mdm.bars('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 15:07:00', tz='UTC'),
                      pd.Timestamp('2010-01-01 15:50:00', tz='UTC'))
    assert len(actual) == 4
    assert actual.get_location(2, as_dict=True) == {'datetime': pd.Timestamp('2010-01-01 15:09:00', tz='UTC'),
                                                    'open': 91.3, 'high': 92.23, 'low': 90.9, 'close': 91.32,
                                                    'volume': 175}

    # test out of range on both sides
    assert_frame_equal(mdm.bars('stock', 'test.sym.3', '1min', pd.Timestamp('2009-06-06 9:30:00', tz=NYC),
                                pd.Timestamp('2009-06-06 10:00:00', tz=NYC)),
                       rc.DataFrame(index_name='datetime', sort=True,
                                    columns=['open', 'high', 'low', 'close', 'volume']))


def test_bar():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    hdm = data_manager.HistoricalDataManager(csvdf, host='temp')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(hdm, ldm)
    mdm.add_symbols('stock', ['test.sym.3', 'test.sym.1'], '1min')

    # when calling before any data, will raise error
    with pytest.raises(IndexError):
        mdm.bar('stock', 'test.sym.3', '1min')

    for date in pd.date_range('2010-01-01 10:01:00', periods=10, freq='1min', tz=NYC):
        mdm.bartime = date
        mdm.update('stock', '1min')

    # first bar (using Bar structure)
    assert mdm.bar('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 10:01:00', tz=NYC)) == \
           structures.Bar(pd.Timestamp('2010-01-01 10:01:00', tz=NYC), 94.35, 95.27, 94.24, 95.07, 121)

    # bar in the middle (standard python dict)
    assert mdm.bar('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 10:06:00', tz=NYC)) == \
           {'datetime': pd.Timestamp('2010-01-01 10:06:00', tz=NYC),
            'open': 92.13, 'high': 92.37, 'low': 91.95, 'close': 92.14, 'volume': 141}

    # bar at the end
    assert mdm.bar('stock', 'test.sym.3', '1min', pd.Timestamp('2010-01-01 10:10:00', tz=NYC)) == \
           {'datetime': pd.Timestamp('2010-01-01 10:10:00', tz=NYC),
            'open': 91.35, 'high': 93.35, 'low': 90.69, 'close': 92.70, 'volume': 199}

    # out of range on timestamp
    with pytest.raises(ValueError):
        assert mdm.bar('stock', 'test.sym.3', '1min', pd.Timestamp('1999-05-05 09:30:00', tz=NYC))


def test_current_bar():
    csvdf = datalib.CsvDataFeed(inst_dir / 'csv_data_feed')
    ldm = data_manager.LiveDataManager(csvdf, host='temp')
    mdm = data_manager.MarketDataManager(None, ldm)
    mdm.add_symbols('stock', ['test.sym.3', 'AAPL', 'MSFT'], '1min')
    mdm.bartime = '2010-01-01 09:31:00'
    mdm.update('stock', '1min')
    mdm.bartime = '2010-01-01 09:40:00'
    mdm.update('stock', '1min')

    assert mdm.current_bar('stock', 'MSFT', '1min') == structures.Bar(pd.Timestamp('2010-01-01 09:40:00', tz=NYC),
                                                                      42.92, 45.03, 41.94, 44.23, 563)

    # calling bar with no datetime is the same as calling current bar
    assert mdm.current_bar('stock', 'AAPL', '1min') == mdm.bar('stock', 'AAPL', '1min')


def test_last_valid_bar():
    seng = symboldb.symbol_engine('stock', host="temp")
    symboldf = datalib.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    ldm = data_manager.LiveDataManager(symboldf, host='temp')
    mdm = data_manager.MarketDataManager(None, ldm)

    mdm.add_symbols('stock', 'test.sym.9', '1min')

    # load data before any data in database
    mdm.bartime = '2010-01-04 09:30:00'
    mdm.update('stock', '1min')
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') is None

    # load data for a datetime that exists
    mdm.bartime = '2010-01-04 16:00:00'
    mdm.update('stock', '1min')
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == \
           structures.Bar(pd.Timestamp('2010-01-04 16:00:00', tz='America/New_York'), 67.920000000000002,
                          68.739999999999995, 67.430000000000007, 67.980000000000004, 104.0)

    # load data for a datetime that does NOT exist
    mdm.bartime = '2010-01-05 09:30:00'
    mdm.update('stock', '1min')
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == \
           {'datetime': pd.Timestamp('2010-01-04 16:00:00', tz='America/New_York'), 'open': 67.920000000000002,
            'high': 68.739999999999995, 'low': 67.430000000000007, 'close': 67.980000000000004, 'volume': 104.0}

    # load another bar that does exist
    mdm.bartime = '2010-01-05 09:31:00'
    mdm.update('stock', '1min')
    assert mdm.last_valid_bar('stock', 'test.sym.9', '1min') == \
           {'datetime': pd.Timestamp('2010-01-05 09:31:00', tz='America/New_York'), 'open': 67.890000000000001,
            'high': 70.079999999999998, 'low': 67.859999999999999, 'close': 69.219999999999999, 'volume': 124}
