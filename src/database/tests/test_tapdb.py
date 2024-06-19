"""
Unit tests for TAPDB (Trades and Positions Database)
"""

import database.utils as dbutils
import pandas as pd
import pytest
import raccoon as rc
import sqlalchemy
from database import tapdb
from pandas.testing import assert_frame_equal
from raccoon.utils import assert_frame_equal as rc_assert_frame_equal

# Global variables
prod_tapdb = None
temp_tapdb = None


def setup_module():
    global prod_tapdb, temp_tapdb
    prod_tapdb = tapdb.tapdb_engine(host='localhost')
    temp_tapdb = dbutils.make_engine('temp_tapdb', host='localhost')
    dbutils.copy_table_schema(prod_tapdb, temp_tapdb)


def teardown_module():
    prod_tapdb.dispose()
    temp_tapdb.dispose()


def test_engine():
    assert isinstance(prod_tapdb, sqlalchemy.engine.Engine)


def test_source_upsert():
    engine = temp_tapdb
    dbutils.copy_table_data(prod_tapdb, temp_tapdb)

    sources = tapdb.get_sources(engine)
    assert sources.empty

    tapdb.insert_source(engine, 'source1')
    tapdb.insert_source(engine, 'source2')
    expected = pd.DataFrame({'source_name': ['source1', 'source2']})

    actual = tapdb.get_sources(engine)
    assert_frame_equal(actual, expected)

    # not a valid string
    with pytest.raises(ValueError):
        tapdb.update_source(engine, 'source2', 123)

    tapdb.update_source(engine, 'source2', 'new_source')
    expected = pd.DataFrame({'source_name': ['source1', 'new_source']})

    actual = tapdb.get_sources(engine)
    assert set(actual['source_name'].tolist()) == set(expected['source_name'].tolist())


def test_position_insert():
    engine = temp_tapdb
    dbutils.copy_table_data(prod_tapdb, engine, ['source'])

    # get positions when empty
    expected = pd.DataFrame(columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])
    expected = expected.astype({'datetime': 'datetime64[ns]'})

    actual = tapdb.get_positions(engine)
    assert_frame_equal(actual, expected)

    # insert position row
    tapdb.insert_position(engine, 'test.source.1', 'test.strat.1', 'stock', 'test.sym.1',
                          pd.Timestamp('2017-02-07 16:00', tz='America/New_York'), 12345)

    expected = pd.DataFrame({'source': 'test.source.1', 'strategy': 'test.strat.1', 'product_type': 'stock',
                             'symbol': 'test.sym.1', 'datetime': pd.Timestamp('2017-02-07 21:00', tz='UTC'),
                             'position': 12345.0}, index=[0],
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(engine)
    assert_frame_equal(actual, expected)

    tapdb.insert_position(engine, 'test.source.1', 'test.strat.2', 'stock', 'test.sym.2',
                          pd.Timestamp('2017-02-07 16:00', tz='America/New_York'), 99.99)

    expected = pd.DataFrame({'source': ['test.source.1', 'test.source.1'], 'strategy': ['test.strat.1', 'test.strat.2'],
                             'product_type': ['stock', 'stock'], 'symbol': ['test.sym.1', 'test.sym.2'],
                             'datetime': [pd.Timestamp('2017-02-07 21:00', tz='UTC')] * 2,
                             'position': [12345.0, 99.99]}, index=[0, 1],
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(engine)
    assert_frame_equal(actual, expected)

    # insert with bad datetime (no time zone)
    with pytest.raises(TypeError):
        tapdb.insert_position(engine, 'test.source.1', 'test.strat.1', 'stock', 'test.sym.1',
                              pd.Timestamp('2017-02-07 16:00'), 12345)

    # insert with bad position value (string)
    with pytest.raises(ValueError):
        tapdb.insert_position(engine, 'test.source.1', 'test.strat.1', 'stock', 'test.sym.1',
                              pd.Timestamp('2017-02-07 16:00', tz='America/New_York'), "String")

    # insert a pandas DataFrame of positions
    positions = pd.DataFrame(
        {'source': ['test.source.2', 'test.source.1'], 'strategy': ['test.strat.2', 'test.strat.2'],
         'product_type': ['future', 'stock'], 'symbol': ['test_sym_4', 'test.sym.3'],
         'datetime': [pd.Timestamp('2017-02-07 16:30', tz='America/New_York')] * 2,
         'position': [-100, 150]})
    tapdb.insert_positions(engine, positions)

    expected = pd.DataFrame({'source': ['test.source.2', 'test.source.1', 'test.source.1', 'test.source.1'],
                             'strategy': ['test.strat.2', 'test.strat.2', 'test.strat.2', 'test.strat.1'],
                             'product_type': ['future', 'stock', 'stock', 'stock'],
                             'symbol': ['test_sym_4', 'test.sym.2', 'test.sym.3', 'test.sym.1'],
                             'datetime': [pd.Timestamp('2017-02-07 21:30', tz='UTC'),
                                          pd.Timestamp('2017-02-07 21:00', tz='UTC'),
                                          pd.Timestamp('2017-02-07 21:30', tz='UTC'),
                                          pd.Timestamp('2017-02-07 21:00', tz='UTC')],
                             'position': [-100.0, 99.99, 150.0, 12345.0]}, index=[0, 1, 2, 3],
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    actual = tapdb.get_positions(engine)
    actual = actual.sort_values('position').reset_index(drop=True)
    assert_frame_equal(actual, expected)

    # insert empty dataframe
    empty = pd.DataFrame(columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])
    tapdb.insert_positions(engine, empty)
    actual = tapdb.get_positions(engine)
    actual = actual.sort_values('position').reset_index(drop=True)
    assert_frame_equal(actual, expected)

    # test max_datetime using this temp data
    assert tapdb.max_datetime(engine) == pd.Timestamp('2017-02-07 21:30', tz='UTC')


def test_get_positions():
    # for doing the comparisons below
    def assert_results(act, exp, rows):
        act = act.sort_values('position').reset_index(drop=True)
        exp = exp.copy().iloc[rows].sort_values('position').reset_index(drop=True)
        assert_frame_equal(act, exp)

    engine = prod_tapdb
    # setup expected
    expected = pd.DataFrame({'source': ['test.source.1', 'test.source.1', 'test.source.2', 'test.source.1'],
                             'strategy': ['test.strat.1', 'test.strat.2', 'test.strat.2', 'test.strat.2'],
                             'product_type': ['stock', 'stock', 'future', 'stock'],
                             'symbol': ['test.sym.1', 'test.sym.2', 'test_sym_4', 'test.sym.3'],
                             'datetime': [pd.Timestamp('2000-02-07 21:00', tz='UTC'),
                                          pd.Timestamp('2000-02-07 21:00', tz='UTC'),
                                          pd.Timestamp('2000-02-07 21:30', tz='UTC'),
                                          pd.Timestamp('2000-02-07 21:30', tz='UTC')],
                             'position': [12345.0, 99.99, -100.0, 150.0]},
                            columns=['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position'])

    # get all expected
    actual = tapdb.get_positions(engine)
    assert len(actual) > 0

    # get by datetime
    actual = tapdb.get_positions(engine, datetime=pd.Timestamp('2000-02-07 21:30', tz='UTC'))
    assert_results(actual, expected, [2, 3])

    # get by source
    actual = tapdb.get_positions(engine, source='test.source.1')
    assert_results(actual, expected, [0, 1, 3])

    # get by strategy
    actual = tapdb.get_positions(engine, strategy='test.strat.2')
    assert_results(actual, expected, [1, 2, 3])

    # get by source + strategy
    actual = tapdb.get_positions(engine, source='test.source.1', strategy='test.strat.2')
    assert_results(actual, expected, [1, 3])

    # get by source + datetime
    actual = tapdb.get_positions(engine, source='test.source.1', datetime=pd.Timestamp('2000-02-07 21:00', tz='UTC'))
    assert_results(actual, expected, [0, 1])

    # get by source + strategy + datetime
    actual = tapdb.get_positions(engine, source='test.source.1', strategy='test.strat.2',
                                 datetime=pd.Timestamp('2000-02-07 21:00', tz='UTC'))
    assert_results(actual, expected, [1])


def test_max_datetime():
    engine = prod_tapdb

    assert tapdb.max_datetime(engine, source='test.source.1') == pd.Timestamp('2000-02-07 21:30', tz='UTC')
    assert tapdb.max_datetime(engine, source='test.source.2') == pd.Timestamp('2000-02-07 21:30', tz='UTC')
    assert tapdb.max_datetime(engine, strategy='test.strat.1') == pd.Timestamp('2000-02-07 21:00', tz='UTC')
    assert tapdb.max_datetime(engine, source='test.source.1', strategy='test.strat.1') == \
           pd.Timestamp('2000-02-07 21:00', tz='UTC')

    # test with no data
    engine = temp_tapdb
    dbutils.copy_table_data(prod_tapdb, engine, ['source'])
    assert tapdb.max_datetime(engine) is None
    assert tapdb.max_datetime(engine, source='test.source.1') is None
    assert tapdb.max_datetime(engine, strategy='test.strat.1') is None
    assert tapdb.max_datetime(engine, strategy='test.strat.1', source='test.source.1') is None


def test_positions_df():
    engine = temp_tapdb
    dbutils.copy_table_data(prod_tapdb, engine, ['source'])

    expected = rc.DataFrame({'current_position': 50.0, 'start_position': 0, 'net_quantity': 50.0,
                             'buy_quantity': 100.0, 'sell_quantity': 50.0, 'buy_avg_price': 10.0,
                             'sell_avg_price': 25.0, 'buy_pnl': 0.0, 'sell_pnl': 0.0, 'trade_pnl': 0.0,
                             'position_pnl': 0.0, 'gross_pnl': 0.0, 'commission': -1.5, 'net_pnl': 0.0,
                             'prior_close_price': None, 'current_price': None},
                            index=[('stat_id', 'stock', 'TEST')], sort=True,
                            index_name=('strategy_id', 'product_type', 'symbol'))

    tapdb.insert_positions_df(engine, 'test_unit', pd.Timestamp('2017-03-03 12:00:00', tz='America/New_York'), expected)
    actual = tapdb.get_positions_df(engine, 'test_unit', pd.Timestamp('2017-03-03 12:00:00', tz='America/New_York'))

    rc_assert_frame_equal(actual, expected)


def test_orders_df():
    engine = temp_tapdb
    dbutils.copy_table_data(prod_tapdb, engine, ['source'])

    expected = rc.DataFrame({'state': ['RISK_REJECTED', 'FILLED', 'CANCELED', 'FILLED', 'CANCELED', 'FILLED',
                                       'CANCELED', 'FILLED', 'CANCELED'],
                             'create_timestamp': [pd.Timestamp('2017-03-09 09:30', tz='America/New_York')] * 9,
                             'buy_sell': ['buy', 'buy', 'sell', 'sell', 'buy', 'buy', 'sell', 'buy', 'sell'],
                             'fill_quantity': [None, 25, None, 25, None, 50, None, 100, 52],
                             'fill_price': [None, 51.75, None, 52.25, None, 51.5, None, 51.6, 52.02],
                             'closed': [True, True, True, True, True, True, True, True, True],
                             'booked': [None, True, None, True, None, True, None, True, True]})

    # insert the json into the database
    tapdb.insert_orders_df(engine, 'test_unit', pd.Timestamp('2017-03-03 12:00:00', tz='America/New_York'), expected)

    # get the json from the database
    actual = tapdb.get_orders_df(engine, 'test_unit', pd.Timestamp('2017-03-03 12:00:00', tz='America/New_York'))
    # because the Timestamp gets conerted to representation in the to_json, turn back into the Timestamp
    actual['create_timestamp'] = [pd.Timestamp(x.split("'")[1], tz=x.split("'")[3])
                                  for x in actual['create_timestamp'].to_list()]

    rc_assert_frame_equal(actual, expected)
