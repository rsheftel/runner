"""
Unit test Tomahawk utils
"""

from pathlib import Path

import pandas as pd
import pytest
import raccoon as rc

import puma.utils as twutils

from database import tapdb, utils as dbutils

# Global variables
inst_dir = ""
temp_tapdb = None


def setup_module():
    global inst_dir, temp_tapdb
    
    inst_dir = Path(__file__).parent / "inst"
    prod_tapdb = tapdb.tapdb_engine(host='temp')
    temp_tapdb = dbutils.make_engine('temp_tapdb', host='temp')
    dbutils.copy_table_schema(prod_tapdb, temp_tapdb)
    dbutils.copy_table_data(prod_tapdb, temp_tapdb, include_tables=['source'])


def teardown_module():
    temp_tapdb.dispose()


def test_assert_orders_df():
    orders_df = rc.DataFrame({'strategy_id': ['test_unit'] * 2, 'product_type': ['stock'] * 2,
                              'symbol': ['test.sym.9'] * 2, 'buy_sell': ['buy', 'sell'], 'closed': [True] * 2,
                              'quantity': [50, 100], 'details': [{'price': 49.5}, {'price': 70.25}],
                              'state': ['FILLED', 'CANCELED'], 'fill_price': [49.5, None], 'fill_quantity': [50, None],
                              'commission': [-0.5, None], 'booked': [True, None]}, sort=True)

    # assert working. product_type missing from csv file, should still work
    date = pd.Timestamp('2010-01-04 16:00', tz='America/New_York')
    tapdb.insert_orders_df(temp_tapdb, 'test_unit', date, orders_df)
    twutils.assert_orders_df(temp_tapdb, inst_dir, 'test_unit', date)

    # change the input value, should fail
    date = pd.Timestamp('2010-01-05 16:00', tz='America/New_York')
    tapdb.insert_orders_df(temp_tapdb, 'test_unit', date, orders_df)
    # this will fail because quantity in the csv file does not match the TAPDB values
    with pytest.raises(AssertionError):
        twutils.assert_orders_df(temp_tapdb, inst_dir, 'test_unit', date)


def test_assert_positions_df():
    # the csv file has 0.000001 on some of the values that the TAPDB will not to test rounding
    positions_df = rc.DataFrame({'current_position': [50], 'start_position': [0], 'buy_quantity': [50],
                                 'sell_quantity': [0], 'net_quantity': [50], 'sell_avg_price': [0],
                                 'buy_avg_price': [49.5],
                                 'buy_pnl': [125.0], 'sell_pnl': [0.0], 'trade_pnl': [125.0], 'position_pnl': [799.0],
                                 'gross_pnl': [924.0], 'commission': [-0.5], 'net_pnl': [923.5],
                                 'current_price': [67.98], 'prior_close_price': [52.0]},
                                sort=True, index=[('test_04', 'stock', 'test.sym.9')],
                                index_name=('strategy_id', 'product_type', 'symbol'))

    # assert working. Note 'start_position' missing from the csv, should still work
    date = pd.Timestamp('2010-01-04 16:00', tz='America/New_York')
    tapdb.insert_positions_df(temp_tapdb, 'test_unit', date, positions_df)
    twutils.assert_positions_df(temp_tapdb, inst_dir, 'test_unit', date)

    # change the input value, should fail
    date = pd.Timestamp('2010-01-05 16:00', tz='America/New_York')
    tapdb.insert_positions_df(temp_tapdb, 'test_unit', date, positions_df)
    # this will fail because quantity in the csv file does not match the TAPDB values
    with pytest.raises(AssertionError):
        twutils.assert_positions_df(temp_tapdb, inst_dir, 'test_unit', date)
