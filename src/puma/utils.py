"""
Tomahawk utilities
"""

import os
import sys
from typing import Union

import numpy as np
import pandas as pd
from numpy.testing import assert_almost_equal
from raccoon.utils import assert_frame_equal

import utils.pandas as pdutils
from config.datetime import default_time_zone
from database import tapdb


def assert_orders_equal(order_left, order_right, check_state=True, check_state_df=True, check_fills_df=True,
                        check_id=True, check_replaces=False):
    """
    Assert function for unit testing to compare two Order objects.

    :param order_left: first Order object
    :param order_right: second Order object
    :param check_state: If True then check the Order state
    :param check_state_df: If True then check the Order state_df (states only, not the timestamps)
    :param check_fills_df: If True then check the Order fills_df
    :param check_id: If True then check that the object IDs are the same, that the orders are the same object
    :param check_replaces: If True then check the Order replaces DataFrame
    :return: nothing
    """
    assert order_left.originator_id == order_right.originator_id
    assert order_left.strategy_id == order_right.strategy_id
    assert order_left.symbol == order_right.symbol
    assert order_left.buy_sell == order_right.buy_sell
    assert order_left.quantity == order_right.quantity
    assert order_left.type == order_right.type
    assert order_left.details == order_right.details
    assert order_left.fill_price == order_right.fill_price
    assert order_left.fill_quantity == order_right.fill_quantity
    assert order_left.commission == order_right.commission
    assert order_left.booked == order_right.booked
    assert order_left.closed == order_right.closed

    if check_id:
        assert id(order_left) == id(order_right)

    if check_state:
        assert order_left.state == order_right.state

    if check_state_df:
        state_df_left = order_left.state_df
        state_df_right = order_right.state_df
        assert state_df_left['state'].to_list() == state_df_right['state'].to_list()

    if check_fills_df:
        assert_frame_equal(order_left.fills, order_right.fills)

    if check_replaces:
        assert_frame_equal(order_left.replaces, order_right.replaces)


def assert_positions_df(engine, directory: str, source: str, datetime: Union[str, pd.Timestamp]) -> None:
    """
    Assert function to test the persisted positions_df DataFrames that are stored in TAPDB with frozen csv files.
    Will raise an error if the DataFrames are different. Only compares the columns that are in the csv, so missing
    columns are not detected.

    :param engine: sqlalchemy engine for TAPDB
    :param directory: directory of the csv files
    :param source: source name
    :param datetime: datetime
    :return: nothing
    """
    if isinstance(datetime, str):
        datetime = pd.Timestamp(datetime).tz_localize(default_time_zone)
    datetime_str = datetime.strftime('%Y-%m-%d_%H-%M-%S')

    # load expected from file
    filename = os.path.join(directory, source + '_positions_df_' + datetime_str + '.csv')
    expected = pd.read_csv(filename, index_col="('strategy_id', 'product_type', 'symbol')")
    expected = expected.replace(np.nan, None)
    # convert the index from string to tuple
    expected = pdutils.pd_to_rc(expected)
    # index tuples get loaded as strings, turn into tuples
    expected.index_name = eval(expected.index_name)
    expected.index = [eval(x) for x in expected.index]
    expected.sort = True

    # get actual from database
    actual = tapdb.get_positions_df(engine, source, datetime)
    actual = actual[expected.columns]

    # compare
    try:
        assert_frame_equal(actual, expected, assert_almost_equal, {'decimal': 5})
    except Exception as e:
        print(f'datetime:{datetime}', file=sys.stderr)
        print(f'\nexpected:\n{expected}', file=sys.stderr)
        print(f'\nactual:\n{actual}', file=sys.stderr)
        raise e


def assert_orders_df(engine, directory: str, source: str, datetime: Union[str, pd.Timestamp]) -> None:
    """
    Assert function to test the persisted orders_df DataFrames that are stored in TAPDB with frozen csv files.
    Will raise an error if the DataFrames are different. Only compares the columns that are in the csv, so missing
    columns are not detected.

    :param engine: sqlalchemy engine for TAPDB
    :param directory: directory of the csv files
    :param source: source name
    :param datetime: datetime
    :return: nothing
    """
    if isinstance(datetime, str):
        datetime = pd.Timestamp(datetime).tz_localize(default_time_zone)
    datetime_str = datetime.strftime('%Y-%m-%d_%H-%M-%S')

    # load expected from file
    filename = os.path.join(directory, source + '_orders_df_' + datetime_str + '.csv')
    expected = pd.read_csv(filename, index_col=None)
    expected = expected.replace(np.nan, None)
    expected = pdutils.pd_to_rc(expected)
    if 'details' in expected.columns:
        expected['details'] = [eval(x) for x in expected['details'].to_list()]
    expected.sort = True

    # get actual from database
    actual = tapdb.get_orders_df(engine, source, datetime)
    actual = actual[expected.columns]

    # compare
    try:
        assert_frame_equal(actual, expected)
    except Exception as e:
        print(f'datetime:{datetime}', file=sys.stderr)
        print(f'\nexpected:\n{expected}', file=sys.stderr)
        print(f'\nactual:\n{actual}', file=sys.stderr)
        raise e


def assert_persisted_dfs(engine, directory: str, source: str, datetime: Union[str, pd.Timestamp]) -> None:
    """
    Assert function to test the persisted DataFrames that are stored in TAPDB with frozen csv files.
    Will raise an error if the DataFrames are different. Only compares the columns that are in the csv, so missing
    columns are not detected.

    :param engine: sqlalchemy engine for TAPDB
    :param directory: directory of the csv files
    :param source: source name
    :param datetime: datetime
    :return: nothing
    """
    assert_positions_df(engine, directory, source, datetime)
    assert_orders_df(engine, directory, source, datetime)
