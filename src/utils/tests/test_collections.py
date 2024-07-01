"""
Unit tests for collections module
"""
import statistics

import utils.collections as cutils
import numpy as np
import operator
import raccoon as rc
from raccoon.utils import assert_frame_equal


def test_flatten_list():
    assert cutils.flatten_list([1, 2, 3]) == [1, 2, 3]
    assert cutils.flatten_list([1, [2, 3]]) == [1, 2, 3]
    assert cutils.flatten_list([[1], [2, 3]]) == [1, 2, 3]
    assert cutils.flatten_list([[1], [[2, 4], 3]]) == [1, 2, 4, 3]


def test_list_and():
    assert cutils.list_and([], []) == []
    assert cutils.list_and([True], []) == []
    assert cutils.list_and([], [True]) == []
    assert cutils.list_and([False], [True]) == [False]
    assert cutils.list_and([False, True, True], [True, True, True]) == [False, True, True]
    assert cutils.list_and([False, True, True], [True, True, False]) == [False, True, False]
    assert cutils.list_and([False, True, True], [True, True, False, True]) == [False, True, False]


def test_invert_list_of_dict():
    a1 = {'a': 1, 'b': 4, 'c': 7}
    a2 = {'a': 2, 'b': 5, 'c': 8}
    a3 = {'z': 9, 'y': 3, 'w': 6}

    expected = {'a': [1, 2], 'b': [4, 5], 'c': [7, 8]}
    actual = cutils.invert_list_of_dict([a1, a2])
    assert actual == expected

    expected = {'a': [1, 2, None], 'b': [4, 5, None], 'c': [7, 8, None],
                'w': [None, None, 6], 'y': [None, None, 3], 'z': [None, None, 9]}
    actual = cutils.invert_list_of_dict([a1, a2, a3])
    assert actual == expected


def test_element_math():
    assert cutils.element_math([1, 3, 5], [6, 7, 8], operator.add) == [7, 10, 13]
    assert cutils.element_math([1, 3, 5], [6, 7, 8], operator.sub) == [-5, -4, -3]
    assert cutils.element_math([1, 3, 5], [6, 7, 8], operator.mul) == [6, 21, 40]
    assert cutils.element_math([1, 3, 5], [6, 7, 8], operator.truediv) == [1 / 6, 3 / 7, 5 / 8]


def test_strip_leading_none():
    # No Nones
    x = [2, 3, 4, 5]
    y = [6, 7, 8, 9]

    res_x, res_y = cutils.strip_leading_none(x, y)
    assert res_x == [2, 3, 4, 5]
    assert res_y == [6, 7, 8, 9]

    # same number of Nones
    x = [None, None, 2, 3, 4, 5]
    y = [np.nan, np.nan, 6, 7, 8, 9]

    res_x, res_y = cutils.strip_leading_none(x, y)
    assert res_x == [2, 3, 4, 5]
    assert res_y == [6, 7, 8, 9]

    # different length of Nones
    x = [None, None, 2, 3, 4, 5]
    y = [np.nan, np.nan, None, None, 8, 9]

    res_x, res_y = cutils.strip_leading_none(x, y)
    assert res_x == [4, 5]
    assert res_y == [8, 9]

    x = [None, None, None, np.nan, 4, 5]
    y = [np.nan, np.nan, 6, 7, 8, 9]

    res_x, res_y = cutils.strip_leading_none(x, y)
    assert res_x == [4, 5]
    assert res_y == [8, 9]


def test_aggregate():
    df = rc.DataFrame({'col_1': [1, 2, 3], 'col_2': [4, 5, 6]}, index=[('a', 'b'), ('d', 'b'), ('a', 'c')],
                      index_name=('first', 'second'), sort=False)

    expected = rc.DataFrame({'col_1': [4, 2], 'col_2': [10, 5]}, index=['a', 'd'], index_name='first',
                            columns=df.columns, sort=False)
    actual = cutils.aggregate_rc(df, 'first', sum)

    assert_frame_equal(actual, expected)

    expected = rc.DataFrame({'col_1': [1.5, 3], 'col_2': [4.5, 6]}, index=['b', 'c'], index_name='second',
                            columns=df.columns, sort=False)
    actual = cutils.aggregate_rc(df, 'second', statistics.mean)

    assert_frame_equal(actual, expected)


def test_sorted_in():
    assert cutils.sorted_in([1, 2, 4, 5], 2) is True
    assert cutils.sorted_in([1, 2, 4, 5], 1) is True
    assert cutils.sorted_in([1, 2, 4, 5], 5) is True
    assert cutils.sorted_in([1, 2, 4, 5], 0) is False
    assert cutils.sorted_in([1, 2, 4, 5], 3) is False
    assert cutils.sorted_in([1, 2, 4, 5], 6) is False
