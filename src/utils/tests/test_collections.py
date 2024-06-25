"""
Unit tests for collections module
"""
import utils.collections as cutils
import operator


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
