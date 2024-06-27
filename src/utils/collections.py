"""
Utilities for lists / dicts and other collections
"""

import more_itertools
import numpy as np


def flatten_list(input_list):
    """
    Takes a list of lists and flattens to a simple list. Any number of levels works.

    :param input_list: List with multiple lists inside of lists
    :return: single flat list
    """

    return [y for l in input_list for y in flatten_list(l)] if type(input_list) is list else [input_list]


def list_and(left_list, right_list):
    """
    An element-wise and for two lists of booleans. It will compare each element in each list at the same position
    and return a list with the result of the AND comparisons. If lists are different lengths the length of the shorter
    list is used and the longer list is truncated.

    :param left_list: first list to compare
    :param right_list: second list to compare
    :return: list of booleans
    """
    return [x & y for x, y in zip(left_list, right_list)]


def invert_list_of_dict(dictionaries):
    """
    Takes in a list of dicts and returns a dict of lists. All the dicts should have the same keys. If any dict is
    missing any of the keys, that dict will not be included in the results

    :param dictionaries: list of dictionaries
    :return: dictionary of lists
    """
    all_keys = set(flatten_list([list(x.keys()) for x in dictionaries]))
    return {k: [x.get(k, None) for x in dictionaries] for k in all_keys}


def element_math(left_list, right_list, operator):
    """
    Performs element wise math operations on two lists using a function from the standard operator module.

    :param left_list: list to use as the left side of the operation, in the operator module docs this is "a"
    :param right_list: list to use as the right side of the operation, in the operator module docs this is "b"
    :param operator: function from the operator module
    :return: list of results
    """
    return [operator(x, y) for x, y in zip(left_list, right_list)]


def strip_leading_none(x, y):
    """
    Strips the leading None or np.nan from the x and y lists. If they are not of same length after striping then the
    longer list will be truncated on the left to be the same length as the shorter. The truncation is by dropping values
    at the beginning of the list.

    :param x: list of values
    :param y: list of values
    :return: x, y with leading None removed and lengths the same
    """
    y = list(more_itertools.lstrip(y, lambda i: (i is None) or np.isnan(i)))
    x = list(more_itertools.lstrip(x, lambda i: (i is None) or np.isnan(i)))
    if len(x) < len(y):
        y = y[len(y) - len(x):]
    elif len(x) > len(y):
        x = x[len(x) - len(y):]
    return x, y
