"""
Utilities for lists / dicts and other collections
"""


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
