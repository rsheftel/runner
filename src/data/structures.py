"""
Data structures that holds market data bars and associated
"""

from types import MappingProxyType
from typing import NamedTuple

SymbolTuple = NamedTuple('SymbolTuple', [('product_type', str), ('symbol', str), ('frequency', str)])


# noinspection PyPep8Naming
def Bar(datetime, open, high, low, close, volume):
    """
    Creates a dict of the Bar structure

    :param datetime: datetime
    :param open: open
    :param high: high
    :param low: low
    :param close: close
    :param volume: volume
    :return: immutable dict
    """
    return MappingProxyType({'datetime': datetime,
                             'open': open, 'high': high, 'low': low, 'close': close, 'volume': volume})
