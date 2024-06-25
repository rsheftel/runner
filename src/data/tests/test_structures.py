"""
unit test for the Bar data structure and associated functions
"""

from data import structures


def test_bar():
    assert structures.Bar('2017-01-01', 100, 101, 99, 100.5, 999) == {'datetime': '2017-01-01', 'open': 100,
                                                                      'high': 101, 'low': 99, 'close': 100.5,
                                                                      'volume': 999}


def test_symbol_tuple():
    struct = structures.SymbolTuple('stock', 'TEST', '1min')
    assert struct.product_type == 'stock'
    assert struct.symbol == 'TEST'
    assert struct.frequency == '1min'
