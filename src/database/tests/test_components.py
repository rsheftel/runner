"""
Tests for components
"""

import pytest

from database import components


def test_component_ts_name():
    assert components.component_time_series_name('stock', 'AAPL', '1min', 'close') == 'AAPL_stock_1min_close'
    assert components.component_time_series_name('stock', 'AAPL', None, 'split') == 'AAPL_stock_split'
    assert components.component_time_series_name('stock', 'AAPL', '1min', 'net_asset_value') == \
           'AAPL_stock_net_asset_value'

    with pytest.raises(ValueError):
        components.component_time_series_name('stock', 'AAPL', '1D', 'BAD')


def standard_components():
    assert components.standard_components('stock') == ['open', 'high', 'low', 'close', 'volume']
    assert components.standard_components('future') == ['open', 'high', 'low', 'close', 'volume', 'open_interest']


def test_additional_components():
    assert components.additional_components('stock') == ['split', 'dividend']
    assert components.additional_components('stock', 'closed_end_fund') == ['split', 'dividend', 'net_asset_value']
    assert components.additional_components('stock', 'ALL') == ['split', 'dividend', 'net_asset_value']
    assert components.additional_components('stock', 'all') == ['split', 'dividend', 'net_asset_value']
    with pytest.raises(ValueError):
        components.additional_components('BAD')


def test_all_components():
    assert components.all_components('stock') == ['open', 'high', 'low', 'close', 'volume', 'split', 'dividend']
    assert components.all_components('stock', 'closed_end_fund') == \
           ['open', 'high', 'low', 'close', 'volume', 'split', 'dividend', 'net_asset_value']
    assert components.all_components('stock', 'all') == \
           ['open', 'high', 'low', 'close', 'volume', 'split', 'dividend', 'net_asset_value']
