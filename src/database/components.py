"""
Functions for the the TSDB components of a symbol
"""

# Constant definitions
price_components = ['open', 'high', 'low', 'close']
bar_components = ['open', 'high', 'low', 'close', 'volume', 'open_interest']
daily_components = ['split', 'dividend', 'front_contract', 'front_contract_tomorrow', 'net_asset_value']


def component_time_series_name(product_type, symbol, frequency, component):
    """
    Returns the standard tsdb time_series name for the component of a symbol

    :param product_type: product type
    :param symbol: symbol
    :param frequency: frequency in standard format
    :param component: component
    :return: string of time_series name
    """
    if component in bar_components:
        return symbol + "_" + product_type + "_" + frequency + "_" + component
    elif component in daily_components:
        return symbol + "_" + product_type + "_" + component
    else:
        raise ValueError("component not valid!")


def standard_components(product_type):
    """
    Get the standard components for a given product type.

    :param product_type: product type
    :return: list of component names
    """
    components = ['open', 'high', 'low', 'close', 'volume']
    if product_type == 'future':
        components.append('open_interest')
    return components


def additional_components(product_type, security_type=None):
    """
    Get the additional components for a given product_type and security_type

    :param str product_type: product type
    :param str security_type: security type, None will default to base, or 'ALL" for a superset of all security types
    :return: list of components names
    """
    if product_type == 'stock':
        components = ['split', 'dividend']
    elif product_type == 'future':
        components = ['front_contract', 'front_contract_tomorrow']
    else:
        raise ValueError(f'Bad product_type {product_type}')

    if security_type is None:
        pass
    elif (security_type == 'closed_end_fund') or (security_type.upper() == 'ALL'):
        components.extend(['net_asset_value'])

    return components


def all_components(product_type, security_type=None):
    """
    Returns all the components for a given product and security type

    :param str product_type: product type
    :param str security_type: security type, None will default to base, or 'ALL" for a superset of all security types
    :return: list of all components
    """
    return standard_components(product_type) + additional_components(product_type, security_type)
