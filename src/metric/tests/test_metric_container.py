"""
MetricContainer ABC unit tests
"""

import pandas as pd

from database import symboldb
import metric.unit_test as test_metrics

from import data
from data.structures import SymbolTuple
from metric.metric import Metric
from metric.metric_container import SignalContainer

mdm = None
seng = None


def setup_module():
    global mdm, seng
    
    
    seng = symboldb.symbol_engine('stock', host='localhost')
    symboldf = data.SymbolDBDataFeed({'stock': seng}, source='test_source_02')
    ldm = data.LiveDataManager(symboldf)
    mdm = data.MarketDataManager(None, ldm)


def teardown_module():
    seng.dispose()


def test_initialize():
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    mc = SignalContainer(mdm, test_metrics.UnitTest01, [(x, 'close') for x in symbols])

    assert isinstance(mc, SignalContainer)
    assert isinstance(mc.uuid, str)
    assert len(mc.metrics) == 3
    assert isinstance(mc.metrics[('stock', 'test.sym.9', '1min'), 'metric'], Metric)


def test_calculate():
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.10', 'test.sym.11']]
    # Use the UnitTest01 metric which is a basic accumulator
    mc = SignalContainer(mdm, test_metrics.UnitTest01, [(x, 'close') for x in symbols])

    datetimes = pd.date_range('2010-01-04 09:31:00', tz='America/New_York', freq='1min', periods=10)

    # initial calculation
    mdm.bartime = datetimes[0]
    mdm.update('stock', '1min')
    mc.calculate()

    assert mc.metrics[('stock', 'test.sym.9', '1min'), 'metric'][0] == 51.75
    assert mc.metrics[('stock', 'test.sym.10', '1min'), 'metric'][0] == 44.0
    assert mc.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 100.25

    # Next bar
    mdm.bartime = datetimes[1]
    mdm.update('stock', '1min')
    mc.calculate()

    assert mc.metrics[('stock', 'test.sym.9', '1min'), 'metric'][0] == 51.75 + 51.62
    assert mc.metrics[('stock', 'test.sym.10', '1min'), 'metric'][0] == 44.0 + 43.93
    assert mc.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 100.25 + 99.33
