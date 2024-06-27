"""
Metric Event Processor test suite
"""

from database import symboldb
import metric.metric_container as mc
import metric.unit_test as test_metrics
import puma as tw
import data.data_manager as dutils
import pandas as pd
from data.structures import SymbolTuple
from pytest import approx

# Global variables
seng = None


def setup_module():
    global seng
    seng = symboldb.symbol_engine('stock', host='localhost')


def teardown_module():
    seng.dispose()


def setup_objects():
    mdm = dutils.market_data_manager('SymbolDBDataFeed', engines={'stock': seng}, source='test_source_02')

    # setup metric objects
    # Use the UnitTest01 metric which is a basic accumulator
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.9', 'test.sym.11']]
    container01 = mc.SignalContainer(mdm, test_metrics.UnitTest01, [(x, 'close') for x in symbols])

    # Use the UnitTest05 of dual moving averages
    symbols = [SymbolTuple(product_type='stock', frequency='1min', symbol=x) for x
               in ['test.sym.10', 'test.sym.11']]
    container05 = mc.SignalContainer(mdm, test_metrics.UnitTest05, [(x, 'close') for x in symbols],
                                     length_fast=2, length_slow=3)

    return mdm, container01, container05


def test_initialize():
    mdm, container01, container05 = setup_objects()
    mp = tw.MetricProcessor([container01, container05], mdm)

    assert isinstance(mp.uuid, str)
    assert isinstance(mp.metric_containers, list)
    assert isinstance(mp.metric_containers[0], mc.MetricContainer)


def test_process_bar():
    mdm, container01, container05 = setup_objects()
    mp = tw.MetricProcessor([container01, container05], mdm)

    datetimes = pd.date_range('2010-01-04 09:31:00', tz='America/New_York', freq='1min', periods=10)

    # initial calculation
    mdm.bartime = datetimes[0]
    mp.process_bar('1min')

    assert container01.metrics[('stock', 'test.sym.9', '1min'), 'metric'][0] == 51.75
    assert container01.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 100.25

    assert container05.metrics[('stock', 'test.sym.10', '1min'), 'metric'][0] == 0
    assert container05.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 0

    # now run for 5 times and get results
    for x in range(1, 5):
        mdm.bartime = datetimes[x]
        mdm.update('stock', '1min')
        mp.process_bar('1min')

    assert container01.metrics[('stock', 'test.sym.9', '1min'), 'metric'][0] == 258.74
    assert container01.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == 493.22

    assert container05.metrics[('stock', 'test.sym.10', '1min'), 'metric'][0] == approx(0.15)
    assert container05.metrics[('stock', 'test.sym.11', '1min'), 'metric'][0] == approx(-0.34)
