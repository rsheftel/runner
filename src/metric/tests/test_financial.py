import collections

import raccoon as rc

import data
from metric.financial import PositionManagerMetric
from metric.simple import Accumulate


def test_pnl():
    mdm = data.MarketDataManager(None, None)

    mock_df = rc.DataFrame(index_name=('strategy_id', 'product_type', 'symbol'), columns=['net_pnl'], sort=True)
    pm = collections.namedtuple('MockPositionManager', 'positions_df')(mock_df)

    pnl = PositionManagerMetric(mdm, pm, 'net_pnl', sum)
    pnl_test = PositionManagerMetric(mdm, pm, 'net_pnl', sum, symbol='TEST')
    pnl_strat02 = PositionManagerMetric(mdm, pm, 'net_pnl', sum, strategy_id='strat02')

    equity = Accumulate(mdm, pnl)

    mdm.bartime = '2017-05-01'
    assert pnl.value(0) == 0
    assert equity[0] == 0
    assert pnl_test[0] == 0
    assert pnl_strat02[0] == 0

    mdm.bartime = '2017-05-02'
    mock_df[('strat01', 'stock', 'TEST'), 'net_pnl'] = 50
    assert pnl.value(0) == 50
    assert equity[0] == 50
    assert pnl_test[0] == 50
    assert pnl_strat02[0] == 0

    mdm.bartime = '2017-05-03'
    mock_df[('strat01', 'stock', 'TEST'), 'net_pnl'] = -100
    mock_df[('strat01', 'stock', 'AAPL'), 'net_pnl'] = -100
    assert pnl.value(0) == -200
    assert pnl.value(-1) == 50
    assert equity[0] == -150
    assert pnl_test[0] == -100
    assert pnl_strat02[0] == 0

    mdm.bartime = '2017-05-04'
    mock_df[('strat01', 'stock', 'TEST'), 'net_pnl'] = -100
    mock_df[('strat02', 'stock', 'TEST'), 'net_pnl'] = 88
    mock_df[('strat01', 'stock', 'AAPL'), 'net_pnl'] = -100
    assert pnl.value(0) == -100 + 88 - 100
    assert equity[0] == -262
    assert pnl_test[0] == -100 + 88
    assert pnl_strat02[0] == 88
