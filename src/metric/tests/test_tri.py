"""
Unit tests for TRI metric
"""

import os

import pandas as pd
import raccoon as rc
from numpy.testing import assert_almost_equal

from import data
from metric.regression import *
from metric.total_return_index import *
from utils.pandas import pd_to_rc

inst_dir = ''


def setup_module():
    global inst_dir
    inst_dir = os.path.normpath("./metric/tests/inst/")


def spin_data(mdm, met, all_data, y_data, x_data):
    for row in all_data.iterrows():
        datetime = row.pop('datetime')
        mdm.bartime = datetime
        y_data.append_row(datetime, {'Y': row['Y']})
        x_data.append_row(datetime, {'X': row['X']})
        met.value(0)


def test_tri_ewols():
    mdm = data.MarketDataManager(None, None)
    csv_data = pd.read_csv(os.path.join(inst_dir, 'tri_data.csv'), index_col=['datetime'])
    all_data = pd_to_rc(csv_data, False)

    y_data = rc.DataFrame(columns=['Y'])
    x_data = rc.DataFrame(columns=['X'])

    tri = TotalReturnIndex(mdm, (y_data, 'Y'), (x_data, 'X'), ExpWeightedOrdinaryLeastSquares, lag_bars=1,
                           window_length=10, half_life=5)

    spin_data(mdm, tri, all_data, y_data, x_data)

    expected = [None, 100, 99.8310309, 99.4165193, 99.7988057, 100.4893596, 100.0141613, 101.0246958, 100.7827847,
                101.7699951, 102.9850653, 102.9545866, 103.3463218, 104.7967256, 104.6052871, 104.2503002, 104.1796958,
                104.2322624, 103.9045877, 103.2934139, 102.7752798, 101.8336131, 101.6783092, 103.1346749, 102.3522037]

    assert tri.data.data[:1] == expected[:1]
    assert_almost_equal(tri.data.data[1:], expected[1:])

    # now with a 3 day lag for the change on change calculation
    mdm = data.MarketDataManager(None, None)
    csv_data = pd.read_csv(os.path.join(inst_dir, 'tri_data.csv'), index_col=['datetime'])
    all_data = pd_to_rc(csv_data, False)

    y_data = rc.DataFrame(columns=['Y'])
    x_data = rc.DataFrame(columns=['X'])

    tri = TotalReturnIndex(mdm, (y_data, 'Y'), (x_data, 'X'), ExpWeightedOrdinaryLeastSquares, lag_bars=3,
                           window_length=10, half_life=5)

    spin_data(mdm, tri, all_data, y_data, x_data)

    expected = [None, None, None, 100, 106.0433333, 106.6228682, 105.8630555, 106.9660898, 106.5460462, 107.7148307,
                108.942609, 108.9190469, 109.2460364, 110.6017178, 110.549785, 110.2772491, 110.115696, 110.2643165,
                109.9490265, 109.0769494, 108.222155, 107.1474948, 107.1781005, 108.8227192, 108.2343037]

    assert tri.data.data[:3] == expected[:3]
    assert_almost_equal(tri.data.data[3:], expected[3:])


def test_tri_fixed_dollar():
    mdm = data.MarketDataManager(None, None)
    csv_data = pd.read_csv(os.path.join(inst_dir, 'tri_data.csv'), index_col=['datetime'])
    all_data = pd_to_rc(csv_data, False)

    y_data = rc.DataFrame(columns=['Y'])
    x_data = rc.DataFrame(columns=['X'])

    # EqualDollarWeight with lag_bars=0 means a level-on-level regression
    tri = TotalReturnIndex(mdm, (y_data, 'Y'), (x_data, 'X'), EqualDollarWeighted, lag_bars=0, window_length=10)

    spin_data(mdm, tri, all_data, y_data, x_data)

    expected = [100.0, 99.0025, 99.3065587227917, 99.3671475420098, 99.8881238340147, 100.766487818155,
                100.662312884058, 101.595026007536, 101.462812616791, 102.364568516776, 103.524828723034,
                103.560351488632, 103.813395073956, 105.100463441986, 105.236818909439, 104.843290781139,
                104.486680090434, 104.282144311392, 103.9383131132, 103.563853732486, 103.161556138609,
                102.201423759145, 102.368952455496, 104.140865842156, 103.898937518386]

    assert_almost_equal(tri.data.data, expected)


def test_jagged_data():
    mdm = data.MarketDataManager(None, None)
    csv_data = pd.read_csv(os.path.join(inst_dir, 'tri_data.csv'), index_col=['datetime'])
    all_data = pd_to_rc(csv_data, False)

    # set the first 3 rows in Y to None, and then 2 rows to np.nan to make the data jagged
    all_data.set_locations([0, 1, 2], 'Y', None)
    all_data.set_locations([3, 4], 'Y', np.nan)

    y_data = rc.DataFrame(columns=['Y'])
    x_data = rc.DataFrame(columns=['X'])

    # EqualDollarWeight with lag_bars=0 means a level-on-level regression
    tri = TotalReturnIndex(mdm, (y_data, 'Y'), (x_data, 'X'), EqualDollarWeighted, lag_bars=0, window_length=10)

    spin_data(mdm, tri, all_data, y_data, x_data)

    expected = [None, None, None, None, None, 100, 99.902312182358, 100.833675784306, 100.704833113779,
                101.603721806046, 102.754042171716, 102.787232463631, 103.042082501788, 104.330541774474,
                104.464725213409, 104.071197085109, 103.714586394404, 103.510050615362, 103.16621941717,
                102.791760036456, 102.389462442579, 101.429330063115, 101.596858759466, 103.368772146126,
                103.126843822356]

    assert tri.data.data[:5] == expected[:5]
    assert_almost_equal(tri.data.data[5:], expected[5:])
