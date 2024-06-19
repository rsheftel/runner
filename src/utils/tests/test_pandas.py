import pandas as pd
import raccoon as rc
from pandas.testing import assert_frame_equal
import utils.pandas as pd_utils
from raccoon.utils import assert_frame_equal as rc_assert_frame_equal


def test_rc_to_pd():
    # DataFrame input
    rc_df = rc.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"], index=[7, 8, 9])
    expected = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, columns=["a", "b"], index=[7, 8, 9])
    actual = pd_utils.rc_to_pd(rc_df)
    assert_frame_equal(actual, expected)

    # DataFrame series
    rc_srs = rc.Series([4, 5, 6], data_name="b", index=[7, 8, 9])
    expected = pd.DataFrame({"b": [4, 5, 6]}, index=[7, 8, 9])
    actual = pd_utils.rc_to_pd(rc_srs)
    assert_frame_equal(actual, expected)


def test_pd_to_rc():
    pd_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[5, 6, 7], columns=["a", "b"])
    expected = rc.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[5, 6, 7], columns=["a", "b"])
    actual = pd_utils.pd_to_rc(pd_df)
    rc_assert_frame_equal(actual, expected)

    pd_df = pd.DataFrame({"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"])
    pd_df.index.name = "hasName"
    expected = rc.DataFrame(
        {"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"], index_name="hasName", sort=False
    )
    actual = pd_utils.pd_to_rc(pd_df)
    rc_assert_frame_equal(actual, expected)

    expected = rc.DataFrame(
        {"a": [1, 2], "b": [4, 5]}, index=[5, 6], columns=["a", "b"], index_name="hasName", sort=True
    )
    actual = pd_utils.pd_to_rc(pd_df, sort=True)
    rc_assert_frame_equal(actual, expected)
