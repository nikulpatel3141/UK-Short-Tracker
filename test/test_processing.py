import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import pytest

from short_tracker.processing import ffill_fund_discl_data, calc_fund_short_flow_bounds

TEST_DATES = [
    "2022-01-03",
    "2022-01-04",
    "2022-01-05",
    "2022-01-06",
    "2022-01-07",
    "2022-01-10",
]

DISCL_DATES = TEST_DATES[::2]
TEST_DISCL_VALS = [0.6, 0.4, 0.7]

# FIXME: should be a fixture??? More convenient like this
FUND_DISCL_DATA = pd.Series(TEST_DISCL_VALS, index=DISCL_DATES)
FUND_DISCL_DATA.index = pd.to_datetime(FUND_DISCL_DATA.index)


@pytest.mark.parametrize(
    ["discl_threshold", "exp_ffill_data"],
    [
        [0, FUND_DISCL_DATA.reindex(TEST_DATES, method="ffill")],
        [0.5, pd.Series([0.6, 0.6, 0.4, np.nan, 0.7, 0.7], index=TEST_DATES)],
        [1, FUND_DISCL_DATA.reindex(TEST_DATES)],
    ],
)
def test_ffill_fund_discl_data(
    discl_threshold: float,
    exp_ffill_data: pd.Series,
):
    max_date = TEST_DATES[-1]
    act_ffill_data = ffill_fund_discl_data(FUND_DISCL_DATA, max_date, discl_threshold)
    act_ffill_data = act_ffill_data.reindex(exp_ffill_data.index)
    print(act_ffill_data)
    print(exp_ffill_data)
    assert act_ffill_data.equals(exp_ffill_data)


@pytest.mark.parametrize(
    ["discl_threshold", "exp_flow_bound"],
    [
        [0, pd.Series([0.6, 0.0, -0.2, 0.0, 0.3, 0.0], index=TEST_DATES)],
        [0.5, pd.Series([0.1, 0.0, -0.2, 0.0, 0.2, 0.0], index=TEST_DATES)],
        [1, FUND_DISCL_DATA.reindex(TEST_DATES).fillna(0) * 0.0],
    ],
)
def test_short_flow_bounds(
    discl_threshold: float,
    exp_flow_bound: pd.Series,
):
    max_date = TEST_DATES[-1]  # FIXME: repetition
    ffill_data = ffill_fund_discl_data(FUND_DISCL_DATA, max_date, discl_threshold)
    act_flow_bound = calc_fund_short_flow_bounds(ffill_data, discl_threshold)
    act_flow_bound = act_flow_bound.reindex(exp_flow_bound.index)
    print(exp_flow_bound)
    print(act_flow_bound)
    assert np.isclose(act_flow_bound - exp_flow_bound, 0).all()
