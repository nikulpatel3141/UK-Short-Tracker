import logging
from datetime import date
from typing import Union
from functools import reduce

import pandas as pd

from short_tracker.config import METRICS_LOOKBACK, ADV_CALC_LOOKBACK
from short_tracker.utils import n_bdays_ago
from short_tracker.data import (
    DATE_COL,
    FUND_COL,
    ISIN_COL,
    ITEM_COL,
    SHARE_ISSUER_COL,
    SHORT_POS_COL,
    CLOSE_COL,
    ADJ_CLOSE_COL,
    VALUE_COL,
    VOLUME_COL,
    BM_RET_COL,
    SH_OUT_COL,
    TICKER_COL,
    RET_COL,
)

MKT_DATA_COLS = [CLOSE_COL, ADJ_CLOSE_COL, VOLUME_COL]

_REINDEX_BUFFER = 10

logger = logging.getLogger(__name__)


def check_cur_hist_discl_overlap(cur_discl, hist_discl) -> bool:
    """Check if the reported current disclosed positions overlap with
    any of the historical positions.
    """
    idx = [DATE_COL, FUND_COL, ISIN_COL]
    overlap_ind = cur_discl.set_index(idx).index.isin(hist_discl.set_index(idx).index)
    return overlap_ind.any()


def ffill_fund_discl_data(
    discl_data: pd.Series,
    max_date: Union[date, None],
    discl_threshold: float,
) -> pd.Series:
    """Reindex the given disclosures series (indexed on date) for a single fund
    using the following logic:
    - Reindex on business days from the minimum date in the data to max_date
    - Forward fill if the current position is >= the discl_threshold

    Args:
    - max_date: reindex up to this date. If None given then use the
    max date in the data instead.
    """
    idx = discl_data.index

    if not max_date:
        max_date = idx.max()

    tgt_idx = pd.bdate_range(idx.min(), max_date, name=idx.name)
    reindexed_data = discl_data.reindex(tgt_idx)

    data_ffill = reindexed_data.ffill()
    data_ffill = data_ffill.where(data_ffill >= discl_threshold)
    return reindexed_data.fillna(data_ffill)


def ffill_discl_data(
    discl_data: pd.DataFrame,
    max_date: Union[date, None],
    discl_threshold: float,
) -> pd.DataFrame:
    """Same as ffill_fund_discl_data but for multiple funds"""
    ffill_discl_data_ = lambda x: ffill_fund_discl_data(
        x.set_index(DATE_COL),
        max_date=max_date,
        discl_threshold=discl_threshold,
    )
    return (
        discl_data.groupby([FUND_COL, SHARE_ISSUER_COL, ISIN_COL])
        .apply(ffill_discl_data_)
        .reset_index()
    )


def remove_dupl_shorts(discl_df: pd.DataFrame) -> pd.DataFrame:
    """Checks for duplicate disclosures per fund + date + isin.
    If a duplicate is found:
    - silently drop if the net short values are equal
    - otherwise log the duplicated rows and take the first one

    Args:
        discl_df: a df of disclosures per date + fund + isin

    Returns:
        pd.DataFrame: of the input disclosures but with duplicates removed
        as described above

    Warning: doesn't detect duplicates from mismatching isins (though this is an unlikely
    edge case...)
    """
    discl_df_ = discl_df.drop_duplicates()
    prim_key_cols = [FUND_COL, SHARE_ISSUER_COL, ISIN_COL, DATE_COL]

    dupl_rows = discl_df_.duplicated(subset=prim_key_cols, keep=False)
    num_dupl_rows = dupl_rows.sum()

    if num_dupl_rows:
        logger.warning(
            f"Found {num_dupl_rows} duplicated rows: "
            f"{discl_df_.loc[dupl_rows].to_string()}"
        )
        logger.warning("Assuming the max disclosure is correct...")
        discl_df_ = discl_df_.groupby(prim_key_cols).max()
    return discl_df_.reset_index()


def calc_fund_short_flow_bounds(
    ffill_discl_data: pd.Series,
    discl_threshold: float,
):
    """Calculate a lower bound for movement into/out of disclosed short positions.
    If all funds are disclosed this is equivalent to .diff(), otherwise take into account
    the threshold,

    Examples:
    - if a fund newly discloses a 0.8% short with a 0.5% threshold, then the
    minimum flow is 0.3% since they could've been just under the threshold previously.
    - if a fund covers a 0.8% short then the minimum flow is -0.3%
    """
    discl_diff = ffill_discl_data.diff()  # nan when boundary is crossed
    threshold_discl = ffill_discl_data.where(
        discl_diff.isna() & (ffill_discl_data >= discl_threshold)
    )
    threshold_cross_bound = threshold_discl - discl_threshold
    return discl_diff.fillna(threshold_cross_bound).fillna(0)


def extract_sec_tickers(sec_metadata: dict) -> dict:
    """Take a response from querying OpenFIGI as returned by `query_sec_metadata`
    and extract a map from queried identifier to ticker. Will pick the first returned ticker
    in the list if multiple are returned.

    #FIXME: suboptimal way to deal with ambiguous tickers,
    # should return for further processing.
    """
    id_ticker_map = {}

    for id_, id_data in sec_metadata.items():
        tickers = list({x["ticker"] for x in id_data})
        if len(tickers) > 1:
            logger.warning(
                f"Ambiguous tickers for id {id_}: {tickers}. Picking the first one..."
            )
        ticker = tickers[0]
        id_ticker_map[id_] = ticker
    return id_ticker_map


def process_mkt_data(mkt_data: dict) -> pd.DataFrame:
    """Take a dict of market data returned by `query_mkt_data` keyed on
    tickers and concatenate to a single long dataframe with columns date, ticker,
    item (close, volume, ...), value.
    """
    mkt_data_df_list = []

    for ticker, ticker_data in mkt_data.items():
        if ticker_data is None:
            continue

        ticker_data_ = ticker_data[MKT_DATA_COLS].assign(**{TICKER_COL: ticker})
        mkt_data_df_list.append(ticker_data_)
    mkt_data_df = pd.concat(mkt_data_df_list)

    mkt_data_df_stacked = (
        mkt_data_df.rename_axis(index=DATE_COL, columns=ITEM_COL)
        .set_index(TICKER_COL, append=True)
        .stack()
        .rename(VALUE_COL)
        .reset_index()
    )
    return mkt_data_df_stacked


def subset_top_shorts(cur_discl: pd.DataFrame, top_n: int):
    """Subset a dataframe of current disclosures on the top_n overall shorted
    names, and the top_n individual shorts.
    """
    top_sec_shorts = (
        cur_discl.groupby([DATE_COL, SHARE_ISSUER_COL, ISIN_COL])[SHORT_POS_COL]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )
    top_fund_shorts = cur_discl.sort_values(by=SHORT_POS_COL, ascending=False).head(
        top_n
    )
    return top_sec_shorts, top_fund_shorts


def reindex_mkt_data(mkt_data, reindex_dates, ffill=True, bfill=False):
    """Reindex market data on the given dates with optional forward and backwards filling.

    Returns: a df of market data reindexed as above with the same columns as the input
    data (ticker, date, item, value).
    """
    mkt_data_ = mkt_data.pivot(
        index=DATE_COL, columns=TICKER_COL, values=[VALUE_COL]
    ).reindex(reindex_dates)

    if ffill:
        mkt_data_ = mkt_data_.ffill()
    if bfill:
        mkt_data_ = mkt_data_.bfill()
    return mkt_data_.stack().reset_index()


def reindex_rename_mkt_data(
    mkt_data, reindex_dates, item, val_name, bfill=False, ffill=True
):
    """Call reindex_mkt_data on the subset where ITEM_COL=item and rename the
    value column to the given name.
    """
    df = reindex_mkt_data(
        mkt_data[mkt_data[ITEM_COL] == item],
        reindex_dates,
        bfill=bfill,
        ffill=ffill,
    )
    return df.rename(columns={VALUE_COL: val_name})


def calc_reindex_dates(latest_rpt_date):
    """Calculate a sequence of business dates to reindex over before
    joining data.
    """
    lookback_days = METRICS_LOOKBACK + _REINDEX_BUFFER
    lookback_date = n_bdays_ago(lookback_days, latest_rpt_date)
    reindex_dates = pd.bdate_range(lookback_date, latest_rpt_date, name=DATE_COL)
    return reindex_dates


def calc_returns(close_prices_df, reindex_dates):
    """Calculate returns using the given market data for (adjusted) close prices
    FIXME: messy
    """
    adj_close = reindex_rename_mkt_data(
        close_prices_df, reindex_dates, ADJ_CLOSE_COL, ADJ_CLOSE_COL, bfill=True
    )
    adj_close.loc[:, ADJ_CLOSE_COL] = adj_close[ADJ_CLOSE_COL] / 100  # GBX to GBP
    adj_close = adj_close.sort_values(by=[TICKER_COL, DATE_COL])

    calc_ret = lambda df: df.set_index(DATE_COL)[[ADJ_CLOSE_COL]].pct_change()
    returns = (
        adj_close.groupby(TICKER_COL)
        .apply(calc_ret)
        .rename(columns={ADJ_CLOSE_COL: RET_COL})
        .reset_index()
    )
    return adj_close, returns


def calc_adv(volume_data, lookback) -> pd.DataFrame:
    """Calculate a (flat) estimate of average daily trading volume"""
    vol = volume_data.sort_values(by=[TICKER_COL, DATE_COL]).drop(columns=[ITEM_COL])
    adv = (
        vol.set_index(DATE_COL)
        .groupby(TICKER_COL)[VALUE_COL]
        .rolling(lookback, min_periods=1)
        .mean()
        .rename(VOLUME_COL)
        .reset_index()
    )
    return adv


def prepare_discl_data(discl_data, isin_ticker_map):
    """Join on a ticker -> isin mapping and normalise the short position column
    for the given disclosures df.
    """
    discl_data.loc[:, SHORT_POS_COL] = discl_data.loc[:, SHORT_POS_COL] / 100

    discl_data_ = discl_data.merge(isin_ticker_map, on=[ISIN_COL], how="left")
    return discl_data_


def prepare_mkt_data(mkt_data, reindex_dates, bm_ticker):
    """Prepare market data for joining with short disclosures data
    - reindex all data to
    - calculate returns + benchmark returns
    """
    adj_close, returns = calc_returns(mkt_data, reindex_dates)

    bm_returns = (
        returns[returns[TICKER_COL] == bm_ticker]
        .rename(columns={RET_COL: BM_RET_COL})
        .drop(columns=[TICKER_COL])
    )

    sh_out = reindex_rename_mkt_data(
        mkt_data, reindex_dates, SH_OUT_COL, SH_OUT_COL, bfill=True
    )

    _grp_shift = lambda df: df.set_index(DATE_COL)[[ADJ_CLOSE_COL]].shift()
    adj_close_shifted = adj_close.groupby(TICKER_COL).apply(_grp_shift).reset_index()

    adv = calc_adv(mkt_data[mkt_data[ITEM_COL] == VOLUME_COL], ADV_CALC_LOOKBACK)

    to_merge = [sh_out, adj_close_shifted, returns, adv]

    merge_df_fn = lambda x, y: x.merge(y, on=[DATE_COL, TICKER_COL], how="outer")
    mkt_data_concat = reduce(merge_df_fn, to_merge)
    mkt_data_concat = mkt_data_concat.merge(bm_returns, on=[DATE_COL], how="outer")
    return mkt_data_concat
