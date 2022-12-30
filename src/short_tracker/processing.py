import logging
from datetime import date
from typing import Union

import pandas as pd

from short_tracker.data import (
    DATE_COL,
    FUND_COL,
    ISIN_COL,
    SHARE_ISSUER_COL,
    SHORT_POS_COL,
    CLOSE_COL,
    ADJ_CLOSE_COL,
    VOLUME_COL,
    TICKER_COL,
)

MKT_DATA_COLS = [CLOSE_COL, ADJ_CLOSE_COL, VOLUME_COL]

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
    tickers and concatenate to a single dataframe with the ticker as an
    additional column.
    """
    mkt_data_df_list = []

    for ticker, ticker_data in mkt_data.items():
        if ticker_data is None:
            continue

        ticker_data_ = ticker_data[MKT_DATA_COLS].assign(**{TICKER_COL: ticker})
        mkt_data_df_list.append(ticker_data_)
    return pd.concat(mkt_data_df_list)


def subset_top_shorts(cur_discl: pd.DataFrame, top_n: int):
    """Subset a dataframe of current disclosures on the top_n overall shorted
    names, and the top_n individual shorts.
    """
    top_sec_shorts = (
        cur_discl.groupby([SHARE_ISSUER_COL, ISIN_COL])[SHORT_POS_COL]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )
    top_fund_shorts = cur_discl.sort_values(by=SHORT_POS_COL, ascending=False).head(
        top_n
    )
    return top_sec_shorts, top_fund_shorts
