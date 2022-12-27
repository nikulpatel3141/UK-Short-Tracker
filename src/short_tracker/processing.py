import logging
from datetime import date
from typing import Union

import pandas as pd

from short_tracker.data import DATE_COL, FUND_COL, ISIN_COL, SHARE_ISSUER_COL

logger = logging.getLogger(__name__)


def check_cur_hist_discl_overlap(cur_discl, hist_discl) -> bool:
    """Check if the reported current disclosed positions overlap with
    any of the historical positions.
    """
    idx = [DATE_COL, FUND_COL, ISIN_COL]
    overlap_ind = cur_discl.set_index(idx).index.isin(hist_discl.set_index(idx).index)
    return overlap_ind.any()


def reindex_discl_data(
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
    tgt_idx = pd.bdate_range(idx.min(), max_date)
    reindexed_data = discl_data.reindex(tgt_idx)

    data_ffill = reindexed_data.ffill()
    data_ffill = data_ffill.where(data_ffill >= discl_threshold)
    return reindexed_data.fillna(data_ffill)


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

    dupl_rows = discl_df_.duplicated(subset=prim_key_cols)
    num_dupl_rows = dupl_rows.sum()

    if num_dupl_rows:
        logger.warning(
            f"Found {num_dupl_rows} duplicated rows: "
            f"{discl_df_.loc[dupl_rows].to_string()}"
        )
        logger.warning("Assuming the max disclosure is correct...")
        return discl_df_.groupby(prim_key_cols).max()
    return discl_df_
