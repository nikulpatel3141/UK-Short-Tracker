"""For calculations once we have all required raw data"""

import json
import logging

from sqlalchemy import create_engine
import pandas as pd

from short_tracker.config import CONN_STR, UK_MKT_TICKER, TOP_N_SHORTS, OUT_FILE
from short_tracker.data import (
    DATE_COL,
    RET_COL,
    BM_RET_COL,
    SH_OUT_COL,
    AMOUNT_COL,
    PNL_COL,
    REL_PNL_COL,
    REL_RET_COL,
    EXPO_COL,
    SHORT_POS_COL,
    ADJ_CLOSE_COL,
    DTC_COL,
    VOLUME_COL,
    TICKER_COL,
)  # FIXME: too many imports...
from short_tracker.schemas import (
    SEC_METADATA_TABLE,
    MKT_DATA_TABLE,
    SHORT_DISCL_TABLE,
)
from short_tracker.processing import (
    calc_reindex_dates,
    prepare_discl_data,
    prepare_mkt_data,
    subset_top_shorts,
)

logger = logging.getLogger(__name__)


def query_all_db_data():
    """Query the database specified by CONN_STR for all disclosures data, market data
    and security metadata
    """
    engine = create_engine(CONN_STR)
    isin_ticker_map = pd.read_sql_table(SEC_METADATA_TABLE, con=engine)
    mkt_data = pd.read_sql_table(MKT_DATA_TABLE, con=engine, parse_dates=[DATE_COL])
    discl_data = pd.read_sql_table(
        SHORT_DISCL_TABLE, con=engine, parse_dates=[DATE_COL]
    )
    return discl_data, mkt_data, isin_ticker_map


def augment_discl_metrics(discl_data):
    """Append columns for various metrics to a copy of the
    given disclosures dataframe, specifically calculates and appends:
    - relative return to the benchmark
    - amount held
    - net expo
    - pnl (in GBP)
    - relative GBP pnl to the benchmark
    - days to cover
    """
    discl_data_ = discl_data.copy()
    discl_data_.loc[:, REL_RET_COL] = discl_data_[RET_COL] - discl_data_[BM_RET_COL]
    discl_data_.loc[:, AMOUNT_COL] = (
        discl_data_[SHORT_POS_COL] * discl_data_[SH_OUT_COL]
    )
    discl_data_.loc[:, EXPO_COL] = -discl_data_[AMOUNT_COL] * discl_data_[ADJ_CLOSE_COL]

    discl_data_.loc[:, PNL_COL] = discl_data_[EXPO_COL] * discl_data_[RET_COL]
    discl_data_.loc[:, REL_PNL_COL] = discl_data_[EXPO_COL] * discl_data_[REL_RET_COL]

    discl_data_.loc[:, DTC_COL] = discl_data_[AMOUNT_COL] / discl_data_[VOLUME_COL]
    return discl_data_


def summarise_short_discl(discl_data, mkt_data, isin_ticker_map, top_n):
    """Process the input data and calculate summary stats for the top_n disclosed shorts
    per fund and top_n overall shorts.
    """
    latest_rpt_date = discl_data[DATE_COL].max()
    reindex_dates = calc_reindex_dates(latest_rpt_date)
    mkt_data_concat = prepare_mkt_data(mkt_data, reindex_dates, UK_MKT_TICKER)

    def calc_discl_metrics(discl_subset):
        discl_data_ = prepare_discl_data(discl_subset, isin_ticker_map)
        discl_data_ = discl_data_.merge(
            mkt_data_concat, on=[DATE_COL, TICKER_COL], how="left"
        )
        return augment_discl_metrics(discl_data_)

    top_sec_shorts, top_fund_shorts = subset_top_shorts(discl_data, top_n)

    top_sec_aug = calc_discl_metrics(top_sec_shorts)
    top_fund_aug = calc_discl_metrics(top_fund_shorts)
    return top_sec_aug, top_fund_aug


def main():
    """Retrieve queried data, calculate metrics for the top disclosures
    and saves it to OUT_FILE as a json file.
    """
    logger.info("Retrieving existing data")
    discl_data, mkt_data, isin_ticker_map = query_all_db_data()

    logger.info("Calculating metrics...")
    top_sec_aug, top_fund_aug = summarise_short_discl(
        discl_data, mkt_data, isin_ticker_map, TOP_N_SHORTS
    )
    logger.info(f"Saving output as JSON to {OUT_FILE}")
    output = {"sec": top_sec_aug, "fund": top_fund_aug}
    with open(OUT_FILE, "w") as f:
        json.dump(output, f)
