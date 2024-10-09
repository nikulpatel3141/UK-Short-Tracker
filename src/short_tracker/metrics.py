"""For calculations once we have all required raw data"""

import json
import logging

from sqlalchemy import create_engine
import pandas as pd
import numpy as np

from short_tracker.utils import n_bdays_ago, setup_logging
from short_tracker.config import (
    CONN_STR,
    METRICS_LOOKBACK,
    UK_MKT_TICKER,
    TOP_N_SHORTS,
    OUT_FILE,
)
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
    ISIN_COL,
    SHARE_ISSUER_COL,
    FUND_COL,
    POS_DIFF_COL,  # TODO change to pos change instead of expo change
)
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


DISPL_COLS = [
    SHORT_POS_COL,
    EXPO_COL,
    POS_DIFF_COL,
    RET_COL,
    REL_RET_COL,
    PNL_COL,
    REL_PNL_COL,
    DTC_COL,
]

GBP_K_COLS = [PNL_COL, REL_PNL_COL]
GBP_M_COLS = [EXPO_COL]
PCT_COLS = [SHORT_POS_COL, RET_COL, REL_RET_COL, POS_DIFF_COL]
FLOAT_COLS = [DTC_COL]

# FIXME: repetition
format_ccy_k = lambda x: f"{'-' if x < 0 else ''}£{abs(x*1e-3):,.0f}k"
format_ccy_mm = lambda x: f"{'-' if x < 0 else ''}£{abs(x*1e-6):,.1f}M"
format_date = lambda x: x.strftime("%Y-%m-%d")
FORMAT_DICT = {
    **{k: format_ccy_k for k in GBP_K_COLS},
    **{k: format_ccy_mm for k in GBP_M_COLS},
    **{k: "{:.1f}" for k in FLOAT_COLS},
    **{k: lambda x: f"{100*x:.1f}%" for k in PCT_COLS},
}
ODD_ROW_COL = "#d4d4d4"
EVEN_ROW_COL = "#8c8c8c"
TBL_BORDER = "1px"
TBL_STYLES = [
    {"selector": "tr:nth-child(odd)", "props": f"background-color: {ODD_ROW_COL}"},
    {"selector": "tr:nth-child(even)", "props": f"background-color: {EVEN_ROW_COL}"},
    {"selector": "tr:hover", "props": "background-color: yellow"},
    {"selector": "th", "props": "background-color: #346eeb"},
]


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


def _subset_index(df, df_subset, index_cols):
    """Subsets df on df_susbet where the values in df[index_cols] are
    in df_subset[index_cols]
    """
    idx = df_subset.set_index(index_cols).index
    subset_ind = df.set_index(index_cols).index.isin(idx)
    return df.loc[subset_ind]


def susbet_hist_disclosures(discl_df, top_n, end_date):
    """Subset the disclosures on the top_n current disclosures, but use the
    latest disclosures to subset the previous disclosures too.
    """
    cur_discl = discl_df[discl_df[DATE_COL] == end_date]
    top_sec_shorts, top_fund_shorts = subset_top_shorts(cur_discl, top_n)
    top_sec_shorts_lookback = _subset_index(discl_df, top_sec_shorts, [ISIN_COL])
    top_sec_shorts_lookback = (
        top_sec_shorts_lookback.groupby(
            [ISIN_COL, TICKER_COL, SHARE_ISSUER_COL, DATE_COL]
        )[SHORT_POS_COL]
        .sum()
        .reset_index()
    )

    top_fund_shorts_lookback = _subset_index(
        discl_df, top_fund_shorts, [ISIN_COL, FUND_COL]
    )
    return top_sec_shorts_lookback, top_fund_shorts_lookback


def calc_display_metrics(lookback_discl_df):
    """Aggregates returns, pnl and calculates overall change in exposure
    for the given disclosures, grouped by ticker and also fund if given.
    """
    grp_cols = [TICKER_COL]

    if FUND_COL in lookback_discl_df:
        grp_cols = [*grp_cols, FUND_COL]

    lookback_discl_df_ = lookback_discl_df.sort_values(by=[TICKER_COL, DATE_COL])
    lookback_discl_df_ = augment_discl_metrics(lookback_discl_df_)

    cumulate_ret = lambda x: np.nanprod(1 + x) - 1
    top_tail_diff = lambda x: x.iloc[0] - x.iloc[-1]

    aggfuncs = {
        SHARE_ISSUER_COL: pd.NamedAgg(column=SHARE_ISSUER_COL, aggfunc="last"),  # FIXME
        SHORT_POS_COL: pd.NamedAgg(column=SHORT_POS_COL, aggfunc="last"),
        RET_COL: pd.NamedAgg(column=RET_COL, aggfunc=cumulate_ret),
        # REL_RET_COL: pd.NamedAgg(column=REL_RET_COL, aggfunc=cumulate_ret),
        PNL_COL: pd.NamedAgg(column=PNL_COL, aggfunc="sum"),
        # REL_PNL_COL: pd.NamedAgg(column=REL_PNL_COL, aggfunc="sum"),
        DTC_COL: pd.NamedAgg(column=DTC_COL, aggfunc="last"),
        EXPO_COL: pd.NamedAgg(column=EXPO_COL, aggfunc="last"),
        POS_DIFF_COL: pd.NamedAgg(column=SHORT_POS_COL, aggfunc=top_tail_diff),
    }
    return lookback_discl_df_.groupby(grp_cols).agg(**aggfuncs)


def summarise_short_discl(discl_data, mkt_data, isin_ticker_map):
    """Process the input data and calculate summary stats for the top_n disclosed shorts
    per fund and top_n overall shorts.
    """
    latest_rpt_date = discl_data[DATE_COL].max()
    lookback_date = n_bdays_ago(METRICS_LOOKBACK, latest_rpt_date)
    reindex_dates = calc_reindex_dates(latest_rpt_date)
    mkt_data_concat = prepare_mkt_data(mkt_data, reindex_dates, UK_MKT_TICKER)

    discl_data_ = prepare_discl_data(discl_data, isin_ticker_map)

    dt_cond = (discl_data_[DATE_COL] <= latest_rpt_date) & (
        discl_data_[DATE_COL] >= lookback_date
    )
    lookback_discl = discl_data_[dt_cond]

    top_sec_shorts_lookback, top_fund_shorts_lookback = susbet_hist_disclosures(
        lookback_discl, TOP_N_SHORTS, latest_rpt_date
    )
    top_sec_shorts_lookback_ = top_sec_shorts_lookback.merge(
        mkt_data_concat, on=[DATE_COL, TICKER_COL], how="left"
    )
    top_fund_shorts_lookback_ = top_fund_shorts_lookback.merge(
        mkt_data_concat, on=[DATE_COL, TICKER_COL], how="left"
    )

    fund_short_metrics = calc_display_metrics(top_fund_shorts_lookback_)
    sec_short_metrics = calc_display_metrics(top_sec_shorts_lookback_)

    return sec_short_metrics, fund_short_metrics


def style_metrics_df(metrics_df, report_date):
    """Style the output metrics df:
    - format the values according to FORMAT_DICT (currency, percent, floats, dates)
    - color the headers in blue and rows in alternating colors
    - add a background gradient for the short position
    - add bars for the pnl column
    """
    if FUND_COL not in metrics_df.index.names:
        caption_info = "overall"
    else:
        caption_info = "individual"

    caption = f"""
    Top {len(metrics_df)} {caption_info} disclosed UK shorts as of {format_date(report_date)}
    """
    metrics_df_ = metrics_df.sort_values(by=SHORT_POS_COL, ascending=False).reset_index()

    return (
        metrics_df_.style.format(formatter=FORMAT_DICT)
        .hide(axis="index")
        .set_table_styles(TBL_STYLES)
        .background_gradient(subset=PNL_COL, cmap="RdYlGn", vmin=-5e5, vmax=5e5)
        .set_caption(caption)
    )


def main():
    """Retrieve queried data, calculate metrics for the top disclosures
    and saves it to OUT_FILE as a json file.
    """
    logger.info("Retrieving existing data")
    discl_data, mkt_data, isin_ticker_map = query_all_db_data()

    report_date = discl_data[DATE_COL].max()

    logger.info("Calculating metrics...")
    sec_short_metrics, fund_short_metrics = summarise_short_discl(
        discl_data, mkt_data, isin_ticker_map
    )

    sec_short_metrics_styled = style_metrics_df(sec_short_metrics, report_date)
    fund_short_metrics_styled = style_metrics_df(fund_short_metrics, report_date)

    logger.info(f"Saving output as JSON to {OUT_FILE}")
    output = {
        "sec": sec_short_metrics_styled.to_html(),
        "fund": fund_short_metrics_styled.to_html(),
    }

    with open(OUT_FILE, "w") as f:
        json.dump(output, f)

    logger.info("Done!")


if __name__ == "__main__":
    setup_logging(logging.getLogger())
    main()
