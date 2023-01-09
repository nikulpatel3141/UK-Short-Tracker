"""Query all data for the UK short monitor and stores in an SQLite file.
Keeps only recent data (# of days specified in the config module).

1 - Queries UK Short disclosures data from the FCA website.
    - Note: saves only the current disclosures
2 - Take the TOP_N_SHORTS overall disclosures + fund disclosures and queries
    ticker data from OpenFIGI and market data (prices + shares outstanding)
    for these securities only

Warning: overwrite the filing date with the report date for simplicity

Usage:
$ python3 query_discl_data.py
"""

from datetime import datetime, timedelta
import logging

import pandas as pd
from sqlalchemy import create_engine

from short_tracker.config import (
    CONN_STR,
    MAX_DATA_AGE,
    TOP_N_SHORTS,
    UK_MKT_TICKER,
)
from short_tracker.schemas import (
    SEC_METADATA_TABLE,
    MKT_DATA_TABLE,
    SHORT_DISCL_TABLE,
    seed_db,
)
from short_tracker.utils import setup_logging, n_bdays_ago
from short_tracker.data import (
    FUND_COL,
    ITEM_COL,
    TICKER_COL,
    VALUE_COL,
    FCA_DATE_COL,
    DATE_COL,
    ISIN_COL,
    SH_OUT_COL,
    MKT_CAP_COL,
    query_uk_si_disclosures,
    query_all_sec_metadata,
    query_mkt_data,
    query_quotes,
    SHORT_URL_UK,
)
from short_tracker.processing import (
    subset_top_shorts,
    extract_sec_tickers,
    process_mkt_data,
)

logger = logging.getLogger(__name__)

OPENFIGI_PARAMS = {"idType": "ID_ISIN", "exchCode": "LN"}
_QUERY_DAYS_BUFFER = 10  # add to # of days to query beyond MAX_DATA_AGE


def query_uk_si_disclosures_():
    """Fetch the UK SI disclosures from FCA and return the current disclosures
    labelled with today's date.
    """
    logger.info("Attempting to query for UK short disclosures data")
    discl_data, report_date = query_uk_si_disclosures(SHORT_URL_UK)

    logger.info(f"Retrieved UK short disclosures data with report date {report_date}")

    # track as of date instead of date filed
    cur_discl = discl_data["current"].drop(columns=[FCA_DATE_COL])
    cur_discl.loc[:, DATE_COL] = pd.to_datetime(report_date)
    return cur_discl, report_date


def query_ticker_map(isins):
    """Query tickers from OpenFIGI for the given isins

    Returns:
    - a dict from isin -> ticker
    """
    sec_metadata, err_isins = query_all_sec_metadata(isins, OPENFIGI_PARAMS)
    if err_isins:
        logger.warning(
            f"No OpenFIGI data returned for {len(err_isins)} isins: {err_isins}"
        )

    isin_ticker_map = extract_sec_tickers(sec_metadata)
    return isin_ticker_map


def query_mkt_data_(tickers, report_date) -> pd.DataFrame:
    """Query shares outstanding + price data from Yahoo Finance.

    Also process the data:
    - reshape to a long df with columns DATE_COL, TICKER_COL, ITEM_COL, VALUE_COL
    - ITEM_COL is 'Close', 'Adj Close', 'Shares Outstanding' etc

    Returns: a df of market data as above

    #FIXME: assumes appending '.L' to tickers gives the corresponding
    Yahoo finance ticker. Reasonable since all securities are traded in the UK
    but could probably do better using OpenFIGI data.
    """
    query_tickers = [k.rstrip("/") + ".L" for k in tickers]
    ticker_map = dict(zip(tickers, query_tickers))

    query_start = n_bdays_ago(MAX_DATA_AGE + _QUERY_DAYS_BUFFER)
    mkt_data = {
        tkr: query_mkt_data(qry_tkr, query_start) for tkr, qry_tkr in ticker_map.items()
    }
    quotes = {tkr: query_quotes(qry_tkr) for tkr, qry_tkr in ticker_map.items()}

    mkt_data_ = process_mkt_data(mkt_data)

    quotes_df = pd.DataFrame(quotes)
    sh_out = quotes_df.loc[SH_OUT_COL]
    missing_sh_out = list(sh_out[sh_out.isna()].index)

    if missing_sh_out:
        logger.warning(f"No share outstanding data for tickers: {missing_sh_out}")

    sh_out_ = sh_out.rename_axis(TICKER_COL).rename(VALUE_COL).to_frame().reset_index()

    # BUG: actually one trading day ago, but shouldn't make too much of a difference
    sh_out_.loc[:, DATE_COL] = pd.to_datetime(report_date)
    sh_out_.loc[:, ITEM_COL] = SH_OUT_COL

    mkt_data = pd.concat([mkt_data_, sh_out_])
    mkt_data.loc[:, DATE_COL] = pd.to_datetime(mkt_data[DATE_COL])
    mkt_data.loc[:, VALUE_COL] = pd.to_numeric(mkt_data[VALUE_COL])
    return mkt_data


def concat_old_new_data(
    new_data: pd.DataFrame,
    old_data: pd.DataFrame,
    start_date,
    index_cols: list,
    date_col=DATE_COL,
) -> pd.DataFrame:
    """Convenience function to concatenate old and new data and overwriting
    the old data with the new data where the index_cols overlap. Also truncate
    the data to have dates >= the start date
    """
    new_data_ = pd.concat([new_data, old_data])
    new_data_ = new_data_.drop_duplicates(subset=index_cols, keep="first")
    return new_data_[new_data_[date_col] >= pd.to_datetime(start_date)]


def update_db(
    discl_data: pd.DataFrame,
    mkt_data: pd.DataFrame,
    isin_ticker_map: dict,
    report_date: datetime.date,
):
    """Update the database specified by CONN_STR to:
    1 - delete data beyond the allowed data age (specified by MAX_DATA_AGE)
    2 - upload new SI disclosure + market data + security metadata

    Notes:
    - We replace all price + volume data + security metadata for convenience
    """
    engine = create_engine(CONN_STR)

    start_date = n_bdays_ago(MAX_DATA_AGE, report_date)
    logger.info(f"Will delete data in db older than {start_date}")

    isin_ticker_map_df = (
        pd.Series(isin_ticker_map, name=TICKER_COL).rename_axis(ISIN_COL).reset_index()
    )

    date_cond = f"""
    {DATE_COL} > '{start_date}'
    AND {DATE_COL} < '{report_date}'
    """

    existing_shout_query = f"""
    SELECT * from {MKT_DATA_TABLE}
    WHERE item = '{SH_OUT_COL}' AND
    {date_cond}
    """

    existing_discl_query = f"""
    select * from {SHORT_DISCL_TABLE}
    WHERE {date_cond}
    """

    existing_shout = pd.read_sql_query(
        existing_shout_query, con=engine, parse_dates=[DATE_COL]
    )
    existing_discl = pd.read_sql_query(
        existing_discl_query, con=engine, parse_dates=[DATE_COL]
    )

    upl_mkt_data = concat_old_new_data(
        mkt_data, existing_shout, start_date, [DATE_COL, ITEM_COL, TICKER_COL]
    )
    upl_discl_data = concat_old_new_data(
        discl_data,
        existing_discl,
        start_date,
        [DATE_COL, FUND_COL, ISIN_COL],
    )

    def upload_data(df, table, con):
        delete_stmt = f"DELETE FROM {table}"
        con.execute(delete_stmt)
        df.to_sql(name=table, index=False, if_exists="append", con=con)

    to_upload = [
        [SEC_METADATA_TABLE, isin_ticker_map_df],
        [MKT_DATA_TABLE, upl_mkt_data],
        [SHORT_DISCL_TABLE, upl_discl_data],
    ]

    with engine.begin() as con:
        for (tbl, df) in to_upload:
            upload_data(df, tbl, con)


def main():
    seed_db()  # if db tables aren't already created
    cur_discl, report_date = query_uk_si_disclosures_()

    top_shorts = subset_top_shorts(cur_discl, TOP_N_SHORTS)
    isins = pd.concat([k[ISIN_COL] for k in top_shorts]).unique()

    logger.info(f"Found {len(isins)} isins to query data for")

    logger.info(f"Querying for tickers from OpenFIGI")
    isin_ticker_map = query_ticker_map(isins)
    tickers = [UK_MKT_TICKER, *isin_ticker_map.values()]

    logger.info(f"Querying for market data from Yahoo Finance")
    mkt_data = query_mkt_data_(tickers, report_date)

    logger.info(f"Attempting to update database")
    update_db(
        cur_discl,
        mkt_data,
        isin_ticker_map,
        report_date,
    )

    logger.info("Finished!")


if __name__ == "__main__":
    setup_logging(logging.getLogger())
    main()
