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


from short_tracker.config import DB_FILE, MAX_DATA_AGE, TOP_N_SHORTS, UK_MKT_TICKER
from short_tracker.utils import setup_logging
from short_tracker.data import (
    query_uk_si_disclosures,
    query_all_sec_metadata,
    query_mkt_data,
    query_quotes,
    SHORT_URL_UK,
    FCA_DATE_COL,
    DATE_COL,
    ISIN_COL,
    ITEM_COL,
    VALUE_COL,
)
from short_tracker.processing import subset_top_shorts, extract_sec_tickers

logger = logging.getLogger(__name__)

OPENFIGI_PARAMS = {"idType": "ID_ISIN", "exchCode": "LN"}
_QUERY_DAYS_BUFFER = 10  # add to # of days to query beyond MAX_DATA_AGE


def query_uk_si_disclosures_() -> pd.DataFrame:
    """Fetch the UK SI disclosures from FCA and return the current disclosures
    labelled with today's date.
    """
    logger.info("Attempting to query for UK short disclosures data")
    discl_data, report_date = query_uk_si_disclosures(SHORT_URL_UK)

    logger.info(f"Retrieved UK short disclosures data with report date {report_date}")

    # track as of date instead of date filed
    cur_discl = discl_data["current"].drop(columns=[FCA_DATE_COL])
    cur_discl.loc[:, DATE_COL] = pd.to_datetime(report_date)
    return cur_discl


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


def query_mkt_data_(tickers):
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

    date_now = datetime.today().date()
    query_start = date_now - timedelta(days=MAX_DATA_AGE + _QUERY_DAYS_BUFFER)

    mkt_data = {
        tkr: query_mkt_data(qry_tkr, query_start) for tkr, qry_tkr in ticker_map.items()
    }
    quotes = {tkr: query_quotes(qry_tkr) for tkr, qry_tkr in ticker_map.items()}


def update_db(
    discl_data: pd.DataFrame,
):
    pass


def main():
    cur_discl = query_uk_si_disclosures_()

    top_shorts = subset_top_shorts(cur_discl, TOP_N_SHORTS)
    isins = pd.concat([k[ISIN_COL] for k in top_shorts]).unique()

    logger.info(f"Found {len(isins)} isins to query data for")


if __name__ == "__main__":
    setup_logging(logger)
    main()
