import time
import requests
import logging
from datetime import datetime, time
from dateutil.parser import parse
from typing import Union

from pandas_datareader import data as pdr
from tenacity import retry, retry_if_exception_type, wait_fixed
import yfinance as yf
import pandas as pd

SHORT_URL_UK = "https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx"  # FIXME: this should be somewhere else...
ALPHA_V_KEY = "0E6I0C40CVTM5U1M"

# earliest time UK equity markets will close, short disclosures to be filed after this
UK_MKT_EARLY_CLOSE = time(hour=12, minute=30)
UK_DISCL_THRESHOLD = 0.5

# FIXME: most of these should go in the schema module
# Columns from UK's FCA short disclosures
FUND_COL = "Position Holder"
ISIN_COL = "ISIN"
FCA_DATE_COL = "Position Date"
SHORT_POS_COL = "Net Short Position (%)"
SHARE_ISSUER_COL = "Name of Share Issuer"

EXP_DISCL_COLS = [FUND_COL, ISIN_COL, SHARE_ISSUER_COL, FCA_DATE_COL, SHORT_POS_COL]

# Columns from Yahoo Finance
TICKER_COL = "Ticker"
CLOSE_COL = "Close"
ADJ_CLOSE_COL = "Adj Close"
VOLUME_COL = "Volume"
MKT_CAP_COL = "marketCap"
SH_OUT_COL = "sharesOutstanding"

# for storing market data
DATE_COL = "date"
ITEM_COL = "item"
VALUE_COL = "value"

# calculated columns
RET_COL = "Return"
AMOUNT_COL = "Amount"
BM_RET_COL = "Benchmark Return"
REL_RET_COL = "Relative Return"
EXPO_COL = "Exposure"
PNL_COL = "PnL"
REL_PNL_COL = "Rel PnL"
DTC_COL = "Days to Cover"

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_HEADERS = {"Content-Type": "application/json"}

REQ_CODES = requests.status_codes.codes

logger = logging.getLogger(__name__)


class APIRateLimitException(Exception):
    """Reached the API limit (429 error)"""

    pass


class NotUpdatedError(Exception):
    """Some retrieved content hasn't been updated"""

    pass


def query_sec_metadata(
    ids: list,
    query_params: dict,
):
    """Query OpenFIGI for the given ids

    Args:
    - ids: a list of identifiers of the same type to query data for
    - query_params: type of identifier used for ids, eg {"idType": "ID_ISIN"}

    Returns: a dict of returned data with the originally queried ids as keys,
    and a list of ids returning an error

    Ref: https://www.openfigi.com/api#post-v3-mapping

    #FIXME: allow passing an API key
    #FIXME: return specific errors for invalid ids
    """
    payload = [{**query_params, "idValue": id_} for id_ in ids]
    req = requests.post(
        OPENFIGI_URL,
        json=payload,
        headers=OPENFIGI_HEADERS,
    )
    rcode = req.status_code
    if rcode == REQ_CODES.TOO_MANY:
        raise APIRateLimitException
    elif rcode != REQ_CODES.OK:
        raise ValueError(f"Unexpected exception from OpenFIGI API: {rcode}")

    resp_data = req.json()
    sec_metadata = {}
    error_ids = []

    for id, resp in zip(ids, resp_data):
        try:
            resp = resp["data"]
        except:
            logger.error(f"No data for id {id}, error: {resp.get('error')}")
            error_ids.append(id)
            continue

        sec_metadata[id] = resp

    return sec_metadata, error_ids


def query_all_sec_metadata(
    isins: list, query_params: dict, max_jobsize=10, max_req_rate=25
):
    """Query OpenFIGI using query_sec_metadata for security metadata while respecting
    their API limit (defaults are for without an API key).

    Will poll at 60/max_req_rate requests/second which should respect the limit
    (since the requests themselves take time). Retry at the same rate if we hit
    the limit.

    Args:
    - max_jobsize: max # of ids we can submit per API call.
    - max_req_rate: max number of jobs submitted per minute.
    """
    req_pause = 60.0 / max_req_rate

    @retry(
        retry=retry_if_exception_type(APIRateLimitException), wait=wait_fixed(req_pause)
    )
    def query_func(id_subset):
        return query_sec_metadata(id_subset, query_params)

    sec_metadata, error_ids = {}, []

    for i in range(0, len(isins), max_jobsize):
        isin_subset = isins[i : i + max_jobsize]
        sec_metadata_chunk, error_ids_ = query_func(isin_subset)
        sec_metadata = {**sec_metadata, **sec_metadata_chunk}
        error_ids.extend(error_ids_)

    return sec_metadata, error_ids


def parse_uk_discl_sheet_names(sheet_names: list) -> tuple[list, datetime.date]:
    """Check if the UK SI disclosure sheet names are as expected:
    - there are two sheets
    - they have the same reporting date
    - they both start with "current" or "historic" (up to upper/lower case)

    Returns: a list of the parsed sheet names if in the correct format, ie ["current", "historic"]
    in some order, and a datetime.date for the reporting date

    Raises:
        ValueError: if not in the expected format.

    #FIXME: could be less strict on expected sheet names
    """
    exp_parsed_names = {"current", "historic"}

    if len(sheet_names) != 2:
        raise ValueError(f"Was expecting two sheets, not {len(sheet_names)}")

    split_sheet_names = [x.split(" ") for x in sheet_names]

    rept_dates = set([x[2] for x in split_sheet_names])

    if len(rept_dates) != 1:
        raise ValueError(f"Was expecting a single reporting date, not: {rept_dates}")

    rept_date = parse(list(rept_dates)[0]).date()

    parsed_names = [x[0].lower() for x in split_sheet_names]

    if not set(parsed_names) == exp_parsed_names:
        raise ValueError(
            f"Was expecting {exp_parsed_names} for the parsed sheet names, not {parsed_names}"
        )

    return parsed_names, rept_date


def parse_uk_si_discl_data(data: pd.DataFrame) -> pd.DataFrame:
    """Check we have the expected columns for fund, isin, date, short position,
    up to leading/trailing spaces and case.

    Returns: a dataframe with columns normalised to EXP_DISCL_COLS if present

    Raises
        ValueError: if there are missing expected columns
    """
    cols = [x.strip().lower() for x in data.columns]
    col_map = {x.strip().lower(): x for x in EXP_DISCL_COLS}
    missing_cols = [x for x in col_map if x not in cols]

    if missing_cols:
        raise ValueError(f"Missing columns in returned disclosure data: {missing_cols}")
    return data.rename(columns=col_map)


def query_uk_si_disclosures(discl_url: str, exp_upd_datetime: datetime = None):
    """Send a GET request to the given url to retrieve the daily UK SI disclosures Excel file
    and parse the response into a dict of current and historical disclosures.

    Args:
    - discl_url: url for the file to retrieve
    - exp_upd_datetime: If given then check if the actual file last modified datetime is
    >= than this value

    Returns: a dict of dataframes with keys 'current', 'historic' for the corresponding disclosures,
    and a datetime.date object for the actual reporting date.

    Raises:
    - NotUpdatedError: if exp_upd_datetime given and the actual last modified datetime is
    < this value

    #FIXME: rename - not really specific to UK
    """
    resp = requests.get(discl_url)

    if exp_upd_datetime:
        act_upd_datetime = parse(resp.headers["Last-Modified"]).replace(tzinfo=None)
        # return act_upd_datetime, exp_upd_datetime

        if exp_upd_datetime > act_upd_datetime:
            raise NotUpdatedError(
                f"File at {discl_url} updated at {act_upd_datetime},"
                f" was expecting >= {exp_upd_datetime}"
            )

    ef = pd.ExcelFile(resp.content)
    data = pd.read_excel(ef, sheet_name=ef.sheet_names)

    parsed_sheet_names, rept_date = parse_uk_discl_sheet_names(list(data))
    parsed_data = [parse_uk_si_discl_data(df) for df in data.values()]

    return dict(zip(parsed_sheet_names, parsed_data)), rept_date


def query_quotes(ticker: str) -> Union[pd.Series, None]:
    """Query for a ticker's financial data, eg market cap etc using Pandas Datareader"""
    try:
        data = pdr.get_quote_yahoo(ticker)
    except IndexError:
        logger.warning(f"No quote data for ticker {ticker}")
        return None
    return data.squeeze()


def query_mkt_data(ticker, start_date, adjust=False) -> Union[pd.DataFrame, None]:
    """Query OHLC data from Yahoo Finance"""
    ticker = yf.Ticker(ticker)
    data = ticker.history(start=start_date, auto_adjust=adjust)
    if data.empty:
        logger.warning(f"No price data for ticker {ticker} from date {start_date}")
        return None
    data.index = data.index.date
    return data
