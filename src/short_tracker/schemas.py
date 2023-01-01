from sqlalchemy import Table, Column, Float, String, MetaData, Date, create_engine

from short_tracker.config import CONN_STR
from short_tracker.data import (
    FUND_COL,
    ITEM_COL,
    SHARE_ISSUER_COL,
    SHORT_POS_COL,
    TICKER_COL,
    ISIN_COL,
    DATE_COL,
    VALUE_COL,
)

# table names
SHORT_DISCL_TABLE = "uk_short_discl"
MKT_DATA_TABLE = "market_data"  # OHLC, shares outstanding
SEC_METADATA_TABLE = "sec_metadata"  # ticker, isin

md = MetaData()

mkt_data_tbl = Table(
    MKT_DATA_TABLE,
    md,
    Column(TICKER_COL, String(20), primary_key=True),
    Column(ITEM_COL, String(50), primary_key=True),
    Column(DATE_COL, Date(), primary_key=True),
    Column(VALUE_COL, Float()),
)

sec_metadata_tbl = Table(
    SEC_METADATA_TABLE,
    md,
    Column(TICKER_COL, String(20), primary_key=True),
    Column(ISIN_COL, String(20)),
)

discl_data_tbl = Table(
    SHORT_DISCL_TABLE,
    md,
    Column(ISIN_COL, String(20), primary_key=True),
    Column(FUND_COL, String(100), primary_key=True),
    Column(SHARE_ISSUER_COL, String(200)),
    Column(DATE_COL, Date(), primary_key=True),
    Column(SHORT_POS_COL, Float()),
)


def seed_db():
    """Create the database tables specified above"""
    engine = create_engine(CONN_STR)
    md.create_all(engine)
