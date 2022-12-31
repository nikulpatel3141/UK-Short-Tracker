from pathlib import Path

_CONFIG_PATH = Path(__file__)

DB_FILE = _CONFIG_PATH.parents[2].joinpath("data/data.sqlite")
CONN_STR = f"sqlite:///{DB_FILE}"

# table names
SHORT_DISCL_TABLE = "uk_short_discl"
MKT_DATA_TABLE = "market_data"  # OHLC, shares outstanding
SEC_METADATA_TABLE = "sec_metadata"  # ticker, isin


MAX_DATA_AGE = 30  # # of days of data to keep
TOP_N_SHORTS = 20  # # of top fund/overall shorts to keep
UK_MKT_TICKER = "VUKE"
