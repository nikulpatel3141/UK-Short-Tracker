from pathlib import Path

_CONFIG_PATH = Path(__file__)

DB_FILE = _CONFIG_PATH.parents[2].joinpath("data/data.sqlite")
CONN_STR = f"sqlite:///{DB_FILE}"

MAX_DATA_AGE = 30  # # of days of data to keep
TOP_N_SHORTS = 20  # # of top fund/overall shorts to keep
UK_MKT_TICKER = "VUKE"
