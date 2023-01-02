from pathlib import Path

_CONFIG_PATH = Path(__file__)
_PROJECT_ROOT = _CONFIG_PATH.parents[2]


DB_FILE = _PROJECT_ROOT.joinpath("data/data.sqlite")
CONN_STR = f"sqlite:///{DB_FILE}"

OUT_FILE = _PROJECT_ROOT.joinpath("output/output.json")

MAX_DATA_AGE = 30  # # of days of data to keep
TOP_N_SHORTS = 20  # # of top fund/overall shorts to keep
METRICS_LOOKBACK = 5  # # of days over which to report metrics (eg 5 ~ weekly)
ADV_CALC_LOOKBACK = 22  # # of days for the average daily volume calculation

UK_MKT_TICKER = "VUKE"  # FTSE100 ETF. # FIXME: maybe the actual index is better?
