import logging, sys
from datetime import datetime, timedelta

LOG_FMT = r"%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(logger: logging.Logger):
    """Add a stream handler to stout with default format to the given logger
    and set the logger's level to logging.INFO
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=LOG_FMT)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def n_bdays_ago(n: int, start_date=None) -> datetime.date:
    """Returns a date object for n business days ago relative to the
    given date. If not given, then default to today.
    """
    if start_date is None:
        start_date = datetime.today().date()
    return start_date - timedelta(days=n)
