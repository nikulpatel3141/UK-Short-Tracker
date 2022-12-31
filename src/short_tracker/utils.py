import logging, sys

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
