"""Utility sub package"""
import argparse
from datetime import timedelta, date

from association import spark
from association.utils.log import LoggerMixin, log_time
from association.utils.io import (
    DeltaIOAdapter, read_csv, hard_cache, overwrite_csv, overwrite_mongo_db
)
from association.utils.transformation import date_add_str

LOG = LoggerMixin().log


def get_run_date_from_cli() -> argparse.Namespace:
    """
    Parse command line arguments for modules (options)
    """
    # create parser
    parser = argparse.ArgumentParser(description=__doc__)

    # add arguments to the parser
    parser.add_argument(
        "-rd", "--run-date", type=str,
        default=str(date.today() - timedelta(days=1)),
        help="run date in YYYY-MM-DD style, default: today - 1"
    )

    # parse arguments
    arguments, unknown = parser.parse_known_args()
    if unknown:
        LOG.warning(f"unknown arguments: {unknown}")
    LOG.info(f"arguments: {arguments}")
    # return only known arguments, unknown arguments will be discarded
    return arguments


delta_io = DeltaIOAdapter(spark_session=spark)

__all__ = [
    "LoggerMixin",
    "log_time",
    "DeltaIOAdapter",
    "delta_io",
    "date_add_str",
    "read_csv",
    "overwrite_csv",
    "overwrite_mongo_db",
    "hard_cache",
    "get_run_date_from_cli"
]
