"""TRANSFORMATIONS related utility"""
from datetime import datetime, timedelta


def date_add_str(date_: str, **kwargs) -> str:
    """
    Simple wrapper to add date to string format. Date format should be
    ``"%Y-%m-%d"``. All kwargs parameters will be passed directly to
    ``datetime.timedelta()`` function.
    """
    return str(
        (datetime.strptime(date_, "%Y-%m-%d") +
         timedelta(**kwargs)).date()
    )
