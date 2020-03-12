import datetime
import pandas as pd
import pytz


def date_today():
    return datetime.datetime.now(pytz.timezone("US/Eastern")).date()


def ds_today():
    return str(date_today())


def sql_quote(v):
    """Returns quoted value suitable for SQL insert.

    Is not robust enough to properly protect against SQL injection. Beware."""
    if v is None or pd.isnull(v):
        return "NULL"
    if isinstance(v, int):
        return v
    v = str(v)
    v = v.replace("'", "\\'")
    return f"'{v}'"
