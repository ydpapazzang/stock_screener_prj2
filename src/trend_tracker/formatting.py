from __future__ import annotations

from datetime import date

import pandas as pd


def to_krx_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def format_number(value: float | int | None, digits: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    if digits == 0:
        return f"{value:,.0f}"
    return f"{value:,.{digits}f}"


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:+.1f}%"
