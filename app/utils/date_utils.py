from __future__ import annotations

import datetime as dt
import re
from typing import Any, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dateparser


def parse_datetime(x: Any) -> Optional[dt.datetime]:
    if x is None:
        return None
    if isinstance(x, (dt.datetime, pd.Timestamp)):
        return pd.to_datetime(x).to_pydatetime()
    s = str(x).strip()
    if not s:
        return None
    try:
        return dateparser.parse(s, dayfirst=True)
    except Exception:
        return None


def parse_date(x: Any) -> Optional[dt.date]:
    d = parse_datetime(x)
    return d.date() if d else None


def extract_all_dates(text_val: Any) -> List[dt.date]:
    if text_val is None:
        return []
    s = str(text_val).replace("–", "-").replace("—", "-")
    patterns = [
        r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b",
        r"\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b",
    ]
    found: List[dt.date] = []
    for pat in patterns:
        for m in re.findall(pat, s):
            d = parse_date(m)
            if d:
                found.append(d)
    uniq: List[dt.date] = []
    seen = set()
    for d in found:
        if d not in seen:
            uniq.append(d)
            seen.add(d)
    return uniq


def parse_period(text_val: Any) -> Tuple[Optional[dt.date], Optional[dt.date]]:
    dates = extract_all_dates(text_val)
    if not dates:
        return None, None
    if len(dates) == 1:
        return dates[0], None
    return dates[0], dates[1]
