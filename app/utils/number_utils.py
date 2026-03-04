from __future__ import annotations
import re
from typing import Any, Optional
import numpy as np

_NUM_TOKEN_RE = re.compile(r"-?\d+(?:[ \u00a0]?\d{3})*(?:[.,]\d+)?")


def parse_decimal(x: Any) -> Optional[float]:
    """
    Production-safe numeric parsing:
      - accepts messy strings like '0.00 вал 0.00 нац'
      - picks FIRST numeric token
      - handles comma decimal / thousand separators
    """
    if x is None:
        return None
    if isinstance(x, (int, float, np.number)) and not (
        isinstance(x, float) and np.isnan(x)
    ):
        return float(x)

    s = str(x).strip()
    if not s:
        return None

    s = s.replace("\u00a0", " ")
    m = _NUM_TOKEN_RE.search(s)
    if not m:
        return None

    tok = m.group(0).replace(" ", "").replace("\u00a0", "")
    if tok.count(",") == 1 and tok.count(".") == 0:
        tok = tok.replace(",", ".")
    if tok.count(",") >= 1 and tok.count(".") == 1:
        tok = tok.replace(",", "")
    try:
        return float(tok)
    except Exception:
        return None
