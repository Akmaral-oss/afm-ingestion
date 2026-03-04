from __future__ import annotations
import re
from typing import Any, Optional


def norm_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = s.replace("\u00a0", " ")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[“”\"']", "", s)
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s*:\s*", ":", s)
    s = s.strip(" \t\r\n-—")
    return s


def looks_like_iin_bin(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = re.sub(r"\D", "", str(x))
    return s if len(s) == 12 else None


def looks_like_iban(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = re.sub(r"\s+", "", str(x).upper())
    if s.startswith("KZ") and 10 <= len(s) <= 34:
        return s
    return None
