from __future__ import annotations
import re
from typing import Any, Optional


_MOJIBAKE_UTF8_AS_LATIN1_HINTS = ("Ð", "Ñ", "Ò", "Â")
_MOJIBAKE_UTF8_AS_CP1251_HINTS = ("Р", "С", "Ѓ", "Ћ")


def repair_mojibake(x: Any) -> str:
    if x is None:
        return ""

    s = str(x)

    # UTF-8 bytes decoded as latin1/cp1252: "Ð˜Ð¡Ð¥"
    if any(h in s for h in _MOJIBAKE_UTF8_AS_LATIN1_HINTS):
        try:
            repaired = s.encode("latin1").decode("utf-8")
            if repaired:
                s = repaired
        except Exception:
            pass

    # UTF-8 bytes decoded as cp1251: "РџРµСЂ..."
    if any(h in s for h in _MOJIBAKE_UTF8_AS_CP1251_HINTS):
        try:
            repaired = s.encode("cp1251").decode("utf-8")
            if repaired:
                s = repaired
        except Exception:
            pass

    return s


def clean_optional_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        if x != x:
            return None
    except Exception:
        pass

    s = repair_mojibake(x).strip()
    if not s:
        return None
    if s.lower() in {"nan", "none", "null", "nat", "<na>", "n/a"}:
        return None
    return s


def norm_text(x: Any) -> str:
    cleaned = clean_optional_text(x)
    if cleaned is None:
        return ""
    s = cleaned.lower()
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
