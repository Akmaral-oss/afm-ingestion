from __future__ import annotations
import re
from typing import Any, List
from app.utils.text_utils import norm_text


def is_service_row(row_values: List[Any]) -> bool:
    tokens = [norm_text(v) for v in row_values if v is not None]
    joined = " ".join([t for t in tokens if t]).strip()
    if joined == "":
        return True
    if any(
        k in joined
        for k in [
            "итого",
            "сальдо",
            "остаток",
            "итоговая сумма",
            "начальный баланс",
            "конечный баланс",
            "входящий остаток",
            "исходящий остаток",
        ]
    ):
        return True
    if re.fullmatch(r"(\d+\s*){3,}", joined):
        return True
    return False
