from __future__ import annotations
from typing import Dict, Any, Optional


def derive_direction(core: Dict[str, Any]) -> Optional[str]:
    op_type = core.get("operation_type_raw")
    if op_type:
        t = str(op_type).lower()
        if "дебет" in t:
            return "debit"
        if "кредит" in t:
            return "credit"

    dc = core.get("amount_debit")
    cc = core.get("amount_credit")
    ac = core.get("amount_currency")

    if dc is not None and (cc is None or cc == 0):
        if dc != 0:
            return "debit"

    if cc is not None and (dc is None or dc == 0):
        if cc != 0:
            return "credit"

    if ac is not None:
        return None

    return None