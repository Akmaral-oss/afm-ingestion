from __future__ import annotations

from typing import Any, Dict, Optional

from app.utils.text_utils import norm_text


def derive_direction(core: Dict[str, Any]) -> Optional[str]:
    op_type = norm_text(core.get("operation_type_raw"))
    purpose = norm_text(core.get("purpose_text"))
    combined = " | ".join(part for part in (op_type, purpose) if part)

    if combined:
        if "дебет" in combined:
            return "debit"
        if "кредит" in combined:
            return "credit"

        if (
            "исх" in combined
            or "списан" in combined
            or "списание" in combined
            or "расход" in combined
            or "переводом card to card" in combined
            or "перевод card to card" in combined
        ):
            return "debit"

        if (
            "вх" in combined
            or "зачисл" in combined
            or "поступл" in combined
            or "приход" in combined
            or "зачисление" in combined
        ):
            return "credit"

    dc = core.get("amount_debit")
    cc = core.get("amount_credit")

    if dc is not None and (cc is None or cc == 0) and dc != 0:
        return "debit"

    if cc is not None and (dc is None or dc == 0) and cc != 0:
        return "credit"

    return None
