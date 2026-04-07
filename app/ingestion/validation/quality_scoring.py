from __future__ import annotations
from typing import Dict, Any


def score_row(core: Dict[str, Any]) -> float:
    # placeholder: extend with rule-based score later
    return float(core.get("confidence_score") or 1.0)
