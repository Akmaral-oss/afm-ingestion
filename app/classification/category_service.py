from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np

from .rule_engine import CATEGORY_NAMES, CAT_OTHER, classify_by_rules, clean_purpose_text

log = logging.getLogger(__name__)

_EMBED_THRESHOLD_ACCEPT = 0.35
_EMBED_THRESHOLD_REVIEW = 0.55


@dataclass
class CategoryResult:
    transaction_category: str
    category_confidence: float
    category_source: str
    category_rule_id: str
    needs_review: bool = False


class CategoryService:
    def __init__(self, embedder=None):
        self.embedder = embedder
        self._cat_vecs: Optional[np.ndarray] = None
        self._cat_labels = list(CATEGORY_NAMES.values())

    def classify_rows(self, core_rows: List[Dict[str, Any]]) -> int:
        if not core_rows:
            return 0

        for row in core_rows:
            result = self._classify_one(row)
            row["transaction_category"] = result.transaction_category
            row["category_confidence"] = result.category_confidence
            row["category_source"] = result.category_source
            row["category_rule_id"] = result.category_rule_id
            row["needs_review"] = result.needs_review

        return len(core_rows)

    def _classify_one(self, row: Dict[str, Any]) -> CategoryResult:
        purpose_text = row.get("purpose_text") or ""
        purpose_code = row.get("purpose_code") or ""
        op_type_raw = row.get("operation_type_raw") or ""
        direction = row.get("direction") or ""

        rule_res = classify_by_rules(purpose_text, purpose_code, op_type_raw, direction)
        if rule_res.category_code != CAT_OTHER:
            return CategoryResult(
                transaction_category=rule_res.category_name,
                category_confidence=rule_res.confidence,
                category_source="rule",
                category_rule_id=rule_res.rule_id,
                needs_review=False,
            )

        cleaned = clean_purpose_text(purpose_text)
        if self.embedder and self.embedder.enabled and cleaned:
            try:
                vec = np.asarray(self.embedder.embed([cleaned])[0], dtype=np.float32)
                cat_vecs = self._get_cat_vecs()
                sims = cat_vecs @ vec
                best_idx = int(np.argmax(sims))
                best_sim = float(sims[best_idx])
                if best_sim >= _EMBED_THRESHOLD_ACCEPT:
                    category_name = self._cat_labels[best_idx]
                    return CategoryResult(
                        transaction_category=category_name,
                        category_confidence=round(best_sim, 4),
                        category_source="embedding",
                        category_rule_id="EMBEDDING",
                        needs_review=(best_sim < _EMBED_THRESHOLD_REVIEW),
                    )
            except Exception:
                log.debug("Embedding fallback failed for category classification", exc_info=True)

        return CategoryResult(
            transaction_category=CAT_OTHER,
            category_confidence=1.0,
            category_source="other",
            category_rule_id="OTHER_DEFAULT",
            needs_review=True,
        )

    def _get_cat_vecs(self) -> np.ndarray:
        if self._cat_vecs is not None:
            return self._cat_vecs
        raw = np.asarray(self.embedder.embed(self._cat_labels), dtype=np.float32)
        if raw.ndim != 2 or raw.shape[0] == 0:
            raise RuntimeError("Category embedding matrix is empty")
        norms = np.linalg.norm(raw, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        self._cat_vecs = raw / norms
        return self._cat_vecs
