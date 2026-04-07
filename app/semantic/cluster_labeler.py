from __future__ import annotations

"""
ClusterLabeler

Optionally calls the LLM to generate a human-readable label for each cluster
based on sample transaction texts.

Two modes:
  - heuristic_only=True  → fast, uses top tokens (no LLM call)
  - heuristic_only=False → calls Ollama to produce a concise English/Russian label

The LLM call is intentionally lightweight — a small prompt, short answer.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_LABEL_PROMPT = """\
Below are sample payment descriptions from a group of similar bank transactions.
Produce ONE short label (3-6 words, Russian or English) that best describes \
what these transactions have in common.
Return the label only — no explanation.

Samples:
{samples}

Label:"""


class ClusterLabeler:
    def __init__(
        self,
        engine: Engine,
        llm=None,                    # OllamaBackend or None
        heuristic_only: bool = True,
    ):
        self.engine = engine
        self.llm = llm
        self.heuristic_only = heuristic_only or (llm is None)

    def label_all(self) -> int:
        """Re-label every cluster. Returns count updated."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT cluster_id::text, cluster_label, sample_texts "
                    "FROM afm.semantic_clusters"
                )
            ).fetchall()

        if not rows:
            log.info("No clusters to label.")
            return 0

        updated = 0
        for row in rows:
            cluster_id = row[0]
            samples: List[str] = json.loads(row[2]) if isinstance(row[2], str) else (row[2] or [])

            new_label = self._generate_label(samples[:10])
            if not new_label or new_label == row[1]:
                continue

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE afm.semantic_clusters "
                        "SET cluster_label = :label "
                        "WHERE cluster_id = CAST(:id AS uuid)"
                    ),
                    {"label": new_label, "id": cluster_id},
                )
            updated += 1

        log.info("ClusterLabeler: updated %d labels", updated)
        return updated

    # ── private ───────────────────────────────────────────────────────────────

    def _generate_label(self, samples: List[str]) -> Optional[str]:
        if self.heuristic_only or not samples:
            return self._heuristic_label(samples)

        prompt = _LABEL_PROMPT.format(
            samples="\n".join(f"- {s}" for s in samples[:10])
        )
        try:
            raw = self.llm.generate(prompt)
            label = raw.strip().split("\n")[0][:80]
            return label if label else self._heuristic_label(samples)
        except Exception:
            log.warning("LLM labeling failed — using heuristic")
            return self._heuristic_label(samples)

    @staticmethod
    def _heuristic_label(samples: List[str]) -> Optional[str]:
        from collections import Counter
        token_counts: Counter = Counter()
        for s in samples:
            for tok in s.lower().replace("|", " ").split():
                if len(tok) > 3:
                    token_counts[tok] += 1
        top = [w for w, _ in token_counts.most_common(3)]
        return " / ".join(top) if top else None
