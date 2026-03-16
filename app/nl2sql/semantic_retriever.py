from __future__ import annotations

"""
Semantic retriever.

Fetches relevant context from afm.semantic_catalog using vector similarity.
Populates RetrievedContext with:
  - sample_values  : real purpose_text / operation_type_raw values from data
  - similar_examples: NL→SQL pairs from afm.query_history
"""

import logging
from typing import Any, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .query_models import RetrievedContext

log = logging.getLogger(__name__)


class SemanticRetriever:
    def __init__(
        self,
        engine: Engine,
        embedder,                        # EmbeddingBackend (from ingestion)
        catalog_top_k: int = 8,
        history_top_k: int = 3,
        similarity_threshold: float = 0.50,
    ):
        self.engine = engine
        self.embedder = embedder
        self.catalog_top_k = catalog_top_k
        self.history_top_k = history_top_k
        self.similarity_threshold = similarity_threshold

    # ── public ────────────────────────────────────────────────────────────────

    def retrieve(self, question: str, semantic_topic: Optional[str] = None) -> RetrievedContext:
        """
        Main entry point.  Returns RetrievedContext with sample values and
        similar NL→SQL examples.
        """
        query_text = semantic_topic or question
        ctx = RetrievedContext()

        if not self.embedder.enabled:
            log.debug("Embedder disabled — skipping retrieval")
            return ctx

        try:
            qvec = self.embedder.embed([query_text])[0]
            ctx.sample_values = self._fetch_samples(qvec)
            ctx.similar_examples = self._fetch_examples(qvec)
        except Exception:
            log.exception("SemanticRetriever failed — returning empty context")

        return ctx

    # ── private ───────────────────────────────────────────────────────────────

    def _fetch_samples(self, qvec) -> List[str]:
        """Retrieve sample values from afm.semantic_catalog (type='value')."""
        try:
            vec_bytes = _vec_to_pg(qvec)
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT text
                        FROM afm.semantic_catalog
                        WHERE type = 'value'
                          AND embedding IS NOT NULL
                        ORDER BY embedding <-> CAST(:v AS vector)
                        LIMIT :k
                        """
                    ),
                    {"v": vec_bytes, "k": self.catalog_top_k},
                ).fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            log.warning("_fetch_samples failed (pgvector not installed?)")
            return []

    def _fetch_examples(self, qvec) -> List[dict]:
        """Retrieve similar NL→SQL examples from afm.query_history."""
        try:
            vec_bytes = _vec_to_pg(qvec)
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT question, generated_sql
                        FROM afm.query_history
                        WHERE execution_success = TRUE
                          AND embedding IS NOT NULL
                        ORDER BY embedding <-> CAST(:v AS vector)
                        LIMIT :k
                        """
                    ),
                    {"v": vec_bytes, "k": self.history_top_k},
                ).fetchall()
            return [{"nl": r[0], "sql": r[1]} for r in rows if r[0] and r[1]]
        except Exception:
            log.warning("_fetch_examples failed (pgvector not installed?)")
            return []


# ── helpers ───────────────────────────────────────────────────────────────────

def _vec_to_pg(vec) -> str:
    """Convert numpy array to pgvector literal string '[0.1, 0.2, ...]'."""
    import numpy as np
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{v:.6f}" for v in arr) + "]"
