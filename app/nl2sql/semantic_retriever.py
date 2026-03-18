from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .query_models import RetrievedContext

log = logging.getLogger(__name__)


class SemanticRetriever:
    def __init__(
        self,
        engine: Engine,
        embedder,
        catalog_top_k: int = 8,
        history_top_k: int = 3,
    ):
        self.engine = engine
        self.embedder = embedder
        self.catalog_top_k = catalog_top_k
        self.history_top_k = history_top_k

    def retrieve(self, question: str, semantic_topic: Optional[str] = None) -> RetrievedContext:
        ctx = RetrievedContext()
        if not getattr(self.embedder, "enabled", False):
            return ctx

        query_text = semantic_topic or question
        try:
            qvec = self.embedder.embed([query_text])[0]
            ctx.sample_values = self._fetch_samples(qvec)
            ctx.similar_examples = self._fetch_examples(qvec)
        except Exception:
            log.exception("SemanticRetriever failed — returning empty context")
        return ctx

    def _fetch_samples(self, qvec: np.ndarray) -> List[str]:
        try:
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
                    {"v": _vec_to_pg(qvec), "k": self.catalog_top_k},
                ).fetchall()
            return [row[0] for row in rows if row[0]]
        except Exception:
            log.debug("Sample retrieval skipped", exc_info=True)
            return []

    def _fetch_examples(self, qvec: np.ndarray) -> List[dict]:
        try:
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
                    {"v": _vec_to_pg(qvec), "k": self.history_top_k},
                ).fetchall()
            return [{"nl": row[0], "sql": row[1]} for row in rows if row[0] and row[1]]
        except Exception:
            log.debug("Query history retrieval skipped", exc_info=True)
            return []


def _vec_to_pg(vec: np.ndarray) -> str:
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{value:.6f}" for value in arr) + "]"
