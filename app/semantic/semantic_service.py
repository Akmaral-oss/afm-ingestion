from __future__ import annotations

"""
SemanticService

Single entry point for the NL2SQL layer to interact with the semantic intelligence layer.

Responsibilities:
  1. retrieve_context(question) → RetrievedContext
       - cluster-based query expansion (real sample texts from data)
       - similar NL→SQL examples from query_history
  2. after_ingest(core_rows)
       - auto-update semantic_catalog for new ingested rows
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.engine import Engine

from .query_expander import QueryExpander
from .semantic_catalog_builder import SemanticCatalogBuilder

log = logging.getLogger(__name__)


class SemanticService:
    def __init__(
        self,
        engine: Engine,
        embedder,
        history_top_k: int = 3,
        auto_catalog: bool = True,
    ):
        self.engine = engine
        self.embedder = embedder
        self.auto_catalog = auto_catalog
        self.history_top_k = history_top_k

        self.catalog_builder = SemanticCatalogBuilder(engine, embedder)
        self.expander = QueryExpander(engine, embedder)

    # ── called from QueryService (NL2SQL layer) ───────────────────────────────

    def retrieve_context(self, question: str, semantic_topic: Optional[str] = None):
        """
        Returns RetrievedContext with:
          - sample_values  : cluster-expanded real transaction texts
          - similar_examples : past successful NL→SQL pairs from query_history
        """
        # imported here to avoid circular dependency with nl2sql package
        from app.nl2sql.query_models import RetrievedContext

        ctx = RetrievedContext()
        query_text = semantic_topic or question

        # 1. cluster-based expansion
        if self.embedder.enabled:
            ctx.sample_values = self.expander.expand(query_text)

        # 2. query history retrieval
        ctx.similar_examples = self._fetch_history(query_text)

        return ctx

    # ── called from IngestionPipeline ─────────────────────────────────────────

    def after_ingest(self, core_rows: List[Dict[str, Any]]) -> None:
        """Called at the end of pipeline.ingest_file() to update semantic_catalog."""
        if not self.auto_catalog:
            return
        try:
            n = self.catalog_builder.build_for_rows(core_rows)
            if n:
                self.expander.refresh_cache()
        except Exception:
            log.exception("after_ingest semantic update failed — non-fatal")

    # ── private ───────────────────────────────────────────────────────────────

    def _fetch_history(self, query_text: str) -> List[Dict[str, str]]:
        if not self.embedder.enabled:
            return []
        try:
            import numpy as np
            qvec = self.embedder.embed([query_text])[0]
            arr = np.asarray(qvec, dtype=np.float32).reshape(-1)
            vec_str = "[" + ",".join(f"{v:.6f}" for v in arr) + "]"
            from sqlalchemy import text
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
                    {"v": vec_str, "k": self.history_top_k},
                ).fetchall()
            return [{"nl": r[0], "sql": r[1]} for r in rows if r[0] and r[1]]
        except Exception:
            log.warning("History retrieval failed — returning empty")
            return []
