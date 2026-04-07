"""
app/semantic/semantic_service.py
Single entry point for the NL2SQL layer to interact with the semantic layer.
After each ingestion batch, tracks new catalog rows and auto-triggers
ClusterBuilder.run() once cluster_rebuild_every_n threshold is crossed.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .query_expander import QueryExpander
from .semantic_catalog_builder import SemanticCatalogBuilder

log = logging.getLogger(__name__)

_REBUILD_LOCK = False   # simple in-process guard against concurrent rebuilds


class SemanticService:
    def __init__(
        self,
        engine: Engine,
        embedder,
        history_top_k: int = 3,
        auto_catalog: bool = True,
        cluster_rebuild_every_n: int = 500,
    ):
        self.engine = engine
        self.embedder = embedder
        self.auto_catalog = auto_catalog
        self.history_top_k = history_top_k
        self.cluster_rebuild_every_n = cluster_rebuild_every_n

        self.catalog_builder = SemanticCatalogBuilder(engine, embedder)
        self.expander = QueryExpander(engine, embedder)

        # Track how many catalog rows existed at last rebuild
        self._catalog_count_at_last_rebuild: int = self._get_catalog_count()

    # ── NL2SQL layer interface ─────────────────────────────────────────────

    def retrieve_context(self, question: str, semantic_topic: Optional[str] = None):
        from app.nl2sql.query_models import RetrievedContext
        ctx = RetrievedContext()
        query_text = semantic_topic or question

        if self.embedder.enabled:
            ctx.sample_values = self.expander.expand(query_text)

        ctx.similar_examples = self._fetch_history(query_text)
        return ctx

    # ── Ingestion pipeline interface ───────────────────────────────────────

    def after_ingest(self, core_rows: List[Dict[str, Any]]) -> None:
        """
        Update catalog then check if cluster rebuild is due.
        Rebuild runs in the same thread (fast for ≤200k rows).
        For very large datasets, move _maybe_rebuild_clusters() to
        a background thread or Celery task.
        """
        if not self.auto_catalog:
            return
        try:
            n_inserted = self.catalog_builder.build_for_rows(core_rows)
            if n_inserted > 0:
                self._maybe_rebuild_clusters()
        except Exception:
            log.exception("after_ingest semantic update failed — non-fatal")

    # ── private ───────────────────────────────────────────────────────────

    def _get_catalog_count(self) -> int:
        try:
            with self.engine.connect() as conn:
                return conn.execute(
                    text("SELECT COUNT(*) FROM afm.semantic_catalog WHERE embedding IS NOT NULL")
                ).scalar() or 0
        except Exception:
            return 0

    def _maybe_rebuild_clusters(self) -> None:
        """Rebuild clusters if enough new rows have accumulated."""
        global _REBUILD_LOCK
        if _REBUILD_LOCK:
            log.debug("Cluster rebuild already in progress — skipping")
            return

        current_count = self._get_catalog_count()
        delta = current_count - self._catalog_count_at_last_rebuild

        if delta < self.cluster_rebuild_every_n:
            log.debug(
                "Cluster rebuild not yet due: %d new rows (threshold=%d)",
                delta, self.cluster_rebuild_every_n,
            )
            return

        log.info(
            "Triggering cluster rebuild: %d new catalog rows (threshold=%d)",
            delta, self.cluster_rebuild_every_n,
        )
        _REBUILD_LOCK = True
        try:
            from .cluster_builder import ClusterBuilder
            builder = ClusterBuilder(
                engine=self.engine,
                k_min=8,
                k_max=None,   # Hartigan auto
            )
            n_clusters = builder.run()
            self._catalog_count_at_last_rebuild = current_count
            self.expander.refresh_cache()
            log.info("Auto cluster rebuild complete: %d clusters", n_clusters)
        except Exception:
            log.exception("Auto cluster rebuild failed — non-fatal")
        finally:
            _REBUILD_LOCK = False

    def _fetch_history(self, query_text: str) -> List[Dict[str, str]]:
        if not self.embedder.enabled:
            return []
        try:
            import numpy as np
            qvec = self.embedder.embed([query_text])[0]
            arr = np.asarray(qvec, dtype=np.float32).reshape(-1)
            vec_str = "[" + ",".join(f"{v:.6f}" for v in arr) + "]"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT question, generated_sql
                        FROM afm.query_history
                        WHERE execution_success = TRUE
                          AND embedding IS NOT NULL
                        ORDER BY embedding <-> CAST(:v AS vector)
                        LIMIT :k
                    """),
                    {"v": vec_str, "k": self.history_top_k},
                ).fetchall()
            return [{"nl": r[0], "sql": r[1]} for r in rows if r[0] and r[1]]
        except Exception:
            log.warning("History retrieval failed — returning empty")
            return []
