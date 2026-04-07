from __future__ import annotations

"""
QueryExpander

Given a user's semantic query (e.g. "долг кредит"),
finds the nearest cluster centroids and returns expanded terms.

These expanded terms are injected into the LLM prompt so the model
knows what real database values correspond to the user's intent.

Example:
  Input:  "долг"
  Output: ["погашение займа", "возврат долга", "оплата кредита",
            "кредитный платёж", "repayment"]
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    return float(np.dot(a, b) / denom) if denom > 0 else 0.0


class QueryExpander:
    def __init__(
        self,
        engine: Engine,
        embedder,
        top_clusters: int = 3,
        top_samples_per_cluster: int = 5,
        min_similarity: float = 0.35,
    ):
        self.engine = engine
        self.embedder = embedder
        self.top_clusters = top_clusters
        self.top_samples_per_cluster = top_samples_per_cluster
        self.min_similarity = min_similarity

        # In-memory cluster cache — refreshed on demand
        self._cache: List[Dict[str, Any]] = []

    # ── public ────────────────────────────────────────────────────────────────

    def expand(self, query: str) -> List[str]:
        """
        Returns a deduplicated list of real transaction texts that are
        semantically similar to the query.
        """
        if not self.embedder.enabled:
            return []

        try:
            qvec = self.embedder.embed([query])[0]
            clusters = self._get_clusters()
            if not clusters:
                return self._fallback_catalog_search(qvec)

            ranked = self._rank_clusters(qvec, clusters)
            results: List[str] = []
            seen = set()
            for cluster in ranked[: self.top_clusters]:
                for sample in (cluster["sample_texts"] or []):
                    s = str(sample).strip()
                    if s and s not in seen:
                        results.append(s)
                        seen.add(s)
                    if len(results) >= self.top_clusters * self.top_samples_per_cluster:
                        break

            return results[: self.top_clusters * self.top_samples_per_cluster]

        except Exception:
            log.exception("QueryExpander.expand failed — returning empty")
            return []

    def refresh_cache(self) -> None:
        """Force reload of cluster centroids from DB."""
        self._cache = []
        self._get_clusters()

    # ── private ───────────────────────────────────────────────────────────────

    def _get_clusters(self) -> List[Dict[str, Any]]:
        if self._cache:
            return self._cache

        import json
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT cluster_id::text, cluster_label, "
                    "       centroid_embedding, sample_texts "
                    "FROM afm.semantic_clusters "
                    "WHERE centroid_embedding IS NOT NULL"
                )
            ).fetchall()

        clusters = []
        for r in rows:
            raw_emb = r[2]
            if raw_emb is None:
                continue
            if isinstance(raw_emb, str):
                vec = np.fromstring(raw_emb.strip("[]"), sep=",", dtype=np.float32)
            elif isinstance(raw_emb, (bytes, memoryview)):
                vec = np.frombuffer(bytes(raw_emb), dtype=np.float32)
            else:
                vec = np.asarray(raw_emb, dtype=np.float32)

            samples = json.loads(r[3]) if isinstance(r[3], str) else (r[3] or [])
            clusters.append({
                "cluster_id":    r[0],
                "cluster_label": r[1],
                "centroid":      vec,
                "sample_texts":  samples,
            })

        self._cache = clusters
        log.debug("QueryExpander loaded %d clusters", len(clusters))
        return clusters

    def _rank_clusters(
        self, qvec: np.ndarray, clusters: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        scored = []
        for c in clusters:
            sim = _cosine(qvec, c["centroid"])
            if sim >= self.min_similarity:
                scored.append((sim, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored]

    def _fallback_catalog_search(self, qvec: np.ndarray) -> List[str]:
        """Direct ANN search on semantic_catalog when no clusters exist."""
        try:
            vec_str = "[" + ",".join(f"{v:.6f}" for v in qvec.reshape(-1)) + "]"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT semantic_text
                        FROM afm.semantic_catalog
                        WHERE embedding IS NOT NULL
                        ORDER BY embedding <-> CAST(:v AS vector)
                        LIMIT :k
                        """
                    ),
                    {"v": vec_str, "k": 10},
                ).fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            return []
