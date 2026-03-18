from __future__ import annotations

"""
ClusterBuilder

Reads semantic embeddings from afm.semantic_catalog, runs HDBSCAN clustering,
and writes results to afm.semantic_clusters.

Why HDBSCAN:
  - No need to specify number of clusters upfront
  - Handles noise / rare operations gracefully
  - Works well with varying cluster densities (typical in banking data)

Fallback: if hdbscan is not installed, falls back to MiniBatch KMeans.
"""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_HDBSCAN_AVAILABLE = False
try:
    import hdbscan as _hdbscan_lib   # type: ignore
    _HDBSCAN_AVAILABLE = True
except ImportError:
    pass


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in np.asarray(vec, dtype=np.float32).reshape(-1)) + "]"


class ClusterBuilder:
    def __init__(
        self,
        engine: Engine,
        min_cluster_size: int = 5,
        min_samples: int = 3,
        n_kmeans_clusters: int = 40,    # fallback if HDBSCAN not available
        max_rows: int = 200_000,
    ):
        self.engine = engine
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples
        self.n_kmeans_clusters = n_kmeans_clusters
        self.max_rows = max_rows

    # ── public ────────────────────────────────────────────────────────────────

    def run(self, source_bank: Optional[str] = None) -> int:
        """
        Load embeddings → cluster → save to afm.semantic_clusters.
        Returns number of clusters created.
        """
        log.info("Loading embeddings from semantic_catalog (bank=%s)…", source_bank or "all")
        ids, texts, matrix = self._load_embeddings(source_bank)

        if len(ids) < self.min_cluster_size * 2:
            log.warning("Not enough records (%d) to cluster — skipping", len(ids))
            return 0

        log.info("Clustering %d vectors…", len(ids))
        labels = self._cluster(matrix)

        clusters = self._aggregate(ids, texts, matrix, labels)
        log.info("Found %d clusters (noise excluded)", len(clusters))

        self._save_clusters(clusters, source_bank)
        return len(clusters)

    # ── private ───────────────────────────────────────────────────────────────

    def _load_embeddings(
        self, source_bank: Optional[str]
    ) -> Tuple[List[str], List[str], np.ndarray]:
        sql = (
            "SELECT id::text, semantic_text, embedding "
            "FROM afm.semantic_catalog "
            "WHERE embedding IS NOT NULL"
        )
        params: Dict[str, Any] = {}
        if source_bank:
            sql += " AND source_bank = :b"
            params["b"] = source_bank
        sql += " LIMIT :lim"
        params["lim"] = self.max_rows

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()

        if not rows:
            return [], [], np.empty((0, 1))

        ids   = [r[0] for r in rows]
        texts = [r[1] for r in rows]
        # embedding stored as pgvector — SQLAlchemy returns it as a string "[0.1,0.2,...]"
        # or as bytes depending on driver; handle both
        vecs = []
        for r in rows:
            raw = r[2]
            if isinstance(raw, str):
                vecs.append(np.fromstring(raw.strip("[]"), sep=",", dtype=np.float32))
            elif isinstance(raw, (bytes, memoryview)):
                vecs.append(np.frombuffer(bytes(raw), dtype=np.float32))
            else:
                vecs.append(np.asarray(raw, dtype=np.float32))

        matrix = np.vstack(vecs)
        return ids, texts, matrix

    def _cluster(self, matrix: np.ndarray) -> np.ndarray:
        if _HDBSCAN_AVAILABLE:
            clusterer = _hdbscan_lib.HDBSCAN(
                min_cluster_size=self.min_cluster_size,
                min_samples=self.min_samples,
                metric="euclidean",
                cluster_selection_method="eom",
            )
            return clusterer.fit_predict(matrix)
        else:
            log.warning("hdbscan not installed — falling back to MiniBatchKMeans")
            from sklearn.cluster import MiniBatchKMeans  # type: ignore
            km = MiniBatchKMeans(
                n_clusters=min(self.n_kmeans_clusters, len(matrix) // 2),
                random_state=42,
                n_init=3,
            )
            return km.fit_predict(matrix)

    def _aggregate(
        self,
        ids: List[str],
        texts: List[str],
        matrix: np.ndarray,
        labels: np.ndarray,
    ) -> List[Dict[str, Any]]:
        from collections import defaultdict

        groups: Dict[int, List[int]] = defaultdict(list)
        for idx, lbl in enumerate(labels):
            if int(lbl) == -1:   # HDBSCAN noise
                continue
            groups[int(lbl)].append(idx)

        clusters = []
        for lbl, indices in groups.items():
            cluster_texts = [texts[i] for i in indices]
            cluster_vecs  = matrix[indices]
            centroid = cluster_vecs.mean(axis=0)
            centroid /= np.linalg.norm(centroid) + 1e-9

            # heuristic label: most common tokens
            from collections import Counter
            token_counts: Counter = Counter()
            for t in cluster_texts:
                for tok in t.lower().replace("|", " ").split():
                    if len(tok) > 3:
                        token_counts[tok] += 1
            top_tokens = [w for w, _ in token_counts.most_common(5)]

            clusters.append({
                "cluster_id":         str(uuid.uuid4()),
                "cluster_label":      " / ".join(top_tokens[:3]) or f"cluster_{lbl}",
                "cluster_keywords":   top_tokens,
                "centroid_embedding": _vec_to_pg(centroid),
                "sample_texts":       cluster_texts[:10],
                "tx_count":           len(indices),
            })

        return clusters

    def _save_clusters(
        self, clusters: List[Dict[str, Any]], source_bank: Optional[str]
    ) -> None:
        if not clusters:
            return

        # clear old clusters for this bank before re-inserting
        with self.engine.begin() as conn:
            if source_bank:
                conn.execute(
                    text(
                        "DELETE FROM afm.semantic_clusters WHERE source_bank = :b"
                    ),
                    {"b": source_bank},
                )
            else:
                conn.execute(text("DELETE FROM afm.semantic_clusters"))

            for c in clusters:
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.semantic_clusters
                          (cluster_id, source_bank, cluster_label, cluster_keywords,
                           centroid_embedding, sample_texts, tx_count)
                        VALUES (
                          CAST(:cluster_id AS uuid),
                          :source_bank,
                          :cluster_label,
                          CAST(:cluster_keywords AS jsonb),
                          CAST(:centroid_embedding AS vector),
                          CAST(:sample_texts AS jsonb),
                          :tx_count
                        );
                        """
                    ),
                    {
                        "cluster_id":         c["cluster_id"],
                        "source_bank":        source_bank,
                        "cluster_label":      c["cluster_label"],
                        "cluster_keywords":   json.dumps(c["cluster_keywords"], ensure_ascii=False),
                        "centroid_embedding": c["centroid_embedding"],
                        "sample_texts":       json.dumps(c["sample_texts"], ensure_ascii=False),
                        "tx_count":           c["tx_count"],
                    },
                )
        log.info("Saved %d clusters (bank=%s)", len(clusters), source_bank or "all")
