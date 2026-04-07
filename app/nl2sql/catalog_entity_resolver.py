from __future__ import annotations

"""
CatalogEntityResolver

Replaces hardcoded keyword lists in entity_extractor.py with a dynamic,
catalog-driven approach.

On every query:
  1. Embed the user question with BGE-M3.
  2. Find the top-k nearest K-means cluster centroids in afm.semantic_clusters
     (using cosine similarity on L2-normalised vectors).
  3. Extract real sample_texts from those clusters.
  4. Return cluster labels + LIKE patterns built from real data vocabulary.

Benefits over keyword lists:
  - Works for any language (Russian, Kazakh, English, mixed).
  - Automatically adapts as new data arrives and clusters are rebuilt.
  - No manual maintenance required — clusters reflect actual DB vocabulary.
  - Handles synonyms automatically (e.g. "зарплата" and "оклад" end up in
    the same cluster even without an explicit synonym rule).

Usage:
    resolver = CatalogEntityResolver(engine, embedder, top_k_clusters=3)
    resolution = resolver.resolve("выплата зарплаты в Kaspi за январь 2024")
    # resolution.cluster_labels -> ["зарплата / выплата / оклад"]
    # resolution.like_patterns  -> ["%зарплат%", "%выплат%", "%оклад%"]
    # resolution.llm_context    -> text injected into the LLM prompt
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class CatalogResolution:
    """Result returned by CatalogEntityResolver.resolve()."""
    cluster_labels: List[str] = field(default_factory=list)
    cluster_ids: List[str] = field(default_factory=list)
    sample_texts: List[str] = field(default_factory=list)
    like_patterns: List[str] = field(default_factory=list)
    llm_context: str = ""


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

class CatalogEntityResolver:
    """
    Dynamic topic resolver backed by the K-means semantic_clusters table.

    Parameters
    ----------
    engine : SQLAlchemy Engine
    embedder : EmbeddingBackend (from app.ingestion.mapping.embedding_mapper)
    top_k_clusters : int
        How many nearest clusters to retrieve (default 3).
    min_similarity : float
        Cosine similarity threshold — clusters below this score are ignored
        (default 0.30).  Lowering this makes the resolver more permissive.
    max_patterns : int
        Maximum number of LIKE patterns to generate (default 8).
    """

    def __init__(
        self,
        engine: Engine,
        embedder,
        top_k_clusters: int = 3,
        min_similarity: float = 0.30,
        max_patterns: int = 8,
    ):
        self.engine = engine
        self.embedder = embedder
        self.top_k_clusters = top_k_clusters
        self.min_similarity = min_similarity
        self.max_patterns = max_patterns

    # -- public ----------------------------------------------------------------

    def resolve(self, question: str) -> CatalogResolution:
        """
        Embed the question and look up the nearest K-means clusters.
        Returns a CatalogResolution with labels, sample texts, and LIKE patterns.
        """
        resolution = CatalogResolution()

        if not self.embedder.enabled:
            log.debug("CatalogEntityResolver: embedder disabled — returning empty resolution")
            return resolution

        try:
            # 1. Embed and L2-normalise the question
            qvec = self._embed_normalised(question)

            # 2. Find nearest clusters by cosine similarity
            clusters = self._find_nearest_clusters(qvec)
            if not clusters:
                log.debug("No clusters found above threshold for: %s", question[:60])
                return resolution

            # 3. Collect outputs
            all_samples: List[str] = []
            for c in clusters:
                resolution.cluster_labels.append(c["cluster_label"])
                resolution.cluster_ids.append(c["cluster_id"])
                all_samples.extend(c["sample_texts"])

            resolution.sample_texts = list(dict.fromkeys(all_samples))  # deduplicated

            # 4. Build LIKE patterns from real vocabulary
            resolution.like_patterns = self._build_like_patterns(
                all_samples, clusters
            )

            # 5. Build LLM context block
            resolution.llm_context = self._build_llm_context(question, clusters)

            log.debug(
                "CatalogEntityResolver: %d clusters, %d patterns for: %s",
                len(clusters), len(resolution.like_patterns), question[:60],
            )

        except Exception:
            log.exception("CatalogEntityResolver.resolve failed — returning empty resolution")

        return resolution

    # -- private: embedding ----------------------------------------------------

    def _embed_normalised(self, text_: str) -> np.ndarray:
        """Embed and L2-normalise so cosine similarity = dot product."""
        from sklearn.preprocessing import normalize  # type: ignore
        vec = self.embedder.embed([text_])[0]
        arr = np.asarray(vec, dtype=np.float32).reshape(1, -1)
        return normalize(arr, norm="l2")[0]

    # -- private: cluster lookup -----------------------------------------------

    def _find_nearest_clusters(self, qvec: np.ndarray) -> List[Dict[str, Any]]:
        """
        Load all cluster centroids from DB and return the top-k nearest
        ones with cosine similarity above min_similarity.

        We load centroids in-memory (there are only 8–60 clusters) rather
        than relying on pgvector ANN, so the result is deterministic and
        does not require a pgvector index on semantic_clusters.
        """
        import json

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT cluster_id::text, cluster_label,
                           centroid_embedding, sample_texts
                    FROM afm.semantic_clusters
                    WHERE centroid_embedding IS NOT NULL
                    """
                )
            ).fetchall()

        if not rows:
            return []

        scored = []
        for r in rows:
            raw_emb = r[2]
            if raw_emb is None:
                continue
            # Parse centroid vector
            if isinstance(raw_emb, str):
                cvec = np.fromstring(raw_emb.strip("[]"), sep=",", dtype=np.float32)
            elif isinstance(raw_emb, (bytes, memoryview)):
                cvec = np.frombuffer(bytes(raw_emb), dtype=np.float32)
            else:
                cvec = np.asarray(raw_emb, dtype=np.float32)

            # Normalise centroid (should already be normalised, but be safe)
            norm = np.linalg.norm(cvec)
            if norm > 1e-9:
                cvec = cvec / norm

            # Cosine similarity = dot product of unit vectors
            sim = float(np.dot(qvec, cvec))
            if sim < self.min_similarity:
                continue

            samples_raw = r[3]
            samples = json.loads(samples_raw) if isinstance(samples_raw, str) else (samples_raw or [])

            scored.append({
                "cluster_id":    r[0],
                "cluster_label": r[1],
                "similarity":    sim,
                "sample_texts":  samples,
            })

        # Sort by similarity descending, take top-k
        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return scored[: self.top_k_clusters]

    # -- private: pattern building ---------------------------------------------

    def _build_like_patterns(
        self,
        sample_texts: List[str],
        clusters: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Extract the most frequent meaningful tokens from sample texts and
        cluster labels, then convert them to SQL LIKE patterns.

        Priority:
          1. Keywords from cluster_label (most representative)
          2. High-frequency tokens from sample_texts
        """
        from collections import Counter

        seen: set[str] = set()
        patterns: List[str] = []

        # Priority 1: cluster label tokens
        for c in clusters:
            for tok in c["cluster_label"].lower().replace("/", " ").split():
                tok = tok.strip()
                if len(tok) >= 4 and tok not in seen:
                    seen.add(tok)
                    patterns.append(f"%{tok}%")
                    if len(patterns) >= self.max_patterns:
                        return patterns

        # Priority 2: high-frequency tokens from sample texts
        counter: Counter = Counter()
        for t in sample_texts:
            for tok in t.lower().replace("|", " ").split():
                tok = tok.strip()
                if len(tok) >= 4:
                    counter[tok] += 1

        for tok, _count in counter.most_common(self.max_patterns * 2):
            if tok not in seen:
                seen.add(tok)
                patterns.append(f"%{tok}%")
                if len(patterns) >= self.max_patterns:
                    break

        return patterns

    # -- private: LLM context --------------------------------------------------

    def _build_llm_context(
        self, question: str, clusters: List[Dict[str, Any]]
    ) -> str:
        """
        Build a text block injected into the LLM prompt that explains
        which topic clusters were found and what real values they contain.
        """
        lines = ["SEMANTIC CATALOG MATCHES (from K-means clusters):"]
        for i, c in enumerate(clusters, 1):
            sim_pct = int(c["similarity"] * 100)
            lines.append(
                f"  Cluster {i}: \"{c['cluster_label']}\" (similarity={sim_pct}%)"
            )
            samples = c["sample_texts"][:5]
            if samples:
                lines.append("    Real values from data:")
                for s in samples:
                    lines.append(f"      - {s}")
        lines.append("")
        lines.append(
            "Use the real values above to build LIKE anchors in your WHERE clause."
        )
        return "\n".join(lines)
