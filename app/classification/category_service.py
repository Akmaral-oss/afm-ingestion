"""
app/classification/category_service.py  — v4.0
Category columns теперь встроены в transactions_core напрямую.
Нет отдельной таблицы transaction_classification.

Поля в transactions_core:
  transaction_category  TEXT
  category_confidence   NUMERIC(5,4)
  category_source       TEXT   (rule | embedding | other)
  category_rule_id      TEXT
  needs_review          BOOLEAN

classify_rows() мутирует row-дикты ДО bulk_insert → категория
вставляется вместе с транзакцией за один запрос.

reclassify_from_db() делает UPDATE transactions_core WHERE tx_id.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .rule_engine import (
    classify_by_rules,
    clean_purpose_text,
    CATEGORY_NAMES,
    CAT_OTHER,
)

log = logging.getLogger(__name__)

_EMBED_THRESHOLD_ACCEPT = 0.35
_EMBED_THRESHOLD_REVIEW = 0.55
_BATCH = 512


@dataclass
class CategoryResult:
    tx_id:                str
    transaction_category: str
    category_confidence:  float
    category_source:      str
    category_rule_id:     str
    needs_review:         bool = False


class CategoryService:
    def __init__(self, engine: Engine, embedder=None):
        self.engine   = engine
        self.embedder = embedder
        self._cat_vecs: Optional[np.ndarray] = None

    # ── ingestion integration ─────────────────────────────────────────────────

    def classify_rows(self, core_rows: List[Dict[str, Any]]) -> int:
        """
        Классифицирует строки IN-PLACE (мутирует dict).
        Вызывать ДО bulk_insert_core_dedup — тогда категория
        вставляется за один INSERT вместе с остальными полями.
        Возвращает число обработанных строк.
        """
        if not core_rows:
            return 0

        for row in core_rows:
            res = self._classify_one(row)
            # мутируем dict — bulk_insert подхватит эти поля автоматически
            row["transaction_category"] = res.transaction_category
            row["category_confidence"]  = res.category_confidence
            row["category_source"]      = res.category_source
            row["category_rule_id"]     = res.category_rule_id
            row["needs_review"]         = res.needs_review

        by_source: Dict[str, int] = {}
        for r in core_rows:
            src = r["category_source"]
            by_source[src] = by_source.get(src, 0) + 1

        log.info(
            "CategoryService: classified %d rows %s",
            len(core_rows),
            " ".join(f"{s}={n}" for s, n in by_source.items()),
        )
        return len(core_rows)

    def reclassify_from_db(self, limit: int = 200_000) -> int:
        """
        Backfill: перечитывает строки у которых category_source='other'
        или transaction_category='ПРОЧЕЕ' и обновляет их прямо в transactions_core.
        """
        log.info("Backfill: loading rows to reclassify (limit=%d)...", limit)
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT tx_id::text, purpose_text, purpose_code,
                       operation_type_raw, direction
                FROM   afm.transactions_core
                WHERE  transaction_category = 'ПРОЧЕЕ'
                   OR  category_source = 'other'
                   OR  transaction_category IS NULL
                LIMIT  :lim
            """), {"lim": limit}).fetchall()

        if not rows:
            log.info("Nothing to backfill.")
            return 0

        log.info("Reclassifying %d rows...", len(rows))
        results: List[CategoryResult] = []
        for r in rows:
            results.append(self._classify_one({
                "tx_id":              r[0],
                "purpose_text":       r[1],
                "purpose_code":       r[2],
                "operation_type_raw": r[3],
                "direction":          r[4],
            }))

        # UPDATE transactions_core
        with self.engine.begin() as conn:
            for i in range(0, len(results), _BATCH):
                conn.execute(text("""
                    UPDATE afm.transactions_core SET
                      transaction_category = :cat,
                      category_confidence  = :conf,
                      category_source      = :src,
                      category_rule_id     = :rule_id,
                      needs_review         = :needs_review
                    WHERE tx_id = CAST(:tx_id AS uuid)
                """), [
                    {
                        "tx_id":        r.tx_id,
                        "cat":          r.transaction_category,
                        "conf":         r.category_confidence,
                        "src":          r.category_source,
                        "rule_id":      r.category_rule_id,
                        "needs_review": r.needs_review,
                    }
                    for r in results[i: i + _BATCH]
                ])

        log.info("Backfill complete: %d rows updated.", len(results))
        return len(results)

    def rebuild_clusters_from_categories(self) -> int:
        """Rebuild semantic_clusters using category centroids."""
        if not (self.embedder and self.embedder.enabled):
            log.warning("Embedder disabled — skipping category cluster rebuild.")
            return 0

        import json
        log.info("Rebuilding semantic_clusters from fixed categories...")
        cat_vecs  = self._get_cat_vecs()
        centroids = self._compute_centroids_from_db()

        clusters = []
        for i, (code, name) in enumerate(CATEGORY_NAMES.items()):
            centroid = centroids.get(code, cat_vecs[i])
            clusters.append({
                "cluster_id":         str(uuid.uuid4()),
                "cluster_label":      name,
                "cluster_keywords":   [w for w in name.lower().replace("/", " ").split() if len(w) > 2],
                "centroid_embedding": _vec_to_pg(centroid),
                "sample_texts":       self._fetch_samples(code),
                "tx_count":           self._count_category(code),
            })

        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM afm.semantic_clusters"))
            for c in clusters:
                conn.execute(text("""
                    INSERT INTO afm.semantic_clusters
                      (cluster_id, source_bank, cluster_label, cluster_keywords,
                       centroid_embedding, sample_texts, tx_count)
                    VALUES (CAST(:cid AS uuid), NULL, :label,
                            CAST(:kw AS jsonb), CAST(:emb AS vector),
                            CAST(:st AS jsonb), :cnt)
                """), {
                    "cid":   c["cluster_id"],
                    "label": c["cluster_label"],
                    "kw":    json.dumps(c["cluster_keywords"],   ensure_ascii=False),
                    "emb":   c["centroid_embedding"],
                    "st":    json.dumps(c["sample_texts"],       ensure_ascii=False),
                    "cnt":   c["tx_count"],
                })

        log.info("Rebuilt %d category clusters.", len(clusters))
        return len(clusters)

    # ── single-row classification ─────────────────────────────────────────────

    def _classify_one(self, row: Dict[str, Any]) -> CategoryResult:
        purpose_text = row.get("purpose_text") or ""
        purpose_code = row.get("purpose_code") or ""
        op_type_raw  = row.get("operation_type_raw") or ""
        direction    = row.get("direction") or ""
        tx_id        = str(row.get("tx_id") or uuid.uuid4())

        cleaned = clean_purpose_text(purpose_text)

        # Stage 1: rules
        rule_res = classify_by_rules(purpose_text, purpose_code, op_type_raw, direction)
        if rule_res.category_code != CAT_OTHER:
            return CategoryResult(
                tx_id=tx_id,
                transaction_category=rule_res.category_code,
                category_confidence=rule_res.confidence,
                category_source="rule",
                category_rule_id=rule_res.rule_id,
                needs_review=False,
            )

        # Stage 2: embedding cosine fallback
        if self.embedder and self.embedder.enabled and cleaned.strip():
            try:
                vec      = np.asarray(self.embedder.embed([cleaned])[0], dtype=np.float32)
                cat_vecs = self._get_cat_vecs()
                sims     = cat_vecs @ vec
                best_idx = int(np.argmax(sims))
                best_sim = float(sims[best_idx])
                if best_sim >= _EMBED_THRESHOLD_ACCEPT:
                    cat_code = list(CATEGORY_NAMES.keys())[best_idx]
                    return CategoryResult(
                        tx_id=tx_id,
                        transaction_category=cat_code,
                        category_confidence=round(best_sim, 4),
                        category_source="embedding",
                        category_rule_id="ЭМБЕДДИНГ",
                        needs_review=(best_sim < _EMBED_THRESHOLD_REVIEW),
                    )
            except Exception:
                log.debug("Embedding fallback failed for tx %s", tx_id, exc_info=True)

        # Stage 3: OTHER
        return CategoryResult(
            tx_id=tx_id,
            transaction_category=CAT_OTHER,
            category_confidence=1.0,
            category_source="other",
            category_rule_id="ПРОЧЕЕ_ПО_УМОЛЧАНИЮ",
            needs_review=True,
        )

    # ── embedding helpers ─────────────────────────────────────────────────────

    def _get_cat_vecs(self) -> np.ndarray:
        if self._cat_vecs is not None:
            return self._cat_vecs
        from sklearn.preprocessing import normalize
        vecs = self.embedder.embed(list(CATEGORY_NAMES.values()))
        self._cat_vecs = normalize(np.asarray(vecs, dtype=np.float32), norm="l2")
        return self._cat_vecs

    def _compute_centroids_from_db(self) -> Dict[str, np.ndarray]:
        centroids: Dict[str, np.ndarray] = {}
        try:
            with self.engine.connect() as conn:
                cats = conn.execute(text("""
                    SELECT transaction_category, COUNT(*) AS n
                    FROM   afm.transactions_core
                    WHERE  semantic_embedding IS NOT NULL
                      AND  transaction_category IS NOT NULL
                    GROUP  BY transaction_category
                """)).fetchall()
            for cat_code, n in cats:
                if n < 5:
                    continue
                with self.engine.connect() as conn:
                    rows = conn.execute(text("""
                        SELECT semantic_embedding
                        FROM   afm.transactions_core
                        WHERE  transaction_category = :cat
                          AND  semantic_embedding IS NOT NULL
                        LIMIT  5000
                    """), {"cat": cat_code}).fetchall()
                vecs = []
                for (raw,) in rows:
                    if isinstance(raw, str):
                        v = np.fromstring(raw.strip("[]"), sep=",", dtype=np.float32)
                    elif isinstance(raw, (bytes, memoryview)):
                        v = np.frombuffer(bytes(raw), dtype=np.float32)
                    else:
                        v = np.asarray(raw, dtype=np.float32)
                    vecs.append(v)
                if vecs:
                    c   = np.vstack(vecs).mean(axis=0)
                    nrm = np.linalg.norm(c)
                    centroids[cat_code] = c / nrm if nrm > 1e-9 else c
        except Exception:
            log.exception("_compute_centroids_from_db failed")
        return centroids

    def _fetch_samples(self, cat_code: str, n: int = 10) -> List[str]:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT purpose_text
                    FROM   afm.transactions_core
                    WHERE  transaction_category = :cat
                      AND  purpose_text IS NOT NULL
                    LIMIT  :n
                """), {"cat": cat_code, "n": n}).fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            return []

    def _count_category(self, cat_code: str) -> int:
        try:
            with self.engine.connect() as conn:
                return conn.execute(text(
                    "SELECT COUNT(*) FROM afm.transactions_core WHERE transaction_category = :c"
                ), {"c": cat_code}).scalar() or 0
        except Exception:
            return 0


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in np.asarray(vec, dtype=np.float32).reshape(-1)) + "]"
