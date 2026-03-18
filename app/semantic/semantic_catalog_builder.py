from __future__ import annotations

"""
SemanticCatalogBuilder

After each ingestion batch:
  1. Reads new transactions_core rows with semantic_text but no catalog entry.
  2. Embeds semantic_text with BGE-M3.
  3. Inserts records into afm.semantic_catalog.

Fix: INSERT now includes type='tx' and text=semantic_text so the unified
semantic_catalog schema works for both SemanticCatalogBuilder and
seed_catalog / SemanticRetriever (which expects type='value', text column).
"""

import logging
import uuid
from typing import Any, Dict, List

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_BATCH = 256


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in np.asarray(vec, dtype=np.float32).reshape(-1)) + "]"


class SemanticCatalogBuilder:
    def __init__(self, engine: Engine, embedder):
        self.engine = engine
        self.embedder = embedder

    def build_for_rows(self, core_rows: List[Dict[str, Any]]) -> int:
        if not self.embedder.enabled:
            return 0

        eligible = [r for r in core_rows if r.get("semantic_text")]
        if not eligible:
            return 0

        inserted = 0
        for i in range(0, len(eligible), _BATCH):
            batch = eligible[i: i + _BATCH]
            texts = [r["semantic_text"] for r in batch]
            try:
                vecs = self.embedder.embed(texts)
            except Exception:
                log.exception("Embedding batch %d failed — skipping", i)
                continue

            records = []
            for row, vec in zip(batch, vecs):
                records.append({
                    "id":            str(uuid.uuid4()),
                    "type":          "tx",
                    "text":          row["semantic_text"],
                    "tx_id":         row["tx_id"],
                    "source_bank":   row["source_bank"],
                    "semantic_text": row["semantic_text"],
                    "source_columns": {
                        k: row.get(k)
                        for k in ("operation_type_raw", "sdp_name", "purpose_text", "raw_note")
                        if row.get(k)
                    },
                    "embedding": _vec_to_pg(vec),
                })

            inserted += self._insert_batch(records)

        log.info("SemanticCatalogBuilder: inserted %d / %d records", inserted, len(eligible))
        return inserted

    def rebuild_from_db(self, limit: int = 100_000) -> int:
        if not self.embedder.enabled:
            log.warning("Embedder disabled — rebuild skipped")
            return 0

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT tc.tx_id, tc.source_bank, tc.semantic_text,
                           tc.operation_type_raw, tc.sdp_name,
                           tc.purpose_text, tc.raw_note
                    FROM afm.transactions_core tc
                    LEFT JOIN afm.semantic_catalog sc ON sc.tx_id = tc.tx_id
                    WHERE tc.semantic_text IS NOT NULL
                      AND sc.id IS NULL
                    LIMIT :lim
                    """
                ),
                {"lim": limit},
            ).mappings().all()

        if not rows:
            log.info("No new rows to catalog.")
            return 0

        log.info("Rebuilding catalog for %d transactions…", len(rows))
        return self.build_for_rows([dict(r) for r in rows])

    def _insert_batch(self, records: List[Dict[str, Any]]) -> int:
        import json
        count = 0
        with self.engine.begin() as conn:
            for r in records:
                try:
                    conn.execute(
                        text(
                            """
                            INSERT INTO afm.semantic_catalog
                              (id, type, text, tx_id, source_bank,
                               semantic_text, source_columns, embedding)
                            VALUES (
                              CAST(:id AS uuid),
                              :type,
                              :text,
                              CAST(:tx_id AS uuid),
                              :source_bank,
                              :semantic_text,
                              CAST(:source_columns AS jsonb),
                              CAST(:embedding AS vector)
                            )
                            ON CONFLICT DO NOTHING;
                            """
                        ),
                        {
                            "id":             r["id"],
                            "type":           r["type"],
                            "text":           r["text"],
                            "tx_id":          r["tx_id"],
                            "source_bank":    r["source_bank"],
                            "semantic_text":  r["semantic_text"],
                            "source_columns": json.dumps(r["source_columns"], ensure_ascii=False),
                            "embedding":      r["embedding"],
                        },
                    )
                    count += 1
                except Exception:
                    log.debug("Skipping duplicate catalog record")
        return count
