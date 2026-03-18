#!/usr/bin/env python3
"""
scripts/seed_catalog.py

One-time (and re-runnable) script that:
  1. Computes BGE-M3 embeddings for all rows in afm.semantic_catalog
     that don't yet have an embedding.
  2. Extracts DISTINCT sample values from key semantic columns and
     inserts them into afm.semantic_catalog (type='value') with embeddings.

Run after: python scripts/ingest_cli.py ...

Usage:
    python scripts/seed_catalog.py --pg "postgresql+psycopg2://..." \
        --model models/bge-m3 \
        [--sample_limit 5000]
"""
from __future__ import annotations

import argparse
import logging

import numpy as np
from sqlalchemy import text

from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

_SEMANTIC_COLUMNS = [
    "purpose_text",
    "operation_type_raw",
    "sdp_name",
    "raw_note",
]


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec.reshape(-1)) + "]"


def embed_catalog(engine, embedder: EmbeddingBackend) -> None:
    """Embed rows in semantic_catalog that lack an embedding."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, text FROM afm.semantic_catalog WHERE embedding IS NULL")
        ).fetchall()

    if not rows:
        log.info("All catalog rows already embedded.")
        return

    log.info("Embedding %d catalog rows…", len(rows))
    texts = [r[1] for r in rows]
    vecs = embedder.embed(texts)

    with engine.begin() as conn:
        for (row_id, _), vec in zip(rows, vecs):
            conn.execute(
                text(
                    "UPDATE afm.semantic_catalog SET embedding = CAST(:v AS vector) WHERE id = CAST(:id AS uuid)"
                ),
                {"v": _vec_to_pg(vec), "id": str(row_id)},
            )
    log.info("Catalog embedding done.")


def seed_sample_values(engine, embedder: EmbeddingBackend, sample_limit: int) -> None:
    """Fetch distinct sample values from semantic columns and insert into catalog."""
    with engine.begin() as conn:
        for col in _SEMANTIC_COLUMNS:
            rows = conn.execute(
                text(
                    f"SELECT DISTINCT {col} FROM afm.transactions_core "
                    f"WHERE {col} IS NOT NULL AND LENGTH({col}) > 3 "
                    f"LIMIT :lim"
                ),
                {"lim": sample_limit},
            ).fetchall()

            values = [r[0] for r in rows if r[0]]
            if not values:
                continue

            log.info("Seeding %d sample values for column '%s'…", len(values), col)
            vecs = embedder.embed(values)

            for text_val, vec in zip(values, vecs):
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.semantic_catalog (type, text, embedding, meta)
                        VALUES ('value', :t, CAST(:v AS vector), CAST(:m AS jsonb))
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "t": text_val,
                        "v": _vec_to_pg(vec),
                        "m": f'{{"column":"{col}"}}',
                    },
                )
    log.info("Sample value seeding done.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg", required=True)
    ap.add_argument("--model", required=True, help="Path to BGE-M3 weights")
    ap.add_argument("--sample_limit", type=int, default=5_000)
    args = ap.parse_args()

    engine = make_engine(args.pg)
    ensure_schema(engine)

    embedder = EmbeddingBackend(args.model)
    if not embedder.enabled:
        raise SystemExit("Embedding model failed to load. Check --model path.")

    embed_catalog(engine, embedder)
    seed_sample_values(engine, embedder, args.sample_limit)
    log.info("Seeding complete.")


if __name__ == "__main__":
    main()
