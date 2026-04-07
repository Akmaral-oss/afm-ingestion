#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.classification.category_service import CategoryService
from app.config import settings
from app.db.schema import ensure_schema
from app.ingestion.mapping.canonical_mapper import _build_semantic_text
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.logging_config import setup_logging

_BATCH = 256


def _vec_to_pg_literal(vec: np.ndarray) -> str:
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{value:.6f}" for value in arr) + "]"


def _build_runtime_settings(args):
    overrides = {
        "PG_DSN": args.pg,
        "EMBEDDING_MODEL_PATH": args.model,
        "AFM_EMBEDDING_PROVIDER": args.embedding_provider,
        "AFM_EMBEDDING_BASE_URL": args.embedding_url,
        "AFM_EMBEDDING_TIMEOUT_S": args.embedding_timeout,
    }
    overrides = {key: value for key, value in overrides.items() if value is not None}
    return settings.model_copy(update=overrides)


def _fetch_rows(engine, limit: int | None) -> List[Dict[str, Any]]:
    sql = """
        SELECT tx_id::text AS tx_id,
               operation_type_raw,
               sdp_name,
               purpose_text,
               raw_note
        FROM afm.transactions_core
        WHERE semantic_text IS NULL OR semantic_embedding IS NULL
        ORDER BY operation_ts NULLS LAST, tx_id
    """
    params: Dict[str, Any] = {}
    if limit:
        sql += " LIMIT :lim"
        params["lim"] = limit

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
        return [dict(row) for row in rows]


def _semantic_embedding_is_vector(engine) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT data_type, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'afm'
                  AND table_name = 'transactions_core'
                  AND column_name = 'semantic_embedding'
                """
            )
        ).first()
    if not row:
        return False
    data_type, udt_name = row
    return str(data_type).lower() == "user-defined" and str(udt_name).lower() == "vector"


def _backfill_semantic(engine, embedder: EmbeddingBackend, limit: int | None) -> int:
    rows = _fetch_rows(engine, limit)
    if not rows:
        return 0

    payloads = []
    for row in rows:
        core = {
            "operation_type_raw": row.get("operation_type_raw"),
            "sdp_name": row.get("sdp_name"),
            "purpose_text": row.get("purpose_text"),
            "raw_note": row.get("raw_note"),
        }
        semantic_text = _build_semantic_text(core) or None
        payloads.append({
            "tx_id": row["tx_id"],
            "semantic_text": semantic_text,
            "semantic_embedding": None,
        })

    if embedder.enabled:
        text_indices = [idx for idx, row in enumerate(payloads) if row["semantic_text"]]
        texts = [payloads[idx]["semantic_text"] for idx in text_indices]
        for start in range(0, len(texts), _BATCH):
            batch_texts = texts[start:start + _BATCH]
            batch_indices = text_indices[start:start + _BATCH]
            vectors = embedder.embed(batch_texts)
            for list_idx, vec in zip(batch_indices, vectors):
                payloads[list_idx]["semantic_embedding"] = _vec_to_pg_literal(vec)

    embedding_is_vector = _semantic_embedding_is_vector(engine)
    update_sql = (
        """
        UPDATE afm.transactions_core
        SET semantic_text = :semantic_text,
            semantic_embedding = CASE
                WHEN :semantic_embedding IS NULL THEN semantic_embedding
                ELSE CAST(:semantic_embedding AS vector)
            END
        WHERE tx_id = CAST(:tx_id AS uuid)
        """
        if embedding_is_vector
        else """
        UPDATE afm.transactions_core
        SET semantic_text = :semantic_text
        WHERE tx_id = CAST(:tx_id AS uuid)
        """
    )

    with engine.begin() as conn:
        for start in range(0, len(payloads), _BATCH):
            batch = payloads[start:start + _BATCH]
            conn.execute(text(update_sql), batch)

    return len(payloads)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg", default=None, help="postgresql+psycopg2://user:pass@host:5432/dbname")
    parser.add_argument("--model", default=None, help="Embedding model (Ollama model name or local path).")
    parser.add_argument("--embedding_provider", default=None, help="Embedding provider: ollama|sentence-transformers|disabled")
    parser.add_argument("--embedding_url", default=None, help="Ollama base URL for embeddings.")
    parser.add_argument("--embedding_timeout", type=int, default=None, help="Ollama embedding timeout in seconds.")
    parser.add_argument("--backfill-categories", action="store_true", help="Reclassify rows and rebuild category clusters.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max rows for semantic backfill.")
    parser.add_argument("--loglevel", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    args = parser.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))
    runtime_settings = _build_runtime_settings(args)
    if not runtime_settings.sync_pg_dsn:
        raise SystemExit("Postgres DSN is not set. Use --pg or AFM_PG_DSN in .env.")

    engine = create_engine(runtime_settings.sync_pg_dsn, future=True)
    ensure_schema(engine)
    embedder = EmbeddingBackend(
        runtime_settings.embedding_model_path,
        provider=runtime_settings.embedding_provider,
        ollama_base_url=runtime_settings.embedding_base_url,
        ollama_timeout_s=runtime_settings.embedding_timeout_s,
    )

    updated_semantic = _backfill_semantic(engine, embedder, args.limit)
    logging.getLogger(__name__).info("Semantic backfill updated %d rows", updated_semantic)

    if args.backfill_categories:
        category_service = CategoryService(engine=engine, embedder=embedder)
        updated_categories = category_service.reclassify_from_db(limit=args.limit or 200_000)
        logging.getLogger(__name__).info("Category backfill updated %d rows", updated_categories)
        try:
            rebuilt = category_service.rebuild_clusters_from_categories()
            logging.getLogger(__name__).info("Rebuilt %d semantic clusters", rebuilt)
        except Exception:
            logging.getLogger(__name__).exception("Cluster rebuild failed")


if __name__ == "__main__":
    main()
