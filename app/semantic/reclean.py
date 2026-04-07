"""
app/semantic/reclean.py
───────────────────────────
Пересчитывает semantic_text для всех строк transactions_core используя
улучшенную функцию build_semantic_text() с очисткой мусора.

Затем очищает semantic_catalog и перестраивает его с новыми текстами.
Итог: кластеризация получит чистые тексты без номеров договоров, ФИО и дат.

Запуск:
  python -m app.semantic.reclean \
    --pg 'postgresql://afm_user:123!@localhost:5433/afm_db' \
    --model models/bge-m3
"""
from __future__ import annotations
import argparse, logging, os, sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pg", required=True)
    p.add_argument("--model", default=None)
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true", help="Show stats without writing")
    args = p.parse_args()

    if args.pg:
        os.environ["AFM_PG_DSN"] = args.pg
    if args.model:
        os.environ["AFM_EMBEDDING_MODEL_PATH"] = args.model

    from sqlalchemy import text
    from app.db.engine import make_engine
    from app.semantic.cluster_builder import build_semantic_text, _clean_for_embedding

    engine = make_engine(args.pg)

    # ── Step 1: Reclean semantic_text in transactions_core ────────────────────
    log.info("Step 1: re-cleaning semantic_text in transactions_core...")

    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM afm.transactions_core")
        ).scalar()
    log.info("Total transactions: %d", total)

    updated = 0
    offset = 0
    while offset < total:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT tx_id, operation_type_raw, sdp_name, purpose_text, raw_note
                    FROM afm.transactions_core
                    ORDER BY tx_id
                    LIMIT :lim OFFSET :off
                """),
                {"lim": args.batch, "off": offset}
            ).fetchall()

        if not rows:
            break

        if not args.dry_run:
            with engine.begin() as conn:
                for row in rows:
                    new_text = build_semantic_text(
                        operation_type_raw=row[1],
                        sdp_name=row[2],
                        purpose_text=row[3],
                        raw_note=row[4],
                    )
                    conn.execute(
                        text("UPDATE afm.transactions_core SET semantic_text = :t WHERE tx_id = CAST(:id AS uuid)"),
                        {"t": new_text, "id": row[0]}
                    )
        updated += len(rows)
        offset += args.batch
        log.info("  Processed %d / %d rows", min(offset, total), total)

    log.info("Step 1 done: %d rows re-cleaned", updated)

    if args.dry_run:
        log.info("DRY RUN — no changes written")
        return

    # ── Step 2: Clear and rebuild semantic_catalog ────────────────────────────
    log.info("Step 2: clearing semantic_catalog...")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM afm.semantic_catalog"))
    log.info("semantic_catalog cleared")

    # ── Step 3: Re-embed (if model available) ─────────────────────────────────
    from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
    embedder = EmbeddingBackend(args.model)

    if not embedder.enabled:
        log.warning(
            "Embedder OFF — catalog will be empty until model is available.\n"
            "Run again with a working --model path to generate embeddings.\n"
            "Cluster build from existing vectors in transactions_core is not affected."
        )
        return

    log.info("Step 3: re-embedding with cleaned texts (this may take a few minutes)...")
    from app.semantic.semantic_catalog_builder import SemanticCatalogBuilder
    builder = SemanticCatalogBuilder(engine, embedder)
    n = builder.rebuild_from_db(limit=200_000)
    log.info("Step 3 done: %d rows catalogued", n)

    # ── Step 4: Rebuild clusters ───────────────────────────────────────────────
    log.info("Step 4: rebuilding clusters with clean vectors...")
    from app.semantic.cluster_builder import ClusterBuilder
    from app.db.schema import rebuild_ivfflat_indexes

    cb = ClusterBuilder(engine=engine, k_min=8)
    n_clusters = cb.run()
    rebuild_ivfflat_indexes(engine)
    log.info("Done: %d clusters rebuilt with clean semantic_text", n_clusters)


if __name__ == "__main__":
    import sys as _sys
    _sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
    main()
