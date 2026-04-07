"""
scripts/build_semantic.py
Offline semantic cluster rebuild + classification backfill.

Usage:
  python -m scripts.build_semantic \
    --pg 'postgresql://afm_user:pass@localhost:5433/afm_db' \
    --model models/bge-m3

  # Also backfill categories for existing rows + rebuild category clusters:
  python -m scripts.build_semantic \
    --pg '...' --model models/bge-m3 \
    --rebuild-from-db --backfill-categories --rebuild-category-clusters
"""
from __future__ import annotations
import argparse, logging, os
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    p = argparse.ArgumentParser(description="Build semantic clusters + backfill categories")
    p.add_argument("--pg",    default=None, help="PostgreSQL DSN (overrides AFM_PG_DSN env)")
    p.add_argument("--model", default=None, help="BGE-M3 model path")
    p.add_argument("--bank",  default=None, help="Filter by source_bank (default: all)")
    p.add_argument("--k-min", type=int, default=8)
    p.add_argument("--k-max", type=int, default=0, help="0 = Hartigan auto")
    p.add_argument("--label-with-llm",        action="store_true")
    p.add_argument("--rebuild-from-db",        action="store_true",
                   help="Re-embed un-catalogued transactions first")
    p.add_argument("--backfill-categories",    action="store_true",
                   help="Classify all rows not yet in transaction_classification")
    p.add_argument("--rebuild-category-clusters", action="store_true",
                   help="Replace k-means clusters with 19 fixed category centroids")
    args = p.parse_args()

    if args.pg:
        os.environ["AFM_PG_DSN"] = args.pg
    if args.model:
        os.environ["AFM_EMBEDDING_MODEL_PATH"] = args.model

    from app.config import load_settings_from_env
    from app.db.engine import make_engine
    from app.db.schema import ensure_schema, rebuild_ivfflat_indexes
    from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
    from app.semantic.cluster_builder import ClusterBuilder
    from app.semantic.cluster_labeler import ClusterLabeler
    from app.semantic.semantic_catalog_builder import SemanticCatalogBuilder

    settings = load_settings_from_env()
    engine   = make_engine(settings.pg_dsn)
    ensure_schema(engine)
    embedder = EmbeddingBackend(settings.embedding_model_path)

    # ── Step 1: re-embed uncatalogued transactions ────────────────────────────
    if args.rebuild_from_db:
        log.info("Re-embedding un-catalogued transactions...")
        n = SemanticCatalogBuilder(engine, embedder).rebuild_from_db()
        log.info("Catalogued %d new rows", n)

    # ── Step 2: backfill classification ──────────────────────────────────────
    if args.backfill_categories:
        log.info("Backfilling transaction categories...")
        from app.classification.category_service import CategoryService
        svc = CategoryService(engine=engine, embedder=embedder)
        n = svc.reclassify_from_db(limit=500_000)
        log.info("Classified %d rows", n)

    # ── Step 3: k-means clusters (discovery tool for OTHER) ───────────────────
    if not args.rebuild_category_clusters:
        log.info("Running ClusterBuilder (bank=%s)...", args.bank or "all")
        k_max      = args.k_max if args.k_max > 0 else None
        n_clusters = ClusterBuilder(engine=engine, k_min=args.k_min, k_max=k_max).run(source_bank=args.bank)
        log.info("Built %d clusters", n_clusters)

        if args.label_with_llm and settings.llm_backend == "ollama":
            from app.nl2sql.sql_generator import OllamaBackend
            llm = OllamaBackend(model=settings.llm_model, base_url=settings.llm_base_url)
            labeler = ClusterLabeler(engine=engine, llm=llm, heuristic_only=False)
        else:
            labeler = ClusterLabeler(engine=engine, heuristic_only=True)
        log.info("Updated %d cluster labels", labeler.label_all())

    # ── Step 4: replace k-means with 19 category centroids ───────────────────
    if args.rebuild_category_clusters:
        log.info("Rebuilding semantic_clusters from fixed business categories...")
        from app.classification.category_service import CategoryService
        svc = CategoryService(engine=engine, embedder=embedder)
        n = svc.rebuild_clusters_from_categories()
        log.info("Rebuilt %d category clusters", n)

    # ── Step 5: rebuild IVFFlat indexes ──────────────────────────────────────
    log.info("Rebuilding IVFFlat indexes...")
    rebuild_ivfflat_indexes(engine)
    log.info("Done.")


if __name__ == "__main__":
    import sys as _sys
    _sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
    main()
