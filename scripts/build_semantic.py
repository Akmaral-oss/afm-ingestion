#!/usr/bin/env python3
"""
scripts/build_semantic.py

Offline semantic intelligence pipeline:

  Step 1 — Catalog:   embed all transactions missing from semantic_catalog
  Step 2 — Clusters:  HDBSCAN / KMeans cluster the catalog embeddings
  Step 3 — Labels:    (optional) LLM-label each cluster

Run this:
  • Once after initial bulk ingestion
  • Periodically (e.g. nightly) as new data arrives

Usage:
    python -m scripts.build_semantic \
        --pg "postgresql+psycopg2://afm:afmpass@localhost:5432/afmdb" \
        --model BAAI/bge-m3 \
        [--bank kaspi] \
        [--llm_url http://localhost:11434] \
        [--llm_model llama4-scout:latest] \
        [--limit 100000]
"""
from __future__ import annotations

import argparse
import logging

from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.logging_config import setup_logging
from app.semantic.semantic_catalog_builder import SemanticCatalogBuilder
from app.semantic.cluster_builder import ClusterBuilder
from app.semantic.cluster_labeler import ClusterLabeler


def main() -> None:
    ap = argparse.ArgumentParser(description="Build semantic catalog + clusters")
    ap.add_argument("--pg",        required=True, help="PostgreSQL DSN")
    ap.add_argument("--model",     required=True, help="BGE-M3 path or HF repo id")
    ap.add_argument("--bank",      default=None,  help="Filter by source_bank (optional)")
    ap.add_argument("--limit",     type=int, default=100_000)
    ap.add_argument("--llm_url",   default=None,  help="Ollama URL for cluster labeling")
    ap.add_argument("--llm_model", default="llama4-scout:latest")
    ap.add_argument("--loglevel",  default="INFO")
    ap.add_argument("--skip_catalog",  action="store_true")
    ap.add_argument("--skip_clusters", action="store_true")
    ap.add_argument("--skip_labels",   action="store_true")
    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))
    log = logging.getLogger("build_semantic")

    engine = make_engine(args.pg)
    ensure_schema(engine)

    embedder = EmbeddingBackend(args.model)
    if not embedder.enabled:
        raise SystemExit("Embedding model failed to load. Check --model path.")

    # ── Step 1: catalog ───────────────────────────────────────────────────────
    if not args.skip_catalog:
        log.info("=== Step 1: Building semantic catalog ===")
        builder = SemanticCatalogBuilder(engine, embedder)
        n = builder.rebuild_from_db(limit=args.limit)
        log.info("Catalog: %d new records", n)
    else:
        log.info("Skipping catalog step.")

    # ── Step 2: clusters ──────────────────────────────────────────────────────
    if not args.skip_clusters:
        log.info("=== Step 2: Clustering ===")
        clusterer = ClusterBuilder(engine)
        n_clusters = clusterer.run(source_bank=args.bank)
        log.info("Clusters created: %d", n_clusters)
    else:
        log.info("Skipping cluster step.")

    # ── Step 3: labels ────────────────────────────────────────────────────────
    if not args.skip_labels:
        log.info("=== Step 3: Labeling clusters ===")
        llm_backend = None
        if args.llm_url:
            from app.nl2sql.sql_generator import OllamaBackend
            from app.nl2sql.sql_generator import SQLGenerator
            llm_backend = SQLGenerator(OllamaBackend(args.llm_model, args.llm_url))
        labeler = ClusterLabeler(
            engine,
            llm=llm_backend,
            heuristic_only=(llm_backend is None),
        )
        n_labeled = labeler.label_all()
        log.info("Clusters (re)labeled: %d", n_labeled)
    else:
        log.info("Skipping label step.")

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
