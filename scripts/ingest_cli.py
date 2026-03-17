#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logging
from app.config import Settings
from app.ingestion.pipeline import IngestionPipeline


def main():

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (optional).",
    )

    ap.add_argument(
        "--pg",
        default=None,
        help="postgresql+psycopg2://user:pass@host:5432/dbname (or AFM_PG_DSN from .env)"
    )

    ap.add_argument(
        "--model",
        default=None,
        help="Embedding model (Ollama model name or local path). Optional."
    )

    ap.add_argument(
        "--embedding_provider",
        default=None,
        help="Embedding provider: ollama|sentence-transformers|disabled"
    )

    ap.add_argument(
        "--embedding_url",
        default=None,
        help="Ollama base URL for embeddings (optional)."
    )

    ap.add_argument(
        "--embedding_timeout",
        type=int,
        default=None,
        help="Ollama embedding timeout in seconds."
    )

    ap.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Embedding threshold for header->canonical mapping."
    )

    ap.add_argument(
        "--format_sim",
        type=float,
        default=None,
        help="Format registry embedding similarity threshold."
    )

    ap.add_argument(
        "--bank",
        default=None,
        help="Force bank label: halyk|kaspi|..."
    )

    ap.add_argument(
        "--rawjson",
        action="store_true",
        help="Store raw_row_json in transactions_core."
    )

    ap.add_argument(
        "--data",
        help="Root folder with bank subfolders (data/kaspi, data/halyk)"
    )

    ap.add_argument(
        "--loglevel",
        default="INFO",
        help="DEBUG|INFO|WARNING|ERROR"
    )

    ap.add_argument(
        "files",
        nargs="*",
        help="Direct XLSX files"
    )

    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    pg_dsn = args.pg or settings.pg_dsn
    if not pg_dsn:
        raise SystemExit("Postgres DSN is not set. Use --pg or AFM_PG_DSN in .env.")

    embedding_model_path = args.model if args.model is not None else settings.embedding_model_path
    embedding_provider = (
        args.embedding_provider if args.embedding_provider is not None else settings.embedding_provider
    )
    embedding_base_url = args.embedding_url or settings.embedding_base_url
    embedding_timeout_s = (
        args.embedding_timeout if args.embedding_timeout is not None else settings.embedding_timeout_s
    )
    embedding_threshold = (
        args.threshold if args.threshold is not None else settings.embedding_threshold
    )
    format_similarity_threshold = (
        args.format_sim if args.format_sim is not None else settings.format_similarity_threshold
    )
    store_raw_row_json = args.rawjson or settings.store_raw_row_json

    settings = Settings(
        pg_dsn=pg_dsn,
        embedding_model_path=embedding_model_path,
        embedding_provider=embedding_provider,
        embedding_base_url=embedding_base_url,
        embedding_timeout_s=embedding_timeout_s,
        embedding_threshold=embedding_threshold,
        format_similarity_threshold=format_similarity_threshold,
        store_raw_row_json=store_raw_row_json,
        parser_version=settings.parser_version,
        max_meta_lookback_rows=settings.max_meta_lookback_rows,
    )

    pipe = IngestionPipeline(settings)

    # -------- MODE 1: folder ingestion --------
    if args.data:

        pipe.ingest_data_folder(args.data)

    # -------- MODE 2: file ingestion --------
    elif args.files:

        for f in args.files:
            pipe.ingest_file(f, source_bank=args.bank)

    else:

        raise SystemExit(
            "Provide either --data <folder> or XLSX files"
        )


if __name__ == "__main__":
    main()