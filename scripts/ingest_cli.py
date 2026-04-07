#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.ingestion.pipeline import IngestionPipeline
from app.logging_config import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=None, help="Path to .env file (optional).")
    parser.add_argument("--pg", default=None, help="postgresql+psycopg2://user:pass@host:5432/dbname (or AFM_PG_DSN from .env)")
    parser.add_argument("--model", default=None, help="Embedding model (Ollama model name or local path).")
    parser.add_argument("--embedding_provider", default=None, help="Embedding provider: ollama|sentence-transformers|disabled")
    parser.add_argument("--embedding_url", default=None, help="Ollama base URL for embeddings.")
    parser.add_argument("--embedding_timeout", type=int, default=None, help="Ollama embedding timeout in seconds.")
    parser.add_argument("--threshold", type=float, default=None, help="Embedding threshold for header mapping.")
    parser.add_argument("--format_sim", type=float, default=None, help="Format registry similarity threshold.")
    parser.add_argument("--bank", default=None, help="Force bank label: halyk|kaspi|...")
    parser.add_argument("--rawjson", action="store_true", help="Store raw_row_json in transactions_core.")
    parser.add_argument("--data", help="Root folder with bank subfolders (data/kaspi, data/halyk)")
    parser.add_argument("--loglevel", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    parser.add_argument("files", nargs="*", help="Direct XLSX files")
    args = parser.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    overrides = {
        "PG_DSN": args.pg,
        "EMBEDDING_MODEL_PATH": args.model,
        "AFM_EMBEDDING_PROVIDER": args.embedding_provider,
        "AFM_EMBEDDING_BASE_URL": args.embedding_url,
        "AFM_EMBEDDING_TIMEOUT_S": args.embedding_timeout,
        "EMBEDDING_THRESHOLD": args.threshold,
        "FORMAT_SIMILARITY_THRESHOLD": args.format_sim,
        "STORE_RAW_ROW_JSON": args.rawjson if args.rawjson else None,
    }
    overrides = {key: value for key, value in overrides.items() if value is not None}
    final_settings = settings.model_copy(update=overrides)

    if not final_settings.sync_pg_dsn:
        raise SystemExit("Postgres DSN is not set. Use --pg or AFM_PG_DSN in .env.")

    pipeline = IngestionPipeline(final_settings)
    if args.data:
        pipeline.ingest_data_folder(args.data)
    elif args.files:
        for file_path in args.files:
            pipeline.ingest_file(file_path, source_bank=args.bank)
    else:
        raise SystemExit("Provide either --data <folder> or XLSX files")


if __name__ == "__main__":
    main()
