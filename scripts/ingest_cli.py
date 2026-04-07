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
from app.config import settings
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

    # --- ШАГ 1: Собираем только те аргументы, которые юзер ввел вручную ---
    overrides = {
        "pg_dsn": args.pg,
        "embedding_model_path": args.model,
        "embedding_provider": args.embedding_provider,
        "embedding_base_url": args.embedding_url,
        "embedding_timeout_s": args.embedding_timeout,
        "embedding_threshold": args.threshold,
        "format_similarity_threshold": args.format_sim,
        "store_raw_row_json": args.rawjson if args.rawjson else None,
    }

    # Удаляем None, чтобы не затереть дефолтные настройки из .env
    overrides = {k: v for k, v in overrides.items() if v is not None}

    # --- ШАГ 2: Создаем финальный конфиг ---
    # Мы берем все значения из базового settings и заменяем их на overrides
    final_settings = settings.model_copy(update=overrides)

    # Проверка критического параметра
    if not final_settings.sync_pg_dsn:
        raise SystemExit("Postgres DSN is not set. Use --pg or AFM_PG_DSN in .env.")

    # --- ШАГ 3: Запуск ---
    pipe = IngestionPipeline(final_settings.ingestion_settings)

    if args.data:
        pipe.ingest_data_folder(args.data)
    elif args.files:
        for f in args.files:
            pipe.ingest_file(f, source_bank=args.bank)
    else:
        raise SystemExit("Provide either --data <folder> or XLSX files")

if __name__ == "__main__":
    main()
