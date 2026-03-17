#!/usr/bin/env python3
"""
scripts/query_cli.py

CLI for the NL→SQL query service.

Usage:
    python scripts/query_cli.py \
    --env-file .env \
    --llm_model "qwen2.5-coder:14b" \
        "платежи по займам больше 5 млн за 2024"

If --pg/--model/--llm_* are omitted, values are loaded from .env.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import load_settings_from_env
from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.nl2sql.sql_generator import OllamaBackend
from app.logging_config import setup_logging


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=None, help="Path to .env file (optional)")
    ap.add_argument("--pg", default=None, help="Postgres DSN. If omitted, AFM_PG_DSN is used.")
    ap.add_argument("--model", default=None, help="Embedding model (Ollama model name or local path)")
    ap.add_argument("--embedding_provider", default=None, help="Embedding provider: ollama|sentence-transformers|disabled")
    ap.add_argument("--embedding_url", default=None, help="Ollama base URL for embeddings")
    ap.add_argument("--embedding_timeout", type=int, default=None, help="Ollama embedding timeout (seconds)")
    ap.add_argument("--llm_url", default=None, help="Ollama base URL. Falls back to AFM_LLM_BASE_URL.")
    ap.add_argument("--llm_model", default=None, help="Ollama model name. Falls back to AFM_LLM_MODEL.")
    ap.add_argument("--llm_timeout", type=int, default=None, help="Ollama request timeout in seconds.")
    ap.add_argument("--max_new_tokens", type=int, default=None, help="Max generated tokens per SQL response.")
    ap.add_argument("--loglevel", default="INFO")
    ap.add_argument("question", nargs="+")
    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    env_settings = load_settings_from_env(args.env_file)
    pg_dsn = args.pg or env_settings.pg_dsn
    if not pg_dsn:
        raise SystemExit("Postgres DSN is not set. Use --pg or AFM_PG_DSN in .env.")

    model_name = args.model if args.model is not None else env_settings.embedding_model_path
    embedding_provider = (
        args.embedding_provider if args.embedding_provider is not None else env_settings.embedding_provider
    )
    embedding_url = args.embedding_url or env_settings.embedding_base_url
    embedding_timeout = (
        args.embedding_timeout if args.embedding_timeout is not None else env_settings.embedding_timeout_s
    )
    llm_url = args.llm_url or env_settings.llm_base_url
    llm_model = args.llm_model or env_settings.llm_model_name
    llm_timeout = args.llm_timeout if args.llm_timeout is not None else env_settings.llm_timeout_s
    llm_max_new_tokens = (
        args.max_new_tokens if args.max_new_tokens is not None else env_settings.llm_max_new_tokens
    )

    engine = make_engine(pg_dsn)
    ensure_schema(engine)

    embedder = EmbeddingBackend(
        model_name,
        provider=embedding_provider,
        ollama_base_url=embedding_url,
        ollama_timeout_s=embedding_timeout,
    )
    llm_backend = OllamaBackend(model=llm_model, base_url=llm_url, timeout_s=llm_timeout)

    service = QueryService.build(
        engine,
        embedder,
        llm_backend,
        max_new_tokens=llm_max_new_tokens,
    )

    question = " ".join(args.question)
    print(f"\n>>> {question}\n")

    result = service.run(question)

    print(f"SQL:\n{result.sql}\n")
    print(f"Rows returned : {len(result.rows)}")
    print(f"Execution time: {result.execution_time_s:.3f}s")
    if result.repaired:
        print("(SQL was auto-repaired)")
    if result.error:
        print(f"ERROR: {result.error}")
    else:
        print(json.dumps(result.rows[:5], ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
