#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.nl2sql.sql_generator import build_llm_backend


def main() -> None:
    parser = argparse.ArgumentParser(description="Run NL2SQL queries against AFM")
    parser.add_argument("question", nargs="+")
    parser.add_argument("--llm-model", default=settings.AFM_LLM_MODEL)
    parser.add_argument("--llm-url", default=settings.AFM_LLM_BASE_URL)
    parser.add_argument("--llm-timeout", type=int, default=settings.AFM_LLM_TIMEOUT_S)
    parser.add_argument("--max-new-tokens", type=int, default=settings.AFM_LLM_MAX_NEW_TOKENS)
    parser.add_argument("--embedding-provider", default=settings.AFM_EMBEDDING_PROVIDER)
    parser.add_argument("--embedding-model", default=settings.nl2sql_embedding_model)
    parser.add_argument("--embedding-url", default=settings.AFM_EMBEDDING_BASE_URL)
    parser.add_argument("--embedding-timeout", type=int, default=settings.AFM_EMBEDDING_TIMEOUT_S)
    args = parser.parse_args()

    engine = make_engine(settings.sync_pg_dsn)
    try:
        ensure_schema(engine)

        embedder = EmbeddingBackend(
            args.embedding_model,
            provider=args.embedding_provider,
            ollama_base_url=args.embedding_url,
            ollama_timeout_s=args.embedding_timeout,
        )
        llm_backend = build_llm_backend(
            args.llm_model,
            base_url=args.llm_url,
            timeout_s=args.llm_timeout,
        )
        service = QueryService.build(
            engine,
            embedder,
            llm_backend,
            save_history=settings.NL2SQL_SAVE_HISTORY,
            max_new_tokens=args.max_new_tokens,
        )

        question = " ".join(args.question)
        result = service.run(question)

        print(f"Question: {result.question}")
        print(f"Success: {result.success}")
        print(f"SQL:\n{result.sql}\n")
        print(f"Rows returned: {len(result.rows)}")
        print(f"Execution time: {result.execution_time_s:.3f}s")
        if result.repaired:
            print("Auto-repaired: yes")
        if result.error:
            print(f"Error: {result.error}")
        else:
            print(json.dumps(result.rows[:5], ensure_ascii=False, indent=2, default=str))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
