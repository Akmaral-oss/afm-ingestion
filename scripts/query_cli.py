#!/usr/bin/env python3
"""
scripts/query_cli.py

CLI for the NL→SQL query service.

Usage:
    python scripts/query_cli.py \
        --pg "postgresql+psycopg2://user:pass@localhost/afm" \
        --model models/bge-m3 \
        --llm_url http://localhost:11434 \
        --llm_model "llama4-scout:latest" \
        "платежи по займам больше 5 млн за 2024"
"""
from __future__ import annotations

import argparse
import json
import logging

from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.nl2sql.sql_generator import OllamaBackend
from app.logging_config import setup_logging


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg", required=True)
    ap.add_argument("--model", default=None, help="BGE-M3 path (optional)")
    ap.add_argument("--llm_url", default="http://localhost:11434")
    ap.add_argument("--llm_model", default="llama4-scout:latest")
    ap.add_argument("--loglevel", default="INFO")
    ap.add_argument("question", nargs="+")
    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    engine = make_engine(args.pg)
    ensure_schema(engine)

    embedder = EmbeddingBackend(args.model)
    llm_backend = OllamaBackend(model=args.llm_model, base_url=args.llm_url)

    service = QueryService.build(engine, embedder, llm_backend)

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
