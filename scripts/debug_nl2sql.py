#!/usr/bin/env python3
"""
scripts/debug_nl2sql.py

Debug tool for analyzing NL2SQL queries without executing them.

Usage:
    python scripts/debug_nl2sql.py --sql "SELECT ... FROM afm.transactions_nl_view WHERE ..."
    python scripts/debug_nl2sql.py --question "топ 10 получателей" --pg "..." --model "..." --llm_url "..."
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
from app.nl2sql.sql_debug import SQLDebugger, print_debug_analysis
from app.logging_config import setup_logging


def debug_sql(sql: str) -> None:
    """Debug a raw SQL string."""
    print(f"\n📝 Analyzing SQL:\n{sql}\n")
    print_debug_analysis(sql)


def debug_question(
    question: str,
    pg_dsn: str,
    model: str,
    llm_url: str,
    llm_model: str,
) -> None:
    """Debug a natural language question through the full pipeline."""
    engine = make_engine(pg_dsn)
    ensure_schema(engine)
    
    embedder = EmbeddingBackend(model)
    llm_backend = OllamaBackend(model=llm_model, base_url=llm_url)
    service = QueryService.build(engine, embedder, llm_backend)
    
    print(f"\n>>> {question}\n")
    result = service.run(question)
    
    print(f"SQL Generated:\n{result.sql}\n")
    print_debug_analysis(result.sql, result.rows)
    
    if result.quality_warnings:
        print("⚠️  Quality Warnings:")
        for w in result.quality_warnings:
            print(f"  • {w}")
    
    if result.error:
        print(f"\n❌ Error: {result.error}")
    else:
        print(f"\n✅ Query succeeded!")
        print(f"Rows: {len(result.rows)}")
        if result.rows:
            print(f"\nFirst result:\n{json.dumps(result.rows[0], ensure_ascii=False, indent=2, default=str)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Debug NL2SQL queries")
    
    ap.add_argument("--sql", help="Raw SQL to analyze")
    ap.add_argument("--question", help="Natural language question")
    ap.add_argument("--pg", help="PostgreSQL DSN")
    ap.add_argument("--model", default="BAAI/bge-m3", help="Embedding model")
    ap.add_argument("--llm_url", default="http://localhost:11434", help="Ollama URL")
    ap.add_argument("--llm_model", default="qwen2.5-coder:7b", help="LLM model")
    ap.add_argument("--loglevel", default="WARNING", help="Log level")
    
    args = ap.parse_args()
    setup_logging(getattr(logging, args.loglevel.upper(), logging.WARNING))
    
    if args.sql:
        debug_sql(args.sql)
    elif args.question:
        if not args.pg:
            raise SystemExit("--pg required when using --question")
        debug_question(args.question, args.pg, args.model, args.llm_url, args.llm_model)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
