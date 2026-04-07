#!/usr/bin/env python3
"""
scripts/query_cli.py

NL → SQL CLI with multi-LLM support:
- Ollama (local)
- XiYanSQL (vLLM server)
- OmniSQL (vLLM server)
"""

from __future__ import annotations

import argparse
import json
import logging

from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.logging_config import setup_logging


# ----------------------------
# LLM BACKENDS
# ----------------------------

from app.nl2sql.sql_generator import OllamaBackend


class VLLMBackend:
    """
    Generic vLLM OpenAI-compatible backend
    used for XiYanSQL / OmniSQL
    """
    def __init__(self, base_url: str, model: str = "model"):
        import requests
        self.base_url = base_url
        self.model = model
        self.requests = requests

    def generate(self, prompt: str) -> str:
        resp = self.requests.post(
            f"{self.base_url}/v1/completions",
            json={
                "model": self.model,
                "prompt": prompt,
                "max_tokens": 256,
                "temperature": 0
            }
        )
        return resp.json()["choices"][0]["text"]


# ----------------------------
# ROUTER
# ----------------------------

def build_llm(args):
    """
    Choose LLM backend dynamically
    """

    if args.llm_provider == "ollama":
        return OllamaBackend(
            model=args.llm_model,
            base_url=args.llm_url
        )

    elif args.llm_provider == "xiyan":
        return VLLMBackend(
            base_url="http://localhost:8001",
            model="XGenerationLab/XiYanSQL-QwenCoder-14B-2504"
        )

    elif args.llm_provider == "omni":
        return VLLMBackend(
            base_url="http://localhost:8002",
            model="seeklhy/OmniSQL-14B"
        )

    else:
        raise ValueError(f"Unknown llm_provider: {args.llm_provider}")


# ----------------------------
# MAIN
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--pg", required=True)

    ap.add_argument("--model", default=None, help="BGE-M3 embedding model")

    ap.add_argument("--llm_url", default="http://localhost:11434")
    ap.add_argument("--llm_model", default="qwen2.5-coder:14b")

    ap.add_argument(
        "--llm_provider",
        default="ollama",
        choices=["ollama", "xiyan", "omni"]
    )

    ap.add_argument("--loglevel", default="INFO")

    ap.add_argument("question", nargs="+")

    args = ap.parse_args()

    setup_logging(getattr(logging, args.loglevel.upper(), logging.INFO))

    # DB init
    engine = make_engine(args.pg)
    ensure_schema(engine)

    # Embeddings
    embedder = EmbeddingBackend(args.model)

    # LLM selection (🔥 key upgrade)
    llm_backend = build_llm(args)

    # Service
    service = QueryService.build(engine, embedder, llm_backend)

    question = " ".join(args.question)

    print("\n============================")
    print("QUESTION:", question)
    print("============================\n")

    result = service.run(question)

    print("SQL:\n", result.sql, "\n")
    print("Rows returned:", len(result.rows))
    print(f"Execution time: {result.execution_time_s:.3f}s")

    if result.repaired:
        print("(SQL was auto-repaired)")

    if result.error:
        print("ERROR:", result.error)
    else:
        print(
            json.dumps(
                result.rows[:5],
                ensure_ascii=False,
                indent=2,
                default=str
            )
        )


if __name__ == "__main__":
    main()