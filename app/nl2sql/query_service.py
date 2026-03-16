"""
QueryService — main orchestrator for the NL→SQL pipeline.

Full flow:
  1. Rule-based entity extraction
  2. Embedding the question (+ semantic topic if detected)
  3. Semantic retrieval (sample values + similar NL→SQL examples)
  4. Build 6-block prompt
  5. LLM SQL generation
  6. SQL validation (guardrails)
  7. Execution
  8. If error → SQL repair loop → re-validate → re-execute
  9. Save to query_history (async-safe: best-effort)

Usage:
    service = QueryService.build(settings, engine, embedder, llm_backend)
    result = service.run("платежи по займам больше 5 млн за 2024")
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .entity_extractor import extract_entities
from .prompt_builder import build_prompt
from .query_executor import QueryExecutor
from .query_models import QueryPlan, RetrievedContext
from .semantic_retriever import SemanticRetriever
from .sql_generator import LLMBackend, SQLGenerator
from .sql_repair import SQLRepair
from .sql_validator import SQLValidationError, validate_sql

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    question: str
    sql: str
    rows: List[Dict[str, Any]]
    execution_time_s: float
    repaired: bool = False
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


# ─────────────────────────────────────────────────────────────────────────────
# Main service
# ─────────────────────────────────────────────────────────────────────────────

class QueryService:
    def __init__(
        self,
        generator: SQLGenerator,
        repair: SQLRepair,
        retriever: SemanticRetriever,
        executor: QueryExecutor,
        embedder,                         # app.ingestion.mapping.embedding_mapper.EmbeddingBackend
        engine: Engine,
        save_history: bool = True,
    ):
        self.generator = generator
        self.repair = repair
        self.retriever = retriever
        self.executor = executor
        self.embedder = embedder
        self.engine = engine
        self.save_history = save_history

    # ── factory ───────────────────────────────────────────────────────────────

    @classmethod
    def build(
        cls,
        engine: Engine,
        embedder,
        llm_backend: LLMBackend,
        save_history: bool = True,
    ) -> "QueryService":
        generator = SQLGenerator(llm_backend)
        repair = SQLRepair(generator)
        retriever = SemanticRetriever(engine, embedder)
        executor = QueryExecutor(engine)
        return cls(
            generator=generator,
            repair=repair,
            retriever=retriever,
            executor=executor,
            embedder=embedder,
            engine=engine,
            save_history=save_history,
        )

    # ── main entry point ──────────────────────────────────────────────────────

    def run(self, question: str) -> QueryResult:
        t0 = time.perf_counter()
        repaired = False
        sql = ""
        rows: List[Dict[str, Any]] = []

        try:
            # 1. rule-based entity extraction
            entities = extract_entities(question)
            log.debug("Extracted entities: %s", entities)

            # 2. embed question for retrieval
            query_embedding = None
            if self.embedder.enabled:
                embed_text = entities.semantic_topic or question
                query_embedding = self.embedder.embed([embed_text])[0]

            # 3. semantic retrieval
            context = self.retriever.retrieve(
                question,
                semantic_topic=entities.semantic_topic,
            )

            # 4. build plan & prompt
            plan = QueryPlan(
                question=question,
                entities=entities,
                context=context,
                query_embedding=query_embedding,
            )
            prompt = build_prompt(plan)
            log.debug("Prompt length: %d chars", len(prompt))

            # 5. LLM SQL generation
            sql = self.generator.generate(prompt)
            log.info("Generated SQL:\n%s", sql)

            # 6. validate
            validate_sql(sql)

            # 7. execute
            rows = self.executor.execute(
                sql,
                query_embedding=query_embedding if ":query_embedding" in sql else None,
            )

        except SQLValidationError as ve:
            # attempt repair
            log.warning("Validation failed: %s — attempting repair", ve)
            sql, rows, repaired = self._repair_and_run(sql, str(ve), query_embedding)

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            result = QueryResult(
                question=question,
                sql=sql,
                rows=[],
                execution_time_s=elapsed,
                error=str(exc),
            )
            self._save_history(result, query_embedding)
            return result

        elapsed = time.perf_counter() - t0
        result = QueryResult(
            question=question,
            sql=sql,
            rows=rows,
            execution_time_s=elapsed,
            repaired=repaired,
        )
        self._save_history(result, query_embedding)
        return result

    # ── repair ────────────────────────────────────────────────────────────────

    def _repair_and_run(
        self,
        original_sql: str,
        error: str,
        query_embedding,
    ):
        repaired_sql = self.repair.repair(original_sql, error)
        validate_sql(repaired_sql)
        rows = self.executor.execute(
            repaired_sql,
            query_embedding=query_embedding if ":query_embedding" in repaired_sql else None,
        )
        return repaired_sql, rows, True

    # ── query_history persistence ─────────────────────────────────────────────

    def _save_history(self, result: QueryResult, embedding) -> None:
        if not self.save_history:
            return
        try:
            from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
            emb_bytes: Optional[bytes] = None
            if self.embedder.enabled and embedding is not None:
                emb_bytes = EmbeddingBackend.vec_to_bytes(embedding)

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.query_history
                          (id, question, generated_sql, execution_success, embedding)
                        VALUES
                          (CAST(:id AS uuid), :q, :sql, :ok, :emb)
                        """
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "q": result.question,
                        "sql": result.sql,
                        "ok": result.success,
                        "emb": emb_bytes,
                    },
                )
        except Exception:
            log.warning("Failed to save query history (non-fatal)")