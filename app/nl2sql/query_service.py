from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .entity_extractor import extract_entities
from .prompt_builder import build_prompt
from .query_executor import QueryExecutor
from .query_models import QueryPlan
from .semantic_retriever import SemanticRetriever
from .sql_generator import LLMBackend, SQLGenerator
from .sql_repair import SQLRepair
from .sql_validator import SQLValidationError, ensure_limit, validate_sql

log = logging.getLogger(__name__)


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


class QueryService:
    def __init__(
        self,
        generator: SQLGenerator,
        repair: SQLRepair,
        retriever: SemanticRetriever,
        executor: QueryExecutor,
        embedder,
        engine: Engine,
        *,
        save_history: bool = True,
        semantic_enabled: bool = False,
    ):
        self.generator = generator
        self.repair = repair
        self.retriever = retriever
        self.executor = executor
        self.embedder = embedder
        self.engine = engine
        self.save_history = save_history
        self.semantic_enabled = semantic_enabled
        self._history_embedding_is_vector: Optional[bool] = None
        self._query_history_exists_cache: Optional[bool] = None

    @classmethod
    def build(
        cls,
        engine: Engine,
        embedder,
        llm_backend: LLMBackend,
        *,
        save_history: bool = True,
        max_new_tokens: int = 512,
    ) -> "QueryService":
        generator = SQLGenerator(llm_backend, max_new_tokens=max_new_tokens)
        return cls(
            generator=generator,
            repair=SQLRepair(generator),
            retriever=SemanticRetriever(engine, embedder),
            executor=QueryExecutor(engine),
            embedder=embedder,
            engine=engine,
            save_history=save_history,
            semantic_enabled=bool(getattr(embedder, "enabled", False)),
        )

    def run(self, question: str) -> QueryResult:
        started_at = time.perf_counter()
        sql = ""
        rows: List[Dict[str, Any]] = []
        repaired = False
        query_embedding = None

        try:
            entities = extract_entities(question)
            if self.semantic_enabled:
                embed_text = entities.semantic_topic or question
                query_embedding = self.embedder.embed([embed_text])[0]

            context = self.retriever.retrieve(question, semantic_topic=entities.semantic_topic)
            prompt = build_prompt(
                QueryPlan(
                    question=question,
                    entities=entities,
                    context=context,
                    query_embedding=query_embedding,
                ),
                include_semantic=self.semantic_enabled,
            )

            sql = ensure_limit(self.generator.generate(prompt))
            validate_sql(sql, allow_semantic=self.semantic_enabled)
            rows = self.executor.execute(
                sql,
                query_embedding=query_embedding if ":query_embedding" in sql else None,
            )
        except (SQLValidationError, Exception) as exc:
            if sql:
                repaired_sql = sql
                try:
                    repaired_sql = ensure_limit(self.repair.repair(sql, str(exc)))
                    validate_sql(repaired_sql, allow_semantic=self.semantic_enabled)
                    rows = self.executor.execute(
                        repaired_sql,
                        query_embedding=query_embedding if ":query_embedding" in repaired_sql else None,
                    )
                    sql = repaired_sql
                    repaired = True
                except Exception as repair_exc:
                    result = QueryResult(
                        question=question,
                        sql=repaired_sql,
                        rows=[],
                        execution_time_s=time.perf_counter() - started_at,
                        error=str(repair_exc),
                    )
                    self._save_history(result, query_embedding)
                    return result
            else:
                result = QueryResult(
                    question=question,
                    sql="",
                    rows=[],
                    execution_time_s=time.perf_counter() - started_at,
                    error=str(exc),
                )
                self._save_history(result, query_embedding)
                return result

        result = QueryResult(
            question=question,
            sql=sql,
            rows=rows,
            execution_time_s=time.perf_counter() - started_at,
            repaired=repaired,
        )
        self._save_history(result, query_embedding)
        return result

    def _save_history(self, result: QueryResult, embedding) -> None:
        if not self.save_history:
            return
        if not self._query_history_exists():
            return
        try:
            from app.ingestion.mapping.embedding_mapper import EmbeddingBackend

            embedding_value: Optional[object] = None
            embedding_expr = ":embedding"
            if self.semantic_enabled and embedding is not None:
                if self._uses_vector_history():
                    embedding_value = _vec_to_pg_literal(embedding)
                    embedding_expr = "CAST(:embedding AS vector)"
                else:
                    embedding_value = EmbeddingBackend.vec_to_bytes(embedding)

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO afm.query_history
                          (id, question, generated_sql, execution_success, embedding)
                        VALUES
                          (CAST(:id AS uuid), :question, :sql, :ok, {embedding_expr})
                        """
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "question": result.question,
                        "sql": result.sql,
                        "ok": result.success,
                        "embedding": embedding_value,
                    },
                )
        except Exception:
            log.warning("Failed to save query history (non-fatal)")

    def _uses_vector_history(self) -> bool:
        if self._history_embedding_is_vector is not None:
            return self._history_embedding_is_vector

        with self.engine.connect() as conn:
            value = conn.execute(
                text(
                    """
                    SELECT atttypid::regtype::text
                    FROM pg_attribute
                    WHERE attrelid = 'afm.query_history'::regclass
                      AND attname = 'embedding'
                      AND NOT attisdropped
                    """
                )
            ).scalar()
        self._history_embedding_is_vector = value == "vector"
        return self._history_embedding_is_vector

    def _query_history_exists(self) -> bool:
        if self._query_history_exists_cache is not None:
            return self._query_history_exists_cache

        with self.engine.connect() as conn:
            value = conn.execute(
                text("SELECT to_regclass('afm.query_history') IS NOT NULL")
            ).scalar()
        self._query_history_exists_cache = bool(value)
        return self._query_history_exists_cache


def _vec_to_pg_literal(vec: Any) -> str:
    import numpy as np

    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{value:.6f}" for value in arr) + "]"
