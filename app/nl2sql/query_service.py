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
import re
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

_ORDER_BY_CLAUSE_RE = re.compile(
    r"(\bORDER\s+BY\b)(?P<clause>.*?)(?=\bLIMIT\b|\bOFFSET\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_ORDER_BY_AMOUNT_RE = re.compile(
    r"(?P<expr>(?:\b\w+\.)?amount_kzt)(?P<suffix>(?:\s+(?:ASC|DESC|NULLS\s+FIRST|NULLS\s+LAST))*)",
    re.IGNORECASE,
)
_SEMANTIC_ORDER_BY_RE = re.compile(
    r"\bORDER\s+BY\s+(?:\w+\.)?semantic_embedding\s*<->\s*:query_embedding(?:\s+(?:ASC|DESC))?",
    re.IGNORECASE,
)
_PROJECT_PARAM_PATTERNS = (
    re.compile(r"%\(\s*project_id\s*\)s", re.IGNORECASE),
    re.compile(r":project_id\b", re.IGNORECASE),
)


def _inject_where_predicate(sql: str, predicate: str) -> str:
    if re.search(re.escape(predicate), sql, re.IGNORECASE):
        return sql

    if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        return re.sub(r"\bWHERE\b", f"WHERE {predicate} AND ", sql, count=1, flags=re.IGNORECASE)

    insertion_match = re.search(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT)\b", sql, re.IGNORECASE)
    if insertion_match:
        idx = insertion_match.start()
        return f"{sql[:idx]}WHERE {predicate}\n{sql[idx:]}"

    stripped = sql.rstrip().rstrip(";")
    return f"{stripped}\nWHERE {predicate};"


def _normalize_ranked_amount_sql(sql: str) -> str:
    if not re.search(r"\bORDER\s+BY\s+(?:\w+\.)?amount_kzt\b", sql, re.IGNORECASE):
        return sql

    normalized = _inject_where_predicate(sql, "amount_kzt IS NOT NULL")

    order_match = _ORDER_BY_CLAUSE_RE.search(normalized)
    if not order_match:
        return normalized

    order_clause = order_match.group("clause")

    def repl(match: re.Match) -> str:
        expr = match.group("expr")
        suffix = (match.group("suffix") or "").upper()
        direction = ""
        if "DESC" in suffix:
            direction = " DESC"
        elif "ASC" in suffix:
            direction = " ASC"
        return f"{expr}{direction} NULLS LAST"

    updated_clause = _ORDER_BY_AMOUNT_RE.sub(repl, order_clause)
    return f"{normalized[:order_match.start('clause')]}{updated_clause}{normalized[order_match.end('clause'):]}"


def _project_predicate(project_id: str) -> str:
    return f"project_id = '{project_id}'"


def _sql_has_project_predicate(sql: str, project_id: str) -> bool:
    pattern = re.compile(
        rf"\bproject_id\b\s*=\s*'{re.escape(project_id)}'",
        re.IGNORECASE,
    )
    return bool(pattern.search(sql))


def _replace_project_placeholders(sql: str, project_id: str) -> str:
    quoted_project_id = f"'{project_id}'"
    updated = sql
    for pattern in _PROJECT_PARAM_PATTERNS:
        updated = pattern.sub(quoted_project_id, updated)
    return updated


def _normalize_project_sql(sql: str, project_id: Optional[str]) -> str:
    if not project_id:
        return sql
    normalized = _replace_project_placeholders(sql, project_id)
    if _sql_has_project_predicate(normalized, project_id):
        return normalized
    return _inject_where_predicate(normalized, _project_predicate(project_id))


def _normalize_embedding_sql(sql: str, embedding_enabled: bool) -> str:
    if embedding_enabled:
        return sql

    normalized = _SEMANTIC_ORDER_BY_RE.sub("ORDER BY operation_date DESC", sql)
    normalized = re.sub(r",\s*:query_embedding\b", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\(\s*:query_embedding\s*\)", "()", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+:query_embedding\b", " ", normalized, flags=re.IGNORECASE)
    return normalized


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
    history_id: Optional[str] = None
    ai_summary: Optional[str] = None

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
        intent_backend: Optional[LLMBackend] = None,
        save_history: bool = True,
        max_new_tokens: int = 512,
    ) -> "QueryService":
        generator = SQLGenerator(llm_backend, max_new_tokens=max_new_tokens)
        repair = SQLRepair(generator)
        retriever = SemanticRetriever(engine, embedder)
        executor = QueryExecutor(engine)
        instance = cls(
            generator=generator,
            repair=repair,
            retriever=retriever,
            executor=executor,
            embedder=embedder,
            engine=engine,
            save_history=save_history,
        )
        instance.intent_backend = intent_backend
        return instance

    # ── main entry point ──────────────────────────────────────────────────────

    async def run(self, question: str, project_id: Optional[str] = None) -> QueryResult:
        import asyncio
        t0 = time.perf_counter()
        repaired = False
        sql = ""
        rows: List[Dict[str, Any]] = []

        try:
            # 0. optional intent routing
            if hasattr(self, 'intent_backend') and self.intent_backend is not None:
                intent_prompt = (
                    f"Message: {question}\n\n"
                    "Classify this message. If it is a greeting, a thank you, or general chat, reply EXACTLY with 'CHAT'. "
                    "If it is asking for data, reports, transactions, or stats, reply EXACTLY with 'DATA'."
                )
                classification = await self.intent_backend.agenerate(intent_prompt, max_new_tokens=10)
                if "CHAT" in classification.upper() and "DATA" not in classification.upper():
                    chat_prompt = (
                        f"User said: {question}\n"
                        "Reply as a helpful data assistant in Russian. Keep it short."
                    )
                    ai_reply = await self.intent_backend.agenerate(chat_prompt, max_new_tokens=400)
                    elapsed = time.perf_counter() - t0
                    result = QueryResult(
                        question=question, sql="", rows=[], execution_time_s=elapsed,
                        ai_summary=ai_reply, history_id=str(uuid.uuid4())
                    )
                    await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
                    return result

            # 1. rule-based entity extraction
            entities = extract_entities(question)
            log.debug("Extracted entities: %s", entities)

            # 2. embed question for retrieval
            query_embedding = None
            if self.embedder.enabled:
                embed_text = entities.semantic_topic or question
                query_embedding = await asyncio.to_thread(self.embedder.embed, [embed_text])
                query_embedding = query_embedding[0]

            # 3. semantic retrieval
            context = await asyncio.to_thread(
                self.retriever.retrieve,
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
            sql = await self.generator.agenerate(prompt)
            sql = _normalize_project_sql(sql, project_id)
            sql = _normalize_embedding_sql(sql, self.embedder.enabled)
            sql = _normalize_ranked_amount_sql(sql)
            log.info("Generated SQL:\n%s", sql)

            # 6. validate
            validate_sql(sql)

            # 7. execute
            def _execute():
                return self.executor.execute(
                    sql,
                    query_embedding=query_embedding if ":query_embedding" in sql else None,
                )
            rows = await asyncio.to_thread(_execute)

        except SQLValidationError as ve:
            # attempt repair
            log.warning("Validation failed: %s — attempting repair", ve)
            sql, rows, repaired = await self._repair_and_run(sql, str(ve), query_embedding, project_id)

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            result = QueryResult(
                question=question,
                sql=sql,
                rows=[],
                execution_time_s=elapsed,
                error=str(exc),
                history_id=str(uuid.uuid4()),
            )
            await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
            return result

        ai_summary = None
        if rows:
            # Smart Data Summarization via Profiling
            if len(rows) > 0:
                try:
                    import pandas as pd
                    df = pd.DataFrame(rows)
                    if len(rows) > 15:
                        # Extract basic stats dynamically
                        stats = df.describe(include='all').to_string()
                        data_context = f"Data Statistics (Too large to show all):\n{stats}\nTop 5 rows:\n{df.head(5).to_dict(orient='records')}"
                    else:
                        data_context = str(rows)
                except Exception:
                    data_context = str(rows[:10])
            else:
                data_context = "No results."

            sum_prompt = (
                f"User asked: {question}\n"
                f"SQL generated: {sql}\n"
                f"Data sample:\n{data_context}\n\n"
                "Provide a short, direct answer in Russian based strictly on the returned data. "
                "Do not explain the SQL. If listing results, keep the wording concise and natural in Russian."
            )
            try:
                ai_summary = await self.generator.backend.agenerate(sum_prompt, max_new_tokens=400)
            except Exception as e:
                log.error("Failed to generate AI summary: %s", e)

        elapsed = time.perf_counter() - t0
        result = QueryResult(
            question=question,
            sql=sql,
            rows=rows,
            execution_time_s=elapsed,
            repaired=repaired,
            history_id=str(uuid.uuid4()),
            ai_summary=ai_summary,
        )
        await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
        return result

    async def run_stream(self, question: str, project_id: Optional[str] = None):
        """Streaming version of run(). Yields partial progress dictionary objects."""
        import asyncio
        import pandas as pd
        t0 = time.perf_counter()
        repaired = False
        sql = ""
        rows: List[Dict[str, Any]] = []
        query_embedding = None

        yield {"event": "status", "data": "Initializing..."}

        try:
            # 0. optional intent routing
            if hasattr(self, 'intent_backend') and self.intent_backend is not None:
                intent_prompt = (
                    f"Message: {question}\n\n"
                    "Classify this message. If it is a greeting, a thank you, or general chat, reply EXACTLY with 'CHAT'. "
                    "If it is asking for data, reports, transactions, or stats, reply EXACTLY with 'DATA'."
                )
                yield {"event": "status", "data": "Checking intent..."}
                classification = await self.intent_backend.agenerate(intent_prompt, max_new_tokens=10)
                if "CHAT" in classification.upper() and "DATA" not in classification.upper():
                    chat_prompt = (
                        f"User said: {question}\n"
                        "Reply as a helpful data assistant in Russian. Keep it short."
                    )
                    
                    yield {"event": "status", "data": "Generating chat response..."}
                    ai_reply = ""
                    async for chunk in self.intent_backend.astream(chat_prompt, max_new_tokens=400):
                        ai_reply += chunk
                        yield {"event": "summary_chunk", "data": chunk}
                        
                    elapsed = time.perf_counter() - t0
                    result = QueryResult(
                        question=question,
                        sql="",
                        rows=[],
                        execution_time_s=elapsed,
                        ai_summary=ai_reply,
                        history_id=str(uuid.uuid4()),
                    )
                    await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
                    yield {"event": "done", "data": {
                        "question": question, "sql": "", "rows": [], "execution_time_s": elapsed,
                        "ai_summary": ai_reply, "history_id": result.history_id
                    }}
                    return

            yield {"event": "status", "data": "Extracting entities..."}
            entities = extract_entities(question)

            if self.embedder.enabled:
                yield {"event": "status", "data": "Embedding question..."}
                embed_text = entities.semantic_topic or question
                query_embedding = await asyncio.to_thread(self.embedder.embed, [embed_text])
                query_embedding = query_embedding[0]

            yield {"event": "status", "data": "Retrieving context..."}
            context = await asyncio.to_thread(self.retriever.retrieve, question, semantic_topic=entities.semantic_topic)

            plan = QueryPlan(question=question, entities=entities, context=context, query_embedding=query_embedding)
            prompt = build_prompt(plan)

            yield {"event": "status", "data": "Generating SQL..."}
            sql = await self.generator.agenerate(prompt)
            sql = _normalize_project_sql(sql, project_id)
            sql = _normalize_embedding_sql(sql, self.embedder.enabled)
            sql = _normalize_ranked_amount_sql(sql)
            
            yield {"event": "sql", "data": sql}
            validate_sql(sql)

            yield {"event": "status", "data": "Executing query..."}
            def _execute():
                return self.executor.execute(sql, query_embedding=query_embedding if ":query_embedding" in sql else None)
            rows = await asyncio.to_thread(_execute)

        except SQLValidationError as ve:
            yield {"event": "status", "data": "Repairing SQL..."}
            sql, rows, repaired = await self._repair_and_run(sql, str(ve), query_embedding, project_id)
            yield {"event": "sql", "data": sql}

        except Exception as exc:
            yield {"event": "error", "error": str(exc)}
            elapsed = time.perf_counter() - t0
            result = QueryResult(question=question, sql=sql, rows=[], execution_time_s=elapsed, error=str(exc), history_id=str(uuid.uuid4()))
            await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
            yield {"event": "done", "data": result.__dict__}
            return

        yield {"event": "rows", "data": rows}

        ai_summary = ""
        if rows:
            yield {"event": "status", "data": "Summarizing data..."}
            if len(rows) > 0:
                try:
                    df = pd.DataFrame(rows)
                    if len(rows) > 15:
                        stats = df.describe(include='all').to_string()
                        data_context = f"Data Statistics (Too large to show all):\n{stats}\nTop 5 rows:\n{df.head(5).to_dict(orient='records')}"
                    else:
                        data_context = str(rows)
                except Exception:
                    data_context = str(rows[:10])
            else:
                data_context = "No results."

            sum_prompt = (
                f"User asked: {question}\n"
                f"SQL generated: {sql}\n"
                f"Data sample:\n{data_context}\n\n"
                "Provide a short, direct answer in Russian based strictly on the returned data. "
                "Do not explain the SQL. If listing results, keep the wording concise and natural in Russian."
            )
            try:
                async for chunk in self.generator.backend.astream(sum_prompt, max_new_tokens=400):
                    ai_summary += chunk
                    yield {"event": "summary_chunk", "data": chunk}
            except Exception as e:
                log.error("Failed to generate AI summary stream: %s", e)

        elapsed = time.perf_counter() - t0
        hist_id = str(uuid.uuid4())
        result = QueryResult(
            question=question, sql=sql, rows=rows, execution_time_s=elapsed,
            repaired=repaired, history_id=hist_id, ai_summary=ai_summary
        )
        await asyncio.to_thread(self._save_history, result, query_embedding, project_id)
        
        yield {"event": "done", "data": {
            "success": True, "question": question, "sql": sql, "rows": rows,
            "execution_time_s": elapsed, "repaired": repaired, "history_id": hist_id,
            "ai_summary": ai_summary, "error": None
        }}

    # ── repair ────────────────────────────────────────────────────────────────

    async def _repair_and_run(
        self,
        original_sql: str,
        error: str,
        query_embedding,
        project_id: Optional[str] = None,
    ):
        import asyncio
        repaired_sql = await self.repair.arepair(original_sql, error)
        repaired_sql = _normalize_project_sql(repaired_sql, project_id)
        repaired_sql = _normalize_embedding_sql(repaired_sql, self.embedder.enabled)
        repaired_sql = _normalize_ranked_amount_sql(repaired_sql)
        validate_sql(repaired_sql)
        def _execute():
            return self.executor.execute(
                repaired_sql,
                query_embedding=query_embedding if ":query_embedding" in repaired_sql else None,
            )
        rows = await asyncio.to_thread(_execute)
        return repaired_sql, rows, True

    # ── query_history persistence ─────────────────────────────────────────────

    def _save_history(self, result: QueryResult, embedding, project_id: Optional[str] = None) -> None:
        if not self.save_history:
            return
        try:
            emb_val = None
            if self.embedder.enabled and embedding is not None:
                import numpy as np
                arr = np.asarray(embedding, dtype=np.float32).reshape(-1)
                emb_val = f"[{','.join(str(x) for x in arr)}]"

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.query_history
                          (id, question, generated_sql, execution_success, embedding,
                           execution_time_ms, row_count, repaired, error_text, project_id)
                        VALUES
                          (CAST(:id AS uuid), :q, :sql, :ok, CAST(:emb AS vector),
                           :exec_ms, :rows, :repaired, :err, CAST(:project_id AS uuid))
                        """
                    ),
                    {
                        "id": result.history_id,
                        "q": result.question,
                        "sql": result.sql,
                        "ok": result.success,
                        "emb": emb_val,
                        "exec_ms": int(result.execution_time_s * 1000) if result.execution_time_s else None,
                        "rows": len(result.rows) if result.rows else 0,
                        "repaired": result.repaired,
                        "err": result.error,
                        "project_id": project_id,
                    },
                )
        except Exception as e:
            # Fallback for environments without pgvector (where column is BYTEA)
            try:
                from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
                emb_bytes = None
                if self.embedder.enabled and embedding is not None:
                    emb_bytes = EmbeddingBackend.vec_to_bytes(embedding)
                
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO afm.query_history
                              (id, question, generated_sql, execution_success, embedding,
                               execution_time_ms, row_count, repaired, error_text, project_id)
                            VALUES
                              (CAST(:id AS uuid), :q, :sql, :ok, :emb,
                               :exec_ms, :rows, :repaired, :err, CAST(:project_id AS uuid))
                            """
                        ),
                        {
                            "id": result.history_id,
                            "q": result.question,
                            "sql": result.sql,
                            "ok": result.success,
                            "emb": emb_bytes,
                            "exec_ms": int(result.execution_time_s * 1000) if result.execution_time_s else None,
                            "rows": len(result.rows) if result.rows else 0,
                            "repaired": result.repaired,
                            "err": result.error,
                            "project_id": project_id,
                        },
                    )
            except Exception as e2:
                log.exception("Failed to save query history (non-fatal): %s / %s", e, e2)
