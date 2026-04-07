"""
app/nl2sql/query_service.py
Main NL→SQL orchestrator. Runs 7-step pipeline:
  entity extraction → embed → cluster lookup → context retrieval
  → prompt build → LLM generate → validate → execute
Includes deterministic Halyk NULL-direction fix and auto-repair on errors.
"""
from __future__ import annotations
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .entity_extractor import extract_entities
from .catalog_entity_resolver import CatalogEntityResolver
from .prompt_builder import build_prompt
from .query_executor import QueryExecutor
from .query_models import QueryPlan, RetrievedContext
from .sql_generator import LLMBackend, SQLGenerator
from .sql_repair import SQLRepair
from .sql_validator import SQLValidationError, validate_sql
from app.semantic.semantic_service import SemanticService

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


# ── Halyk NULL-direction SQL rewrite ────────────────────────────────────────────

_HALYK_DEBIT_RE = re.compile(
    r"direction\s*=\s*'debit'",
    re.IGNORECASE,
)
_HALYK_DEBIT_REPLACEMENT = "(direction = 'debit' OR direction IS NULL)"


def _apply_halyk_direction_fix(sql: str, source_bank_filter: Optional[str]) -> str:
    """
    If the query is filtered to source_bank='halyk' (or no bank
    filter at all, meaning it might touch Halyk rows), rewrite bare
      direction = 'debit'
    into
      (direction = 'debit' OR direction IS NULL)

    This is deterministic and happens regardless of whether the LLM
    remembered the instruction in the prompt.
    """
    if source_bank_filter and source_bank_filter.lower() != "halyk":
        return sql  # Non-Halyk bank — no fix needed

    # Apply fix when Halyk is the source or no bank filter (mixed data)
    if _HALYK_DEBIT_RE.search(sql):
        fixed = _HALYK_DEBIT_RE.sub(_HALYK_DEBIT_REPLACEMENT, sql)
        if fixed != sql:
            log.debug("Applied Halyk NULL-direction fix to SQL")
        return fixed
    return sql


def _apply_null_amount_fix(sql: str) -> str:
    """
    Deterministic post-processing: if the SQL has GROUP BY and uses SUM/AVG/COUNT
    on amount_kzt but has no NULL filter, inject WHERE amount_kzt IS NOT NULL.
    Also injects operation_date IS NOT NULL when both are missing.
    Prevents useless null-filled result rows in aggregation queries.
    """
    upper = sql.upper()

    has_group_by   = bool(re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE))
    has_amount_agg = bool(re.search(r'\b(SUM|AVG|MIN|MAX)\s*\(\s*amount_kzt', sql, re.IGNORECASE))
    already_null_filter = bool(re.search(
        r'amount_kzt\s+IS\s+NOT\s+NULL', sql, re.IGNORECASE
    ))

    if not (has_group_by and has_amount_agg) or already_null_filter:
        return sql

    # Find the WHERE clause to append, or add one before GROUP BY
    if re.search(r'\bWHERE\b', sql, re.IGNORECASE):
        # Append to existing WHERE — add before GROUP BY
        sql = re.sub(
            r'(\bGROUP\s+BY\b)',
            'AND amount_kzt IS NOT NULL\n\1',
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        # No WHERE — inject one before GROUP BY
        sql = re.sub(
            r'(\bGROUP\s+BY\b)',
            'WHERE amount_kzt IS NOT NULL\n\1',
            sql,
            count=1,
            flags=re.IGNORECASE,
        )

    log.debug("_apply_null_amount_fix: injected amount_kzt IS NOT NULL")
    return sql


class QueryService:
    def __init__(
        self,
        generator: SQLGenerator,
        repair: SQLRepair,
        retriever: SemanticService,
        catalog_resolver: CatalogEntityResolver,
        executor: QueryExecutor,
        embedder,
        engine: Engine,
        save_history: bool = True,
    ):
        self.generator = generator
        self.repair = repair
        self.retriever = retriever
        self.catalog_resolver = catalog_resolver
        self.executor = executor
        self.embedder = embedder
        self.engine = engine
        self.save_history = save_history

    @classmethod
    def build(
        cls,
        engine: Engine,
        embedder,
        llm_backend: LLMBackend,
        save_history: bool = True,
        cluster_rebuild_every_n: int = 500,
    ) -> "QueryService":
        generator = SQLGenerator(llm_backend)
        repair = SQLRepair(generator)
        executor = QueryExecutor(engine)
        retriever = SemanticService(
            engine=engine,
            embedder=embedder,
            auto_catalog=False,
            cluster_rebuild_every_n=cluster_rebuild_every_n,
        )
        catalog_resolver = CatalogEntityResolver(
            engine=engine,
            embedder=embedder,
            top_k_clusters=3,
            min_similarity=0.30,
        )
        return cls(
            generator=generator,
            repair=repair,
            retriever=retriever,
            catalog_resolver=catalog_resolver,
            executor=executor,
            embedder=embedder,
            engine=engine,
            save_history=save_history,
        )

    def run(self, question: str) -> QueryResult:
        t0 = time.perf_counter()
        repaired = False
        sql = ""
        rows: List[Dict[str, Any]] = []
        query_embedding = None

        try:
            # 1. Rule-based entity extraction
            entities = extract_entities(question)
            log.debug("Extracted entities: %s", entities)

            # 2. Embed question
            if self.embedder.enabled:
                embed_text = entities.semantic_topic or question
                query_embedding = self.embedder.embed([embed_text])[0]

            # 2b. Catalog-driven topic resolution
            catalog_resolution = self.catalog_resolver.resolve(question)
            if catalog_resolution.cluster_labels and not entities.semantic_topic:
                entities.semantic_topic = catalog_resolution.cluster_labels[0]

            # 3. Semantic retrieval
            context = self.retriever.retrieve_context(
                question=question,
                semantic_topic=entities.semantic_topic,
            )
            if catalog_resolution.sample_texts:
                merged = list(dict.fromkeys(
                    catalog_resolution.sample_texts + context.sample_values
                ))
                context.sample_values = merged[:20]

            # 4. Build plan + prompt
            plan = QueryPlan(
                question=question,
                entities=entities,
                context=context,
                query_embedding=query_embedding,
                catalog_context=catalog_resolution.llm_context,
                embedder_enabled=self.embedder.enabled,
            )
            prompt = build_prompt(plan)

            # 5. LLM SQL generation
            sql = self.generator.generate(prompt)

            # 6. Apply Halyk NULL-direction rewrite deterministically
            bank_filter = entities.source_bank.value if entities.source_bank else None
            sql = _apply_halyk_direction_fix(sql, bank_filter)
            # 6b. Auto-inject amount_kzt IS NOT NULL for GROUP BY aggregations
            sql = _apply_null_amount_fix(sql)

            log.info("Generated SQL:\n%s", sql)

            # 7. Validate
            validate_sql(sql)

            # 8. Execute
            try:
                rows = self.executor.execute(
                    sql,
                    query_embedding=query_embedding if ":query_embedding" in sql else None,
                )
            except Exception as exec_exc:
                # Execution errors go through repair same as validation errors
                log.warning("Execution failed: %s — attempting repair", exec_exc)
                sql, rows, repaired = self._repair_and_run(
                    sql, str(exec_exc), query_embedding, bank_filter
                )

        except SQLValidationError as ve:
            log.warning("Validation failed: %s — attempting repair", ve)
            try:
                sql, rows, repaired = self._repair_and_run(
                    sql, str(ve), query_embedding,
                    entities.source_bank.value if entities.source_bank else None,
                )
            except Exception as repair_exc:
                elapsed = time.perf_counter() - t0
                result = QueryResult(
                    question=question, sql=sql, rows=[],
                    execution_time_s=elapsed, error=str(repair_exc),
                )
                self._save_history(result, query_embedding)
                return result

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            result = QueryResult(
                question=question, sql=sql, rows=[],
                execution_time_s=elapsed, error=str(exc),
            )
            self._save_history(result, query_embedding)
            return result

        elapsed = time.perf_counter() - t0
        result = QueryResult(
            question=question, sql=sql, rows=rows,
            execution_time_s=elapsed, repaired=repaired,
        )
        self._save_history(result, query_embedding)
        return result

    def _repair_and_run(
        self, original_sql: str, error: str,
        query_embedding, bank_filter: Optional[str]
    ):
        repaired_sql = self.repair.repair(original_sql, error)
        # Apply Halyk fix to repaired SQL too
        repaired_sql = _apply_halyk_direction_fix(repaired_sql, bank_filter)
        repaired_sql = _apply_null_amount_fix(repaired_sql)
        validate_sql(repaired_sql)
        rows = self.executor.execute(
            repaired_sql,
            query_embedding=query_embedding if ":query_embedding" in repaired_sql else None,
        )
        return repaired_sql, rows, True

    def _save_history(self, result: QueryResult, embedding) -> None:
        if not self.save_history:
            return
        try:
            emb_str: Optional[str] = None
            if self.embedder.enabled and embedding is not None:
                import numpy as np
                arr = np.asarray(embedding, dtype=np.float32).reshape(-1)
                emb_str = "[" + ",".join(f"{v:.6f}" for v in arr) + "]"

            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO afm.query_history
                          (id, question, generated_sql, execution_success, embedding)
                        VALUES
                          (CAST(:id AS uuid), :q, :sql, :ok, CAST(:emb AS vector))
                    """),
                    {
                        "id": str(uuid.uuid4()),
                        "q": result.question,
                        "sql": result.sql,
                        "ok": result.success,
                        "emb": emb_str,
                    },
                )
        except Exception:
            log.warning("Failed to save query history (non-fatal)")