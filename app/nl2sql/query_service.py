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
from .sql_generator import LLMBackend, SQLGenerator
from .sql_repair import SQLRepair
from .sql_validator import SQLValidationError, validate_sql
from .advanced_templates import get_template_sql, TOPIC_TEMPLATE_MAP

# Fix: import SemanticService (replaces old SemanticRetriever)
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
    quality_warnings: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.error is None


class QueryService:
    def __init__(
        self,
        generator: SQLGenerator,
        repair: SQLRepair,
        retriever: SemanticService,
        executor: QueryExecutor,
        embedder,
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

    @classmethod
    def build(
        cls,
        engine: Engine,
        embedder,
        llm_backend: LLMBackend,
        save_history: bool = True,
    ) -> "QueryService":
        generator = SQLGenerator(llm_backend)
        repair    = SQLRepair(generator)
        executor  = QueryExecutor(engine)

        # Fix: SemanticService instead of SemanticRetriever
        retriever = SemanticService(
            engine=engine,
            embedder=embedder,
            auto_catalog=False,  # catalog updated by IngestionPipeline, not query layer
        )

        return cls(
            generator=generator,
            repair=repair,
            retriever=retriever,
            executor=executor,
            embedder=embedder,
            engine=engine,
            save_history=save_history,
        )

    # ── template routing ──────────────────────────────────────────────────────

    def _try_template_route(self, entities, question: str) -> Optional[str]:
        """
        For well-known AML query types (transit, circular, obnal, etc.) bypass
        the LLM entirely and return a pre-built SQL string.

        Returns None when no template matches — caller falls through to LLM.
        """
        topic = entities.semantic_topic
        if not topic:
            return None

        sql = get_template_sql(topic, entities)
        if sql:
            log.info(
                "Template route: topic=%s → pre-built SQL (LLM skipped)", topic
            )
        return sql

    def run(self, question: str) -> QueryResult:
        t0 = time.perf_counter()
        repaired = False
        sql = ""
        rows: List[Dict[str, Any]] = []
        query_embedding = None

        try:
            # 1. rule-based entity extraction
            entities = extract_entities(question)
            log.debug("Extracted entities: %s", entities)

            # 2. embed question
            if self.embedder.enabled:
                embed_text = entities.semantic_topic or question
                query_embedding = self.embedder.embed([embed_text])[0]

            # 2b. AML template routing — bypasses LLM for known fraud patterns
            template_sql = self._try_template_route(entities, question)
            if template_sql:
                validate_sql(template_sql)
                rows = self.executor.execute(template_sql)
                elapsed = time.perf_counter() - t0
                result = QueryResult(
                    question=question,
                    sql=template_sql,
                    rows=rows,
                    execution_time_s=elapsed,
                )
                result.quality_warnings = self._check_result_quality(template_sql, rows)
                self._save_history(result, query_embedding)
                return result

            # 3. semantic retrieval via SemanticService
            # retrieve_context() returns RetrievedContext with:
            #   - sample_values: cluster-expanded real transaction texts
            #   - similar_examples: past successful NL→SQL pairs
            context = self.retriever.retrieve_context(
                question=question,
                semantic_topic=entities.semantic_topic,
            )

            log.debug(
                "Retrieved %d samples, %d history examples",
                len(context.sample_values),
                len(context.similar_examples),
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
            log.warning("Validation failed: %s — attempting repair", ve)
            try:
                sql, rows, repaired = self._repair_and_run(sql, str(ve), query_embedding)
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
        
        # Check for quality warnings
        warnings = self._check_result_quality(sql, rows)
        result.quality_warnings = warnings
        
        self._save_history(result, query_embedding)
        return result

    def _check_result_quality(self, sql: str, rows: List[Dict[str, Any]]) -> List[str]:
        """
        Check for potential quality issues in the query or results.
        Returns list of warning messages.
        """
        warnings: List[str] = []
        
        # No results with semantic filters
        if not rows and "LIKE" in sql:
            warnings.append(
                "No results found with the applied filters. "
                "Try using a broader search term or removing specific filters."
            )
        
        # Check for GROUP BY without proper aggregates
        if "GROUP BY" in sql.upper() and "SUM(" not in sql and "COUNT(" not in sql:
            warnings.append(
                "GROUP BY query without aggregation functions (SUM, COUNT, AVG). "
                "Results may have many duplicate groups."
            )
        
        # Check for NULL values in GROUP BY results (common with aggregations)
        if "GROUP BY" in sql.upper() and rows:
            null_count = sum(1 for row in rows if any(v is None for v in row.values()))
            if null_count > len(rows) * 0.2:  # More than 20% NULL values
                warnings.append(
                    f"Results contain {null_count}/{len(rows)} rows with NULL values. "
                    "Consider filtering NULL records with WHERE conditions."
                )
        
        # Check for very slow queries
        # This is checked after execution, so timing is available in the result
        
        return warnings

    def _repair_and_run(self, original_sql, error, query_embedding):
        repaired_sql = self.repair.repair(original_sql, error)
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
            # Fix: use pgvector string format, not raw bytes
            emb_str: Optional[str] = None
            if self.embedder.enabled and embedding is not None:
                import numpy as np
                arr = np.asarray(embedding, dtype=np.float32).reshape(-1)
                emb_str = "[" + ",".join(f"{v:.6f}" for v in arr) + "]"

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.query_history
                          (id, question, generated_sql, execution_success, embedding)
                        VALUES
                          (CAST(:id AS uuid), :q, :sql, :ok,
                           CAST(:emb AS vector))
                        """
                    ),
                    {
                        "id":  str(uuid.uuid4()),
                        "q":   result.question,
                        "sql": result.sql,
                        "ok":  result.success,
                        "emb": emb_str,
                    },
                )
        except Exception:
            log.warning("Failed to save query history (non-fatal)")
