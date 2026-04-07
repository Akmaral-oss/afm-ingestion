"""
app/nl2sql/query_executor.py
Executes validated SQL against PostgreSQL.
Raises on DB-level errors so QueryService can route to SQLRepair.
"""
from __future__ import annotations
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_MAX_ROWS = 1_000
_TIMEOUT_MS = 30_000


def _vec_to_pg(vec) -> str:
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{v:.6f}" for v in arr) + "]"


class QueryExecutor:
    def __init__(self, engine: Engine, max_rows: int = _MAX_ROWS):
        self.engine = engine
        self.max_rows = max_rows

    def execute(
        self,
        sql: str,
        query_embedding: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute validated SQL.
        Raises on DB-level errors so QueryService can route to SQLRepair.
        """
        params: Dict[str, Any] = {}
        if ":query_embedding" in sql:
            if query_embedding is None:
                raise ValueError("SQL uses :query_embedding but no embedding was provided")
            params["query_embedding"] = _vec_to_pg(query_embedding)

        sql = _ensure_limit(sql, self.max_rows)

        t0 = time.perf_counter()
        rows: List[Dict[str, Any]] = []
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"SET statement_timeout = {_TIMEOUT_MS}"))
                result = conn.execute(text(sql), params)
                cols = list(result.keys())
                rows = [dict(zip(cols, row)) for row in result.fetchall()]
        except Exception as exc:
            # Re-raise so QueryService.run() catches and routes to repair
            elapsed = time.perf_counter() - t0
            log.error("Query execution failed after %.3fs: %s", elapsed, exc)
            raise
        finally:
            elapsed = time.perf_counter() - t0
            log.info("Query executed in %.3fs — %d rows", elapsed, len(rows))

        return rows


def _ensure_limit(sql: str, cap: int) -> str:
    import re
    if re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
        return sql
    return sql.rstrip().rstrip(";") + f"\nLIMIT {cap};"
