from __future__ import annotations

"""
Query Executor.

Executes validated SQL against PostgreSQL.
- Converts :query_embedding to pgvector format automatically
- Enforces a hard row cap
- Logs execution time
"""

import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_MAX_ROWS = 1_000     # hard cap — no query returns more than this
_TIMEOUT_MS = 30_000  # 30 s statement timeout


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

        Parameters
        ----------
        sql:
            Validated SELECT statement.  May contain :query_embedding
            placeholder for vector search.
        query_embedding:
            Numpy array (BGE-M3 output).  Required when sql contains
            the :query_embedding placeholder.
        """
        params: Dict[str, Any] = {}

        # inject vector if needed
        if ":query_embedding" in sql:
            if query_embedding is None:
                raise ValueError("SQL uses :query_embedding but no embedding was provided")
            params["query_embedding"] = _vec_to_pg(query_embedding)

        # enforce row cap
        if f"LIMIT {self.max_rows}" not in sql.upper():
            sql = _inject_limit(sql, self.max_rows)

        t0 = time.perf_counter()
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"SET statement_timeout = {_TIMEOUT_MS}"))
                result = conn.execute(text(sql), params)
                cols = list(result.keys())
                rows = [dict(zip(cols, row)) for row in result.fetchall()]
        except Exception as exc:
            log.error("Query execution failed: %s", exc)
            raise
        finally:
            elapsed = time.perf_counter() - t0
            log.info("Query executed in %.3fs — %d rows", elapsed, len(rows) if 'rows' in dir() else 0)

        return rows


# ── helpers ───────────────────────────────────────────────────────────────────

def _inject_limit(sql: str, cap: int) -> str:
    """Append LIMIT if missing (last-resort safety net)."""
    import re
    if re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
        return sql
    return sql.rstrip().rstrip(";") + f"\nLIMIT {cap};"
