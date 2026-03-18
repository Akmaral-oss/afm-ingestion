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


def _vec_to_pg(vec: Any) -> str:
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{value:.6f}" for value in arr) + "]"


class QueryExecutor:
    def __init__(self, engine: Engine, max_rows: int = _MAX_ROWS):
        self.engine = engine
        self.max_rows = max_rows

    def execute(
        self,
        sql: str,
        query_embedding: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        effective_sql = _inject_limit(sql, self.max_rows)

        if ":query_embedding" in effective_sql:
            if query_embedding is None:
                raise ValueError("SQL uses :query_embedding but no embedding was provided")
            params["query_embedding"] = _vec_to_pg(query_embedding)

        t0 = time.perf_counter()
        rows: List[Dict[str, Any]] = []
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"SET statement_timeout = {_TIMEOUT_MS}"))
                result = conn.execute(text(effective_sql), params)
                rows = [dict(item) for item in result.mappings().all()]
        except Exception:
            log.exception("Query execution failed")
            raise
        finally:
            elapsed = time.perf_counter() - t0
            log.info("Query executed in %.3fs — %d rows", elapsed, len(rows))

        return rows


def _inject_limit(sql: str, cap: int) -> str:
    import re

    if re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
        return sql
    return sql.rstrip().rstrip(";") + f"\nLIMIT {cap};"
