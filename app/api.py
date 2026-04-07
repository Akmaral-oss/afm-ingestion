"""
app/api.py
FastAPI HTTP layer: POST /query and GET /health.
Bearer token auth via AFM_API_TOKEN env var (disabled if empty).
"""
from __future__ import annotations
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

log = logging.getLogger(__name__)

app = FastAPI(
    title="AFM NL2SQL API",
    description="Natural language to SQL query interface for AFM financial intelligence platform",
    version="2.1.0",
)

_bearer = HTTPBearer(auto_error=False)
_API_TOKEN = os.environ.get("AFM_API_TOKEN", "")  # empty = no auth


def _check_token(creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer)):
    if not _API_TOKEN:
        return  # auth disabled
    if not creds or creds.credentials != _API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Bearer token",
        )


# ── Request / Response models ─────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    save_history: bool = True


class QueryResponse(BaseModel):
    question: str
    sql: str
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_s: float
    repaired: bool
    success: bool
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    embedder: str
    clusters: int
    history_queries: int


# ── Dependency: QueryService singleton ───────────────────────────────────────

_query_service = None


def get_query_service():
    global _query_service
    if _query_service is None:
        raise HTTPException(
            status_code=503,
            detail="QueryService not initialised. Call init_api() first.",
        )
    return _query_service


def init_api(query_service) -> None:
    """Call this from main.py / startup scripts with a built QueryService."""
    global _query_service
    _query_service = query_service
    log.info("API QueryService initialised")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/query", response_model=QueryResponse, dependencies=[Depends(_check_token)])
def run_query(req: QueryRequest, svc=Depends(get_query_service)):
    """Convert a natural language question to SQL and execute it."""
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question must not be empty")

    result = svc.run(req.question)
    return QueryResponse(
        question=result.question,
        sql=result.sql,
        rows=result.rows,
        row_count=len(result.rows),
        execution_time_s=round(result.execution_time_s, 3),
        repaired=result.repaired,
        success=result.success,
        error=result.error,
    )


@app.get("/health", response_model=HealthResponse)
def health(svc=Depends(get_query_service)):
    """Return system health: embedder status, cluster count, history size."""
    from sqlalchemy import text
    engine = svc.engine
    clusters = 0
    history = 0
    try:
        with engine.connect() as conn:
            clusters = conn.execute(
                text("SELECT COUNT(*) FROM afm.semantic_clusters")
            ).scalar() or 0
            history = conn.execute(
                text("SELECT COUNT(*) FROM afm.query_history WHERE execution_success = TRUE")
            ).scalar() or 0
    except Exception as exc:
        log.warning("Health check DB query failed: %s", exc)

    return HealthResponse(
        status="ok",
        embedder="enabled" if svc.embedder.enabled else "disabled",
        clusters=int(clusters),
        history_queries=int(history),
    )


@app.get("/")
def root():
    return {"service": "AFM NL2SQL", "version": "2.1.0", "docs": "/docs"}
