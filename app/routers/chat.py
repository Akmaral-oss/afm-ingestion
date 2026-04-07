from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.engine import Engine

from ..config import ApiSettings
from ..db.engine import make_engine
from ..db.schema import ensure_schema
from ..ingestion.mapping.embedding_mapper import EmbeddingBackend
from ..nl2sql.query_service import QueryService
from ..nl2sql.sql_generator import build_llm_backend
from ..schemas import ChatQueryRequest, ChatQueryResponse
from ..security import decode_access_token

router = APIRouter(prefix="/chat", tags=["Chat"])


@dataclass
class NL2SQLRuntime:
    engine: Engine
    service: QueryService


def _runtime_settings() -> ApiSettings:
    return ApiSettings()


def _runtime_signature(runtime_settings: ApiSettings) -> tuple[str, str, str, str, str, bool, bool]:
    return (
        runtime_settings.sync_pg_dsn,
        runtime_settings.AFM_LLM_BASE_URL,
        runtime_settings.AFM_LLM_MODEL,
        runtime_settings.AFM_INTENT_LLM_MODEL or "",
        runtime_settings.nl2sql_embedding_model or "",
        runtime_settings.NL2SQL_SAVE_HISTORY,
        runtime_settings.NL2SQL_ADMIN_ONLY,
    )


def _require_chat_access(authorization: Optional[str], runtime_settings: ApiSettings) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Bearer token",
        )

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_access_token(token)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc

    if runtime_settings.NL2SQL_ADMIN_ONLY and payload.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required for chat access",
        )
    return payload


@lru_cache(maxsize=1)
def _get_runtime(_: tuple[str, str, str, str, str, bool, bool]) -> NL2SQLRuntime:
    runtime_settings = _runtime_settings()
    engine = make_engine(runtime_settings.sync_pg_dsn)
    ensure_schema(engine)

    embedder = EmbeddingBackend(
        runtime_settings.nl2sql_embedding_model,
        provider=runtime_settings.AFM_EMBEDDING_PROVIDER,
        ollama_base_url=runtime_settings.AFM_EMBEDDING_BASE_URL,
        ollama_timeout_s=runtime_settings.AFM_EMBEDDING_TIMEOUT_S,
    )
    llm_backend = build_llm_backend(
        runtime_settings.AFM_LLM_MODEL,
        base_url=runtime_settings.AFM_LLM_BASE_URL,
        timeout_s=runtime_settings.AFM_LLM_TIMEOUT_S,
    )
    intent_backend = None
    if runtime_settings.AFM_INTENT_LLM_MODEL:
        intent_backend = build_llm_backend(
            runtime_settings.AFM_INTENT_LLM_MODEL,
            base_url=runtime_settings.AFM_LLM_BASE_URL,
            timeout_s=runtime_settings.AFM_LLM_TIMEOUT_S,
        )

    service = QueryService.build(
        engine,
        embedder,
        llm_backend=llm_backend,
        intent_backend=intent_backend,
        save_history=runtime_settings.NL2SQL_SAVE_HISTORY,
        max_new_tokens=runtime_settings.AFM_LLM_MAX_NEW_TOKENS,
    )
    return NL2SQLRuntime(engine=engine, service=service)


def close_chat_runtime() -> None:
    # lru_cache does not expose cached values safely here; clearing the cache is
    # enough for dev reload and process shutdown.
    _get_runtime.cache_clear()


@router.post("/query", response_model=ChatQueryResponse)
async def chat_query(
    body: ChatQueryRequest,
    authorization: Optional[str] = Header(default=None),
):
    runtime_settings = _runtime_settings()
    _require_chat_access(authorization, runtime_settings)

    question = body.question.strip()
    if not question:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Question must not be empty",
        )

    runtime = _get_runtime(_runtime_signature(runtime_settings))
    result = await runtime.service.run(question)
    return ChatQueryResponse(
        success=result.success,
        question=result.question,
        sql=result.sql,
        rows=result.rows,
        execution_time_s=result.execution_time_s,
        repaired=result.repaired,
        error=result.error,
        ai_summary=result.ai_summary,
    )


from fastapi.responses import StreamingResponse
import json

@router.post("/stream")
async def chat_stream(
    body: ChatQueryRequest,
    authorization: Optional[str] = Header(default=None),
):
    runtime_settings = _runtime_settings()
    _require_chat_access(authorization, runtime_settings)

    question = body.question.strip()
    if not question:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Question must not be empty",
        )

    runtime = _get_runtime(_runtime_signature(runtime_settings))

    async def sse_generator():
        try:
            async for chunk in runtime.service.run_stream(question):
                # Standard SSE format: data: JSON\n\n
                data_str = json.dumps(chunk, default=str)
                yield f"data: {data_str}\n\n"
        except Exception as e:
            err_str = json.dumps({"event": "error", "error": str(e)})
            yield f"data: {err_str}\n\n"

    return StreamingResponse(sse_generator(), media_type="text/event-stream")
