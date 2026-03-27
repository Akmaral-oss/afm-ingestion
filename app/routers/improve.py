from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from fastapi import APIRouter, Header
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.engine import Engine
from fastapi.responses import StreamingResponse
import json


from ..config import settings
from ..database import engine
from ..db.schema import ensure_schema
from ..ingestion.mapping.embedding_mapper import EmbeddingBackend
from ..nl2sql.query_service import QueryService
from ..nl2sql.sql_generator import build_llm_backend
from ..schemas import ChatQueryRequest, ChatQueryResponse
from ..security import decode_access_token
from app.exceptions import (
    EmptyQuestionException,
    MissingTokenException,
    AdminRoleRequiredException,
    InvalidTokenException
)


router = APIRouter(prefix="/chat", tags=["Chat"])


@dataclass
class NL2SQLRuntime:
    engine: Engine
    service: QueryService


def _require_chat_access(authorization: Optional[str]) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise MissingTokenException
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_access_token(token)
    except Exception as exc:
        raise InvalidTokenException from exc

    if settings.NL2SQL_ADMIN_ONLY and payload.get("role") != "admin":
        raise AdminRoleRequiredException
    return payload


@lru_cache(maxsize=1)
def _get_runtime() -> NL2SQLRuntime:
    ensure_schema(engine)

    embedder = EmbeddingBackend(
        settings.nl2sql_embedding_model,
        provider=settings.AFM_EMBEDDING_PROVIDER,
        ollama_base_url=settings.AFM_EMBEDDING_BASE_URL,
        ollama_timeout_s=settings.AFM_EMBEDDING_TIMEOUT_S,
    )
    llm_backend = build_llm_backend(
        settings.AFM_LLM_MODEL,
        base_url=settings.AFM_LLM_BASE_URL,
        timeout_s=settings.AFM_LLM_TIMEOUT_S,
    )
    service = QueryService.build(
        engine,
        embedder,
        llm_backend,
        save_history=settings.NL2SQL_SAVE_HISTORY,
        max_new_tokens=settings.AFM_LLM_MAX_NEW_TOKENS,
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
    _require_chat_access(authorization, settings)

    question = body.question.strip()
    if not question:
        raise EmptyQuestionException

    runtime = _get_runtime()
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


@router.post("/stream")
async def chat_stream(
    body: ChatQueryRequest,
    authorization: Optional[str] = Header(default=None),
):
    _require_chat_access(authorization, settings)

    question = body.question.strip()
    if not question:
        raise EmptyQuestionException

    runtime = _get_runtime()

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
