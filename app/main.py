"""
app/main.py
Entrypoint: loads config from env, builds QueryService, starts FastAPI.

Usage:
  AFM_PG_DSN=postgresql://... uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations
import logging

import uvicorn

from app.config import load_settings_from_env
from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.sql_generator import OllamaBackend, HuggingFaceBackend
from app.nl2sql.query_service import QueryService
from app.api import app, init_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def build_app():
    settings = load_settings_from_env()
    engine = make_engine(settings.pg_dsn)
    ensure_schema(engine)

    embedder = EmbeddingBackend(settings.embedding_model_path)

    if settings.llm_backend == "huggingface":
        llm = HuggingFaceBackend(settings.llm_model)
    else:
        llm = OllamaBackend(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
        )

    qs = QueryService.build(
        engine=engine,
        embedder=embedder,
        llm_backend=llm,
        save_history=True,
        cluster_rebuild_every_n=settings.cluster_rebuild_every_n,
    )
    init_api(qs)
    log.info(
        "AFM NL2SQL ready — embedder=%s llm=%s backend=%s",
        "ON" if embedder.enabled else "OFF",
        settings.llm_model,
        settings.llm_backend,
    )
    return settings


_settings = build_app()

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=_settings.api_host,
        port=_settings.api_port,
        reload=False,
    )
