"""
app/config.py
Env-driven settings. Only AFM_PG_DSN is required — all others have safe defaults.
"""
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional


def _e(k: str, d: str = "") -> str:
    return os.environ.get(k, d).strip()


def _ei(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (ValueError, TypeError):
        return d


def _ef(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except (ValueError, TypeError):
        return d


def _eb(k: str, d: bool) -> bool:
    return os.environ.get(k, str(d)).lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    pg_dsn: str

    # Embedding
    embedding_model_path: Optional[str] = None
    embedding_threshold: float = 0.85
    format_similarity_threshold: float = 0.92

    # Ingestion
    store_raw_row_json: bool = False
    parser_version: str = "v2.1-fixed"
    max_meta_lookback_rows: int = 80

    # auto-rebuild clusters after N new catalog rows
    cluster_rebuild_every_n: int = 500  # rebuild clusters after N new catalog rows
    cluster_k_min: int = 8
    cluster_k_max: int = 0              # 0 = Hartigan auto-select

    # LLM
    llm_backend: str = "ollama"         # "ollama" | "huggingface"
    llm_model: str = "qwen2.5-coder:14b"
    llm_base_url: str = "http://localhost:11434"

    # API server
    api_host: str = "0.0.0.0"
    api_port: int = 8000


def load_settings_from_env() -> Settings:
    """
    Build Settings from environment variables.
    Raises ValueError only if AFM_PG_DSN is missing.
    """
    pg_dsn = _e("AFM_PG_DSN")
    if not pg_dsn:
        raise ValueError(
            "AFM_PG_DSN environment variable is required.\n"
            "Example: AFM_PG_DSN=postgresql://user:pass@localhost:5432/afm"
        )
    k_max = _ei("AFM_CLUSTER_K_MAX", 0)
    return Settings(
        pg_dsn=pg_dsn,
        embedding_model_path=_e("AFM_EMBEDDING_MODEL_PATH") or None,
        embedding_threshold=_ef("AFM_EMBEDDING_THRESHOLD", 0.85),
        format_similarity_threshold=_ef("AFM_FORMAT_SIMILARITY_THRESH", 0.92),
        store_raw_row_json=_eb("AFM_STORE_RAW_ROW_JSON", False),
        parser_version=_e("AFM_PARSER_VERSION", "v2.1-fixed"),
        max_meta_lookback_rows=_ei("AFM_MAX_META_LOOKBACK_ROWS", 80),
        cluster_rebuild_every_n=_ei("AFM_CLUSTER_REBUILD_EVERY_N", 500),
        cluster_k_min=_ei("AFM_CLUSTER_K_MIN", 8),
        cluster_k_max=k_max if k_max > 0 else 0,
        llm_backend=_e("AFM_LLM_BACKEND", "ollama"),
        llm_model=_e("AFM_LLM_MODEL", "qwen2.5-coder:14b"),
        llm_base_url=_e("AFM_LLM_BASE_URL", "http://localhost:11434"),
        api_host=_e("AFM_API_HOST", "0.0.0.0"),
        api_port=_ei("AFM_API_PORT", 8000),
    )
