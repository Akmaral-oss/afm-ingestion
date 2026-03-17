from __future__ import annotations
import os
from dataclasses import dataclass
from importlib import import_module
from typing import Optional

def _load_dotenv(*args, **kwargs):
    try:
        return import_module("dotenv").load_dotenv(*args, **kwargs)
    except Exception:  # pragma: no cover
        return False


_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "no", "n", "off"}


def _env_text(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _env_optional_text(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return float(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {value}")


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = ""
    embedding_model_path: Optional[str] = "mxbai-embed-large"  # Ollama model or local path
    embedding_provider: str = "ollama"  # ollama | sentence-transformers | disabled
    embedding_base_url: str = "http://localhost:11434"
    embedding_timeout_s: int = 60
    embedding_threshold: float = 0.85  # header->canonical mapping
    format_similarity_threshold: float = 0.92  # format_registry reuse
    store_raw_row_json: bool = False
    parser_version: str = "v2.0-enterprise"
    max_meta_lookback_rows: int = 80
    llm_base_url: str = "http://localhost:11434"
    llm_model_name: str = "qwen2.5-coder:14b"
    llm_timeout_s: int = 120
    llm_max_new_tokens: int = 512


def load_settings_from_env(env_file: Optional[str] = None) -> Settings:
    if env_file:
        _load_dotenv(dotenv_path=env_file, override=False)
    else:
        _load_dotenv(override=False)

    embedding_provider = _env_text("AFM_EMBEDDING_PROVIDER", "ollama").strip().lower()
    embedding_model_path = (
        _env_optional_text("AFM_EMBEDDING_MODEL")
        or _env_optional_text("AFM_EMBEDDING_MODEL_NAME")
    )
    if embedding_model_path is None and embedding_provider in ("ollama", "auto"):
        embedding_model_path = _env_text("AFM_OLLAMA_EMBEDDING_MODEL", "mxbai-embed-large")

    return Settings(
        pg_dsn=_env_text("AFM_PG_DSN", ""),
        embedding_model_path=embedding_model_path,
        embedding_provider=embedding_provider,
        embedding_base_url=(
            _env_text("AFM_EMBEDDING_BASE_URL", "")
            or _env_text("AFM_LLM_BASE_URL", "")
            or _env_text("OLLAMA_BASE_URL", "http://localhost:11434")
        ),
        embedding_timeout_s=_env_int("AFM_EMBEDDING_TIMEOUT_S", 60),
        embedding_threshold=_env_float("AFM_EMBEDDING_THRESHOLD", 0.85),
        format_similarity_threshold=_env_float("AFM_FORMAT_SIMILARITY_THRESHOLD", 0.92),
        store_raw_row_json=_env_bool("AFM_STORE_RAW_ROW_JSON", False),
        parser_version=_env_text("AFM_PARSER_VERSION", "v2.0-enterprise"),
        max_meta_lookback_rows=_env_int("AFM_MAX_META_LOOKBACK_ROWS", 80),
        llm_base_url=(
            _env_text("AFM_LLM_BASE_URL", "")
            or _env_text("OLLAMA_BASE_URL", "http://localhost:11434")
        ),
        llm_model_name=(
            _env_text("AFM_LLM_MODEL", "")
            or _env_text("OLLAMA_MODEL", "qwen2.5-coder:14b")
        ),
        llm_timeout_s=_env_int("AFM_LLM_TIMEOUT_S", 120),
        llm_max_new_tokens=_env_int("AFM_LLM_MAX_NEW_TOKENS", 512),
    )
