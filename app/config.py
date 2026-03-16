from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    embedding_model_path: Optional[str] = None  # e.g. "models/bge-m3"
    embedding_threshold: float = 0.85  # header->canonical mapping
    format_similarity_threshold: float = 0.92  # format_registry reuse
    store_raw_row_json: bool = False
    parser_version: str = "v2.0-enterprise"
    max_meta_lookback_rows: int = 80


def load_settings_from_env() -> Settings:
    # optional: implement env parsing later (pydantic/envvars)
    raise NotImplementedError("Use CLI args in scripts/ingest_cli.py for now.")
