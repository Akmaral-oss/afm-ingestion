from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    embedding_model_path: Optional[str] = None
    embedding_threshold: float = 0.85
    format_similarity_threshold: float = 0.92
    store_raw_row_json: bool = False
    parser_version: str = "v2.0-enterprise"
    max_meta_lookback_rows: int = 80


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DATABASE_URL: str = "postgresql+asyncpg://afm_user:123!@136.113.11.117:5432/afmdb?ssl=prefer"
    PG_DSN: str = ""

    APP_TITLE: str = "AFM Ingestion API"
    APP_VERSION: str = "1.0.0"

    ENABLE_SEED: bool = True
    ADMIN_EMAIL: str = "myadmin@local"
    ADMIN_PASSWORD: str = "123123"

    PARSER_API_URL: str = "http://127.0.0.1:8000"
    KASPI_PARSER_API_URL: str = ""
    BANK_PARSER_API_URL: str = ""
    HALYK_PARSER_API_URL: str = ""

    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_EMAIL: str = ""
    SMTP_USE_TLS: bool = True
    SMTP_USE_SSL: bool = False
    EMAIL_CODE_TTL_MINUTES: int = 10

    EMBEDDING_MODEL_PATH: str = ""
    EMBEDDING_THRESHOLD: float = 0.85
    FORMAT_SIMILARITY_THRESHOLD: float = 0.92
    STORE_RAW_ROW_JSON: bool = False
    PARSER_VERSION: str = "v2.0-enterprise"
    MAX_META_LOOKBACK_ROWS: int = 80

    AFM_EMBEDDING_PROVIDER: str = "disabled"
    AFM_EMBEDDING_MODEL: str = ""
    AFM_EMBEDDING_BASE_URL: str = "http://localhost:11434"
    AFM_EMBEDDING_TIMEOUT_S: int = 60
    AFM_LLM_BASE_URL: str = "http://localhost:11434"
    AFM_LLM_MODEL: str = "qwen2.5-coder:14b"
    AFM_LLM_TIMEOUT_S: int = 120
    AFM_LLM_MAX_NEW_TOKENS: int = 512
    NL2SQL_SAVE_HISTORY: bool = True
    NL2SQL_ADMIN_ONLY: bool = True

    SESSION_SECRET: str = "CHANGE_ME_SESSION_SECRET"

    # This deployment uses a Postgres instance with a very small connection
    # budget, so keep the API pool conservative while still allowing a small
    # amount of concurrent read traffic.
    DB_POOL_SIZE: int = 2
    DB_MAX_OVERFLOW: int = 0
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800
    DB_POOL_PRE_PING: bool = True

    @staticmethod
    def _to_sync_pg_dsn(dsn: str) -> str:
        if not dsn.startswith("postgresql+asyncpg://"):
            return dsn

        parts = urlsplit(dsn)
        query_items = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            if key == "ssl":
                key = "sslmode"
            query_items.append((key, value))

        return urlunsplit((
            "postgresql+psycopg2",
            parts.netloc,
            parts.path,
            urlencode(query_items),
            parts.fragment,
        ))

    @property
    def sync_pg_dsn(self) -> str:
        if self.PG_DSN:
            return self.PG_DSN
        return self._to_sync_pg_dsn(self.DATABASE_URL)

    @property
    def ingestion_settings(self) -> Settings:
        return Settings(
            pg_dsn=self.sync_pg_dsn,
            embedding_model_path=self.EMBEDDING_MODEL_PATH or None,
            embedding_threshold=self.EMBEDDING_THRESHOLD,
            format_similarity_threshold=self.FORMAT_SIMILARITY_THRESHOLD,
            store_raw_row_json=self.STORE_RAW_ROW_JSON,
            parser_version=self.PARSER_VERSION,
            max_meta_lookback_rows=self.MAX_META_LOOKBACK_ROWS,
        )

    @property
    def nl2sql_embedding_model(self) -> Optional[str]:
        if self.AFM_EMBEDDING_MODEL:
            return self.AFM_EMBEDDING_MODEL
        if self.EMBEDDING_MODEL_PATH:
            return self.EMBEDDING_MODEL_PATH
        return None


settings = ApiSettings()


def load_settings_from_env() -> Settings:
    return settings.ingestion_settings
