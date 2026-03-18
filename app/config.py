from typing import Optional
from pydantic import Field, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # 1. Основные настройки БД
    pg_dsn: str = Field("", validation_alias="AFM_PG_DSN")

    # 2. Эмбеддинги
    # AliasChoices позволяет проверять несколько имен переменных (как в вашем коде)
    embedding_provider: str = Field("ollama", validation_alias="AFM_EMBEDDING_PROVIDER")
    embedding_model_path: str = Field(
        "mxbai-embed-large", 
        validation_alias=AliasChoices("AFM_EMBEDDING_MODEL", "AFM_EMBEDDING_MODEL_NAME", "AFM_OLLAMA_EMBEDDING_MODEL")
    )
    embedding_base_url: str = Field(
        "http://localhost:11434",
        validation_alias=AliasChoices("AFM_EMBEDDING_BASE_URL", "AFM_LLM_BASE_URL", "OLLAMA_BASE_URL")
    )
    embedding_timeout_s: int = Field(60, validation_alias="AFM_EMBEDDING_TIMEOUT_S")
    embedding_threshold: float = Field(0.85, validation_alias="AFM_EMBEDDING_THRESHOLD")
    
    # 3. Парсинг и логика
    format_similarity_threshold: float = Field(0.92, validation_alias="AFM_FORMAT_SIMILARITY_THRESHOLD")
    store_raw_row_json: bool = Field(False, validation_alias="AFM_STORE_RAW_ROW_JSON")
    parser_version: str = Field("v2.0-enterprise", validation_alias="AFM_PARSER_VERSION")
    max_meta_lookback_rows: int = Field(80, validation_alias="AFM_MAX_META_LOOKBACK_ROWS")

    # 4. LLM настройки
    llm_base_url: str = Field(
        "http://localhost:11434",
        validation_alias=AliasChoices("AFM_LLM_BASE_URL", "OLLAMA_BASE_URL")
    )
    llm_model_name: str = Field(
        "qwen2.5-coder:14b",
        validation_alias=AliasChoices("AFM_LLM_MODEL", "OLLAMA_MODEL")
    )
    llm_timeout_s: int = Field(120, validation_alias="AFM_LLM_TIMEOUT_S")
    llm_max_new_tokens: int = Field(512, validation_alias="AFM_LLM_MAX_NEW_TOKENS")

    # Конфигурация загрузки
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # игнорировать лишние переменные в .env
    )

# Использование
settings = Settings()
print(settings.pg_dsn)