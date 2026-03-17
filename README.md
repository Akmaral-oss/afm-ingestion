# AFM Ingestion + NL2SQL

This project now supports centralized runtime configuration through a local `.env` file.

## Environment Setup

1. Install dependencies:
	 `pip install -r requirements.txt`
2. Edit `.env` and set at least:
	 - `AFM_PG_DSN`
	 - `AFM_EMBEDDING_PROVIDER`
	 - `AFM_EMBEDDING_MODEL`
	 - `AFM_LLM_MODEL` (Ollama model)
	 - `AFM_LLM_BASE_URL` (usually `http://localhost:11434`)

You can use `.env.example` as reference.

## Ollama Models

NL2SQL uses Ollama by default.

Embeddings now also default to Ollama. Recommended defaults:

`AFM_EMBEDDING_PROVIDER=ollama`

`AFM_EMBEDDING_MODEL=mxbai-embed-large`

Set the model in `.env`, for example:

`AFM_LLM_MODEL=qwen2.5-coder:14b`

If you want the previous local embedding behavior:

`AFM_EMBEDDING_PROVIDER=sentence-transformers`

`AFM_EMBEDDING_MODEL=models/bge-m3`

Timeout note:

- `AFM_EMBEDDING_TIMEOUT_S=0` means no timeout limit.
- `AFM_LLM_TIMEOUT_S=0` means no timeout limit.

## CLI Usage

All scripts accept `.env` values automatically. CLI flags still override env values.

- Ingestion:
	`python scripts/ingest_cli.py --data data`
- Query:
	`python scripts/query_cli.py "платежи по займам больше 5 млн за 2024"`
- Catalog seeding:
	`python scripts/seed_catalog.py`

Optional custom env file path:

`python scripts/query_cli.py --env-file .env "топ 10 платежей"`

## Streamlit App

Run the UI:

`streamlit run scripts/streamlit_app.py`

The app provides:

- runtime initialization from `.env`
- schema inspection for `afm.transactions_nl_view`
- NL2SQL query box with retry/correction attempts
- reasoning panel (understand → plan → SQL)
- query history table
