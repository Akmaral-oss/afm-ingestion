# AFM Ingestion + NL2SQL

Backend API for transaction ingestion, analytics, admin, Streamlit UI, and NL2SQL chat.

## Requirements

- Python 3.11+
- PowerShell on Windows
- Configured local `.env`

## Environment Setup

1. Install dependencies:
   `pip install -r requirements.txt`
2. Edit `.env` and set at least:
   - `AFM_PG_DSN`
   - `AFM_EMBEDDING_PROVIDER`
   - `AFM_EMBEDDING_MODEL`
   - `AFM_LLM_MODEL`
   - `AFM_LLM_BASE_URL`

You can use `.env.example` as reference.

## Ollama Models

NL2SQL uses Ollama by default.

Recommended defaults:

`AFM_EMBEDDING_PROVIDER=ollama`

`AFM_EMBEDDING_MODEL=mxbai-embed-large`

`AFM_LLM_MODEL=qwen2.5-coder:14b`

Gemini test mode:

- Set `AFM_LLM_MODEL=gemini` or `AFM_LLM_MODEL=gemini:gemini-3-flash-preview`
- Export `GEMINI_API_KEY` in your shell
- Optional test script:
  `python scripts/gemini_llm_test.py "INSERT_INPUT_HERE"`

If you want local sentence-transformers embeddings instead:

`AFM_EMBEDDING_PROVIDER=sentence-transformers`

`AFM_EMBEDDING_MODEL=models/bge-m3`

Timeout note:

- `AFM_EMBEDDING_TIMEOUT_S=0` means no timeout limit
- `AFM_LLM_TIMEOUT_S=0` means no timeout limit

## Run Backend

Current frontend is configured to call the backend on `http://127.0.0.1:8003`.

```powershell
cd D:\afm-ingestion\afm-ingestion
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
```

If you do not want to activate the virtual environment:

```powershell
cd D:\afm-ingestion\afm-ingestion
.\venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
```

## Health Check

```powershell
Invoke-WebRequest http://127.0.0.1:8003/health
```

Expected response:

```json
{"status":"ok","service":"afm-ingestion-api"}
```

## CLI Usage

All scripts accept `.env` values automatically. CLI flags still override env values.

- Ingestion:
  `python scripts/ingest_cli.py --data data`
- Query:
  `python scripts/query_cli.py "топ 10 платежей"`
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
- NL2SQL query box with retry and correction attempts
- reasoning panel
- query history table


## Docker
Run Prometheus and Grafana:
```
docker compose build -up
```
+ postgresql
+ pgadmin


## Notes

- FastAPI app entrypoint: `app.main:app`
- Admin panel: `http://127.0.0.1:8003/admin`
- API docs: `http://127.0.0.1:8003/docs`


