# AFM v6 — Full Run Guide
# PostgreSQL + Ingestion + Classification + Semantic + NL2SQL

===========================================================================
## PREREQUISITES
===========================================================================

```
Python  3.10+
PostgreSQL 14+  with pgvector extension
Ollama          running locally (for NL2SQL)
```

Install Python deps (run from project root):
```bash
pip install -r requirements.txt
```

Install sentence-transformers for BGE-M3 (heavy, ~2 GB):
```bash
pip install sentence-transformers
```

===========================================================================
## STEP 0 — PostgreSQL setup (one-time)
===========================================================================

### 0.1  Create DB user and database

```sql
-- run as postgres superuser
CREATE USER afm_user WITH PASSWORD '123!';
CREATE DATABASE afm_db OWNER afm_user;
\c afm_db
CREATE EXTENSION IF NOT EXISTS vector;     -- pgvector
GRANT ALL PRIVILEGES ON DATABASE afm_db TO afm_user;
GRANT ALL ON SCHEMA public TO afm_user;
```

### 0.2  If running PostgreSQL in Docker

```bash
docker run -d \
  --name afm_pg \
  -e POSTGRES_USER=afm_user \
  -e POSTGRES_PASSWORD='123!' \
  -e POSTGRES_DB=afm_db \
  -p 5433:5432 \
  ankane/pgvector:latest
```

Test connection:
```bash
psql 'postgresql://afm_user:123!@localhost:5433/afm_db' -c "SELECT version();"
```

### 0.3  DB cascade — drop and recreate (if you need a clean slate)

```bash
psql 'postgresql://afm_user:123!@localhost:5433/afm_db' << 'SQL'
DROP SCHEMA IF EXISTS afm CASCADE;
CREATE SCHEMA afm;
SQL
```

Then run any python command below — `ensure_schema()` auto-creates everything.

===========================================================================
## STEP 1 — Schema creation (automatic on first run)
===========================================================================

`ensure_schema()` is called automatically by every CLI command.
You can also trigger it manually:

```bash
python3 - << 'PY'
import os
os.environ["AFM_PG_DSN"] = "postgresql://afm_user:123!@localhost:5433/afm_db"
from app.db.engine import make_engine
from app.db.schema import ensure_schema
ensure_schema(make_engine(os.environ["AFM_PG_DSN"]))
print("Schema OK")
PY
```

Tables created:
  afm.raw_files
  afm.format_registry
  afm.statements
  afm.transactions_core
  afm.transactions_ext
  afm.field_discovery_log
  afm.category_dictionary        ← seeded with 19 categories
  afm.transaction_classification
  afm.category_review_log
  afm.semantic_catalog
  afm.semantic_clusters
  afm.query_history
  VIEW: afm.transactions_nl_view  ← includes transaction_category + signed_amount_kzt

===========================================================================
## STEP 2 — Ingest bank statements
===========================================================================

### 2.1  Ingest entire data folder (recommended)

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m scripts.ingest_cli \
  --pg  'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --data data/
```

This processes all files in:
  data/kaspi/*.xlsx
  data/halyk/*.xls  data/halyk/*.xlsx

Each file goes through:
  1. Bank adapter (Kaspi / Halyk) or universal extractor
  2. Canonical field mapping
  3. Semantic text build + BGE-M3 embedding
  4. Dedup insert (ON CONFLICT row_hash DO UPDATE)
  5. Semantic catalog update
  6. Category classification (rules → embedding fallback → OTHER)

Output per file (logged):
```
Ingested 1.xlsx -> {
  core_rows: 342,
  statements: 1,
  categories_assigned: 342,
  semantic_embedded: True
}
```

### 2.2  Ingest a single file

```bash
python -m scripts.ingest_cli \
  --pg  'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --bank kaspi \
  data/kaspi/1.xlsx
```

### 2.3  Ingest without embeddings (faster, classification still runs)

```bash
python -m scripts.ingest_cli \
  --pg  'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --data data/
```

(omit --model → EmbeddingBackend disabled → rule-based classification only)

### 2.4  Verify ingestion

```bash
psql 'postgresql://afm_user:123!@localhost:5433/afm_db' << 'SQL'
SELECT source_bank, COUNT(*) AS tx, COUNT(cl.tx_id) AS classified
FROM afm.transactions_core tc
LEFT JOIN afm.transaction_classification cl ON cl.tx_id = tc.tx_id
GROUP BY source_bank;

-- category breakdown
SELECT cd.category_name_ru, COUNT(*) AS n
FROM afm.transaction_classification cl
JOIN afm.category_dictionary cd ON cd.category_code = cl.transaction_category
GROUP BY cd.category_name_ru
ORDER BY n DESC;
SQL
```

===========================================================================
## STEP 3 — Build semantic index (your command)
===========================================================================

### 3.1  Standard rebuild (what you had before)

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m scripts.build_semantic \
  --pg    'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --rebuild-from-db
```

This does:
  1. Re-embeds any transactions not yet in semantic_catalog
  2. Runs K-means + DBI auto-k → saves semantic_clusters
  3. Rebuilds IVFFlat indexes

### 3.2  Full v6 pipeline (recommended after fresh ingest)

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m scripts.build_semantic \
  --pg    'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --rebuild-from-db \
  --backfill-categories \
  --rebuild-category-clusters
```

What each flag does:
  --rebuild-from-db          → embed all unprocessed transactions into semantic_catalog
  --backfill-categories      → classify rows not yet in transaction_classification
                               (useful if you ran old pipeline without CategoryService)
  --rebuild-category-clusters → replace K-means clusters with 19 fixed
                               business-category centroids (one cluster per category)

### 3.3  Backfill categories only (no semantic rebuild)

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m scripts.build_semantic \
  --pg    'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --backfill-categories
```

### 3.4  All flags reference

```
--pg                       PostgreSQL DSN
--model                    BGE-M3 path or HuggingFace model ID
--bank kaspi|halyk         Filter by bank (default: all)
--k-min 8                  Minimum clusters for K-means
--k-max 0                  0 = Hartigan auto-select
--rebuild-from-db          Re-embed transactions into semantic_catalog
--backfill-categories      Classify existing rows missing from transaction_classification
--rebuild-category-clusters  Replace K-means with 19 fixed category centroids
--label-with-llm           Use Ollama to generate cluster labels (optional)
```

===========================================================================
## STEP 4 — Re-clean semantic_text (if upgrading from v5.x)
===========================================================================

If you have old data with noisy semantic_text (IBANs, IINs in text):

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m app.semantic.reclean \
  --pg    'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3
```

Then run build_semantic with --rebuild-category-clusters.

===========================================================================
## STEP 5 — Start NL2SQL API server
===========================================================================

### 5.1  Ensure Ollama is running with the model

```bash
ollama serve &
ollama pull qwen2.5-coder:14b
```

### 5.2  Start the API

```bash
AFM_PG_DSN='postgresql://afm_user:123!@localhost:5433/afm_db' \
AFM_EMBEDDING_MODEL_PATH='BAAI/bge-m3' \
AFM_LLM_MODEL='qwen2.5-coder:14b' \
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Or with .env file:
```bash
cp .env.example .env
# edit .env with your values
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Endpoints:
  POST /query       → NL2SQL query
  GET  /health      → system status
  GET  /docs        → Swagger UI

### 5.3  Test the API

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "покажи все транзакции по категории Зарплата"}'

curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "баланс по категориям за 2024"}'
```

===========================================================================
## STEP 6 — CLI query (without API server)
===========================================================================

```bash
PYTHONWARNINGS="ignore::RuntimeWarning" \
python -m scripts.query_cli \
  --pg    'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --llm_provider ollama \
  --llm_model    qwen2.5-coder:14b \
  "покажи все транзакции по категории Зарплата"
```

With XiYanSQL:
```bash
python -m scripts.query_cli \
  --pg 'postgresql://afm_user:123!@localhost:5433/afm_db' \
  --model BAAI/bge-m3 \
  --llm_provider xiyan \
  "топ 10 клиентов по сумме входящих"
```

===========================================================================
## FULL RUN SEQUENCE (clean start)
===========================================================================

```bash
PG='postgresql://afm_user:123!@localhost:5433/afm_db'
MODEL='BAAI/bge-m3'
W='PYTHONWARNINGS=ignore::RuntimeWarning'

# 0. Drop and recreate schema (if needed)
psql "$PG" -c "DROP SCHEMA IF EXISTS afm CASCADE;"

# 1. Ingest all files (creates schema + classifies + embeds)
eval $W python -m scripts.ingest_cli \
  --pg "$PG" --model "$MODEL" --data data/

# 2. Build semantic index + backfill + category clusters
eval $W python -m scripts.build_semantic \
  --pg "$PG" --model "$MODEL" \
  --rebuild-from-db \
  --backfill-categories \
  --rebuild-category-clusters

# 3. Verify
psql "$PG" << 'SQL'
SELECT
  cd.category_name_ru,
  COUNT(*)                          AS tx_count,
  cl.category_source,
  ROUND(AVG(cl.category_confidence)::numeric, 3) AS avg_conf
FROM afm.transaction_classification cl
JOIN afm.category_dictionary cd ON cd.category_code = cl.transaction_category
GROUP BY cd.category_name_ru, cl.category_source
ORDER BY tx_count DESC;
SQL

# 4. Start API
AFM_PG_DSN="$PG" \
AFM_EMBEDDING_MODEL_PATH="$MODEL" \
uvicorn app.main:app --port 8000
```

===========================================================================
## TROUBLESHOOTING
===========================================================================

### "relation afm.transaction_classification does not exist"
  → ensure_schema() wasn't called. Run ingest_cli once or call ensure_schema manually.

### "IVFFlat index creation failed: lists must be > 0"
  → No rows yet. Ingest data first, then run build_semantic.

### "pgvector not available"
  → Install: CREATE EXTENSION vector;  (requires pgvector on the server)
  → Docker: use ankane/pgvector image

### Categories all coming back as OTHER
  → Check purpose_text is populated: SELECT COUNT(*) FROM afm.transactions_core WHERE purpose_text IS NOT NULL;
  → Rule engine matches on purpose_text. If empty, all fall through to OTHER.
  → Run with --model to enable embedding fallback stage.

### "HTTPConnectionPool: Read timed out" during query
  → Ollama is slow. Increase timeout in sql_generator.py OllamaBackend: timeout=180
  → Or switch to faster model: --llm_model qwen2.5-coder:7b

### Duplicate rows on re-ingest
  → Safe — dedup is ON CONFLICT (row_hash) DO UPDATE. No duplicates.

### Re-classify existing data with new rules
  # Drop old classifications and rerun
  psql "$PG" -c "TRUNCATE afm.transaction_classification;"
  eval $W python -m scripts.build_semantic \
    --pg "$PG" --model "$MODEL" --backfill-categories

===========================================================================
## KEY ENVIRONMENT VARIABLES
===========================================================================

  AFM_PG_DSN                    required
  AFM_EMBEDDING_MODEL_PATH       BAAI/bge-m3  or local path
  AFM_LLM_MODEL                  qwen2.5-coder:14b
  AFM_LLM_BASE_URL               http://localhost:11434
  AFM_CLUSTER_REBUILD_EVERY_N    500   (auto-rebuild after N new catalog rows)
  AFM_API_TOKEN                  (empty = no auth)
  AFM_STORE_RAW_ROW_JSON         false
