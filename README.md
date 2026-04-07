# AFM Financial Intelligence Platform v5

NL2SQL pipeline for Kaspi / Halyk bank statements — natural language → PostgreSQL.

## Project structure

```
afm_final_v5/
├── app/
│   ├── config.py              # env-driven settings (AFM_PG_DSN required)
│   ├── api.py                 # FastAPI: POST /query, GET /health
│   ├── main.py                # uvicorn entrypoint
│   │
│   ├── db/
│   │   ├── schema.py          # idempotent DDL, IVFFlat index management
│   │   └── writers.py         # bulk insert with ON CONFLICT DO UPDATE semantic fields
│   │
│   ├── nl2sql/
│   │   ├── query_service.py   # orchestrator: 7-step NL→SQL pipeline
│   │   ├── entity_extractor.py# regex: dates, amounts, bank, direction, top-N
│   │   ├── prompt_builder.py  # 6-block prompt assembly
│   │   ├── sql_generator.py   # OllamaBackend / HuggingFaceBackend
│   │   ├── sql_validator.py   # SELECT-only, no raw tables, LIMIT guard
│   │   ├── sql_repair.py      # LLM repair on validation/execution errors
│   │   ├── query_executor.py  # pgvector cast, row cap, timeout
│   │   └── catalog_entity_resolver.py  # K-means cluster cosine lookup
│   │
│   ├── semantic/
│   │   ├── cluster_builder.py         # K-means + DBI auto-k + smart labels
│   │   ├── semantic_catalog_builder.py# embed transactions → semantic_catalog
│   │   ├── semantic_service.py        # auto cluster rebuild trigger
│   │   ├── query_expander.py          # cluster → sample_texts for prompt
│   │   ├── cluster_labeler.py         # optional LLM relabelling
│   │   └── reclean.py                 # re-clean existing DB semantic_text
│   │
│   ├── ingestion/
│   │   ├── pipeline.py        # main ingestion orchestrator
│   │   ├── adapters/          # KaspiAdapter, HalykAdapter
│   │   ├── extractor/         # ExcelUniversalExtractor, BlockDetector
│   │   ├── mapping/           # CanonicalMapper, EmbeddingBackend
│   │   ├── metadata/          # StatementMetadataExtractor
│   │   ├── registry/          # FormatRegistry, DiscoveryLogger
│   │   └── validation/        # quality scoring, validators
│   │
│   └── utils/                 # date, hash, text, number, json helpers
│
├── scripts/
│   ├── ingest_cli.py          # python -m scripts.ingest_cli --pg ... --data data/
│   ├── build_semantic.py      # python -m scripts.build_semantic --pg ...
│   └── query_cli.py           # python -m scripts.query_cli --pg ... "вопрос"
│
├── tests/
│   └── test_nl2sql.py         # pytest: entity, validator, Halyk fix, config, repair
│
├── .env.example
└── requirements.txt
```

## Quick start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env — set AFM_PG_DSN at minimum

# 3. Ingest
python -m scripts.ingest_cli \
  --pg 'postgresql://user:pass@localhost:5432/afm' \
  --model models/bge-m3 \
  --data data/

# 4. Build clusters
python -m scripts.build_semantic \
  --pg 'postgresql://user:pass@localhost:5432/afm' \
  --model models/bge-m3 \
  --rebuild-from-db

# 5. Query
python -m scripts.query_cli \
  --pg 'postgresql://user:pass@localhost:5432/afm' \
  --model models/bge-m3 \
  --llm_model qwen2.5-coder:14b \
  "топ 10 получателей по сумме за 2024"

# 6. API server
AFM_PG_DSN=postgresql://... uvicorn app.main:app --port 8000
```

## Re-clean existing data (if upgrading from older version)

```bash
# Re-cleans semantic_text + rebuilds catalog + clusters
python -m app.semantic.reclean \
  --pg 'postgresql://user:pass@localhost:5432/afm' \
  --model models/bge-m3
```

## Key env variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AFM_PG_DSN` | — | **Required** |
| `AFM_EMBEDDING_MODEL_PATH` | — | Path to BGE-M3 |
| `AFM_LLM_MODEL` | `qwen2.5-coder:14b` | Ollama model |
| `AFM_CLUSTER_REBUILD_EVERY_N` | `500` | Auto-rebuild threshold |
| `AFM_API_TOKEN` | — | Bearer token (empty = no auth) |
