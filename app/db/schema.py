from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_schema(engine: Engine) -> None:
    """
    Idempotent schema migration.

    Covers:
      • Original ingestion tables (raw_files, format_registry, statements,
        transactions_core, transactions_ext, field_discovery_log)
      • NL2SQL additions:
          - pgvector extension
          - semantic_text / semantic_embedding on transactions_core
          - afm.transactions_nl_view  (replaces the old narrow view)
          - afm.semantic_catalog
          - afm.query_history
    """
    with engine.begin() as conn:

        # ── pgvector ──────────────────────────────────────────────────────────
        # Gracefully skip if the extension is not available in this PG instance.
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            _pgvector_available = True
        except Exception:
            _pgvector_available = False

        # ── schema ────────────────────────────────────────────────────────────
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS afm;"))

        # ── raw_files ─────────────────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.raw_files (
                  file_id           UUID PRIMARY KEY,
                  source_bank       TEXT NOT NULL,
                  original_filename TEXT NOT NULL,
                  sha256            TEXT NOT NULL,
                  uploaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                  parsed_at         TIMESTAMPTZ,
                  parser_version    TEXT NOT NULL,
                  notes             TEXT
                );
                """
            )
        )

        # ── format_registry ───────────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.format_registry (
                  format_id          UUID PRIMARY KEY,
                  source_bank        TEXT,
                  header_fingerprint TEXT UNIQUE,
                  header_sample      JSONB,
                  embedding_vector   BYTEA,
                  first_seen         TIMESTAMPTZ NOT NULL DEFAULT now(),
                  last_seen          TIMESTAMPTZ NOT NULL DEFAULT now(),
                  usage_count        INT NOT NULL DEFAULT 1
                );
                """
            )
        )

        # ── statements ────────────────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.statements (
                  statement_id    UUID PRIMARY KEY,
                  file_id         UUID NOT NULL REFERENCES afm.raw_files(file_id),
                  source_bank     TEXT NOT NULL,
                  source_sheet    TEXT,
                  source_block_id INT,
                  format_id       UUID REFERENCES afm.format_registry(format_id),

                  client_name     TEXT,
                  client_iin_bin  CHAR(12),
                  account_iban    TEXT,
                  account_type    TEXT,
                  currency        TEXT,

                  statement_date  DATE,
                  period_from     DATE,
                  period_to       DATE,

                  opening_balance NUMERIC(18,2),
                  closing_balance NUMERIC(18,2),
                  total_debit     NUMERIC(18,2),
                  total_credit    NUMERIC(18,2),

                  meta_json       JSONB
                );
                """
            )
        )

        # ── transactions_core ─────────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.transactions_core (
                  tx_id              UUID PRIMARY KEY,
                  file_id            UUID NOT NULL REFERENCES afm.raw_files(file_id),
                  statement_id       UUID REFERENCES afm.statements(statement_id),
                  format_id          UUID REFERENCES afm.format_registry(format_id),

                  source_bank         TEXT NOT NULL,
                  source_sheet        TEXT,
                  source_block_id     INT,
                  source_row_no       INT,
                  row_hash            TEXT NOT NULL,

                  operation_ts        TIMESTAMPTZ,
                  operation_date      DATE,

                  currency            TEXT,
                  amount_currency     NUMERIC(18,2),
                  amount_kzt          NUMERIC(18,2),
                  amount_credit       NUMERIC(18,2),
                  amount_debit        NUMERIC(18,2),
                  direction           TEXT,

                  operation_type_raw  TEXT,
                  sdp_name            TEXT,
                  purpose_code        TEXT,
                  purpose_text        TEXT,
                  raw_note            TEXT,

                  payer_name          TEXT,
                  payer_iin_bin       CHAR(12),
                  payer_residency     TEXT,
                  payer_bank          TEXT,
                  payer_account       TEXT,

                  receiver_name       TEXT,
                  receiver_iin_bin    CHAR(12),
                  receiver_residency  TEXT,
                  receiver_bank       TEXT,
                  receiver_account    TEXT,

                  confidence_score    REAL NOT NULL DEFAULT 1.0,
                  parse_warnings      TEXT,
                  raw_row_json        JSONB
                );
                """
            )
        )

        # row_hash unique constraint
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_tx_rowhash') THEN
                    ALTER TABLE afm.transactions_core
                    ADD CONSTRAINT uq_tx_rowhash UNIQUE (row_hash);
                  END IF;
                END $$;
                """
            )
        )

        # ── semantic columns on transactions_core ─────────────────────────────
        conn.execute(
            text(
                "ALTER TABLE afm.transactions_core "
                "ADD COLUMN IF NOT EXISTS semantic_text TEXT;"
            )
        )

        if _pgvector_available:
            # vector(1024) for BGE-M3 — adjust dimension if using another model
            conn.execute(
                text(
                    "ALTER TABLE afm.transactions_core "
                    "ADD COLUMN IF NOT EXISTS semantic_embedding vector(1024);"
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_tx_semantic_emb
                    ON afm.transactions_core
                    USING ivfflat (semantic_embedding vector_cosine_ops)
                    WITH (lists = 100);
                    """
                )
            )
        else:
            # fallback: store as BYTEA if pgvector is not installed
            conn.execute(
                text(
                    "ALTER TABLE afm.transactions_core "
                    "ADD COLUMN IF NOT EXISTS semantic_embedding BYTEA;"
                )
            )

        # ── transactions_ext ──────────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.transactions_ext (
                  tx_id     UUID PRIMARY KEY REFERENCES afm.transactions_core(tx_id),
                  ext_json  JSONB NOT NULL
                );
                """
            )
        )

        # ── field_discovery_log ───────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.field_discovery_log (
                  id              BIGSERIAL PRIMARY KEY,
                  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                  file_id         UUID REFERENCES afm.raw_files(file_id),
                  source_bank     TEXT,
                  format_id       UUID,
                  raw_column_name TEXT NOT NULL,
                  normalized_name TEXT,
                  sample_values   JSONB,
                  suggested_field TEXT,
                  confidence      REAL,
                  status          TEXT NOT NULL DEFAULT 'new'
                );
                """
            )
        )

        # ── drop obsolete columns ─────────────────────────────────────────────
        conn.execute(text("ALTER TABLE afm.statements DROP COLUMN IF EXISTS contract_no;"))
        conn.execute(text("ALTER TABLE afm.transactions_core DROP COLUMN IF EXISTS payer_bank_bic;"))
        conn.execute(text("ALTER TABLE afm.transactions_core DROP COLUMN IF EXISTS receiver_bank_bic;"))

        # ── indexes ───────────────────────────────────────────────────────────
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fmt_bank ON afm.format_registry(source_bank);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stmt_file ON afm.statements(file_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stmt_account ON afm.statements(account_iban);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_core_date ON afm.transactions_core(operation_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_core_file ON afm.transactions_core(file_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_stmt ON afm.transactions_core(statement_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_format ON afm.transactions_core(format_id);"))

        # ── NL2SQL: expanded analytical view ─────────────────────────────────
        conn.execute(
            text(
                """
                CREATE OR REPLACE VIEW afm.transactions_nl_view AS
                SELECT
                  tc.tx_id,
                  tc.source_bank,
                  tc.operation_ts,
                  tc.operation_date,
                  tc.currency,
                  tc.amount_currency,
                  tc.amount_kzt,
                  tc.amount_credit,
                  tc.amount_debit,
                  tc.direction,
                  tc.operation_type_raw,
                  tc.sdp_name,
                  tc.purpose_code,
                  tc.purpose_text,
                  tc.raw_note,
                  tc.payer_name,
                  tc.payer_iin_bin,
                  tc.payer_residency,
                  tc.payer_bank,
                  tc.payer_account,
                  tc.receiver_name,
                  tc.receiver_iin_bin,
                  tc.receiver_residency,
                  tc.receiver_bank,
                  tc.receiver_account,
                  st.client_name,
                  st.client_iin_bin,
                  st.account_iban,
                  st.account_type,
                  st.statement_date,
                  st.period_from,
                  st.period_to,
                  st.opening_balance,
                  st.closing_balance,
                  st.total_debit,
                  st.total_credit,
                  tc.semantic_text,
                  tc.semantic_embedding
                FROM afm.transactions_core tc
                LEFT JOIN afm.statements   st ON st.statement_id = tc.statement_id;
                """
            )
        )

        # ── legacy narrow view (kept for backward compatibility) ──────────────
        conn.execute(
            text(
                """
                CREATE OR REPLACE VIEW afm.transactions_view AS
                SELECT
                  tx_id, source_bank, operation_ts, operation_date,
                  currency, amount_currency, amount_kzt, amount_credit, amount_debit,
                  direction, payer_name, payer_iin_bin, receiver_name, receiver_iin_bin,
                  purpose_text, sdp_name
                FROM afm.transactions_core;
                """
            )
        )

        # ── NL2SQL: semantic catalog ──────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
                  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                  type       TEXT        NOT NULL,
                  text       TEXT        NOT NULL,
                  embedding  vector(1024),
                  meta       JSONB,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
                if _pgvector_available else
                """
                CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
                  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                  type       TEXT        NOT NULL,
                  text       TEXT        NOT NULL,
                  embedding  BYTEA,
                  meta       JSONB,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_sem_cat_type ON afm.semantic_catalog(type);")
        )

        # ── NL2SQL: query history ─────────────────────────────────────────────
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS afm.query_history (
                  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                  question          TEXT        NOT NULL,
                  generated_sql     TEXT,
                  execution_success BOOLEAN     NOT NULL DEFAULT FALSE,
                  user_feedback     SMALLINT,
                  embedding         vector(1024),
                  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
                if _pgvector_available else
                """
                CREATE TABLE IF NOT EXISTS afm.query_history (
                  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
                  question          TEXT        NOT NULL,
                  generated_sql     TEXT,
                  execution_success BOOLEAN     NOT NULL DEFAULT FALSE,
                  user_feedback     SMALLINT,
                  embedding         BYTEA,
                  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_qh_success "
                "ON afm.query_history(execution_success);"
            )
        )
