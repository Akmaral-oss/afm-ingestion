from __future__ import annotations
import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


log = logging.getLogger(__name__)


def _is_ignorable_ddl_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "must be owner of" in message
        or "permission denied" in message
        or "insufficient privilege" in message
    )


def _execute_optional(conn, sql: str, *, label: str) -> None:
    try:
        with conn.begin_nested():
            conn.execute(text(sql))
    except SQLAlchemyError as exc:
        if _is_ignorable_ddl_error(exc):
            log.warning("Skipping optional schema step '%s': %s", label, exc)
            return
        raise


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS afm;"))

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

        _execute_optional(
            conn,
            "ALTER TABLE afm.statements DROP COLUMN IF EXISTS contract_no;",
            label="drop contract_no",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core DROP COLUMN IF EXISTS payer_bank_bic;",
            label="drop payer_bank_bic",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core DROP COLUMN IF EXISTS receiver_bank_bic;",
            label="drop receiver_bank_bic",
        )

        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_fmt_bank ON afm.format_registry(source_bank);",
            label="index idx_fmt_bank",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_stmt_file ON afm.statements(file_id);",
            label="index idx_stmt_file",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_stmt_account ON afm.statements(account_iban);",
            label="index idx_stmt_account",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_tx_core_date ON afm.transactions_core(operation_date);",
            label="index idx_tx_core_date",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_tx_core_file ON afm.transactions_core(file_id);",
            label="index idx_tx_core_file",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_tx_stmt ON afm.transactions_core(statement_id);",
            label="index idx_tx_stmt",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_tx_format ON afm.transactions_core(format_id);",
            label="index idx_tx_format",
        )

        _execute_optional(
            conn,
            """
        CREATE OR REPLACE VIEW afm.transactions_view AS
        SELECT
          tx_id,
          source_bank,
          operation_ts,
          operation_date,
          currency,
          amount_currency,
          amount_kzt,
          amount_credit,
          amount_debit,
          direction,
          payer_name,
          payer_iin_bin,
          receiver_name,
          receiver_iin_bin,
          purpose_text,
          sdp_name
        FROM afm.transactions_core;
        """,
            label="refresh transactions_view",
        )

        _execute_optional(
            conn,
            "CREATE EXTENSION IF NOT EXISTS vector;",
            label="extension vector",
        )
        vector_available = bool(
            conn.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            ).scalar()
        )

        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS semantic_text TEXT;",
            label="add semantic_text",
        )
        _execute_optional(
            conn,
            (
                "ALTER TABLE afm.transactions_core "
                "ADD COLUMN IF NOT EXISTS semantic_embedding vector(1024);"
            )
            if vector_available
            else (
                "ALTER TABLE afm.transactions_core "
                "ADD COLUMN IF NOT EXISTS semantic_embedding BYTEA;"
            ),
            label="add semantic_embedding",
        )
        if vector_available:
            _execute_optional(
                conn,
                """
            CREATE INDEX IF NOT EXISTS idx_tx_semantic_emb
            ON afm.transactions_core
            USING ivfflat (semantic_embedding vector_cosine_ops)
            WITH (lists = 100);
            """,
                label="index idx_tx_semantic_emb",
            )

        _execute_optional(
            conn,
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
          COALESCE(
            NULLIF(tc.semantic_text, ''),
            CONCAT_WS(
              ' | ',
              NULLIF(tc.source_bank, ''),
              NULLIF(tc.direction, ''),
              NULLIF(tc.operation_type_raw, ''),
              NULLIF(tc.sdp_name, ''),
              NULLIF(tc.purpose_text, ''),
              NULLIF(tc.raw_note, ''),
              NULLIF(tc.payer_name, ''),
              NULLIF(tc.receiver_name, '')
            )
          ) AS semantic_text,
          tc.semantic_embedding
        FROM afm.transactions_core tc
        LEFT JOIN afm.statements st ON st.statement_id = tc.statement_id;
        """,
            label="refresh transactions_nl_view",
        )

        _execute_optional(
            conn,
            (
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
                if vector_available
                else
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
            ),
            label="create semantic_catalog",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_sem_cat_type ON afm.semantic_catalog(type);",
            label="index idx_sem_cat_type",
        )

        _execute_optional(
            conn,
            (
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
                if vector_available
                else
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
            ),
            label="create query_history",
        )
        _execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_qh_success ON afm.query_history(execution_success);",
            label="index idx_qh_success",
        )
