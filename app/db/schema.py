from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine


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
          contract_no     TEXT,
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
          payer_bank_bic      TEXT,
          payer_account       TEXT,

          receiver_name       TEXT,
          receiver_iin_bin    CHAR(12),
          receiver_residency  TEXT,
          receiver_bank       TEXT,
          receiver_bank_bic   TEXT,
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

        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_fmt_bank ON afm.format_registry(source_bank);"
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_stmt_file ON afm.statements(file_id);")
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_stmt_account ON afm.statements(account_iban);"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_tx_core_date ON afm.transactions_core(operation_date);"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_tx_core_file ON afm.transactions_core(file_id);"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_tx_stmt ON afm.transactions_core(statement_id);"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_tx_format ON afm.transactions_core(format_id);"
            )
        )

        conn.execute(
            text(
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
        """
            )
        )
