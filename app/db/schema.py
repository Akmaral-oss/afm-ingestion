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
        or 'extension "vector" is not available' in message
        or 'could not open extension control file' in message
        or 'расширение "vector" отсутствует' in message
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
    """
    Maintains the database schema. Tables are primarily created via Base.metadata.create_all
    in main.py, but this function handles complex views, extensions, and incremental
    updates that are hard to express in standard SQLAlchemy.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS afm;"))
        _execute_optional(
            conn,
            "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
            label="extension pgcrypto",
        )
        _execute_optional(
            conn,
            "CREATE EXTENSION IF NOT EXISTS vector;",
            label="extension vector",
        )

        # 1. Incremental Column Updates (for legacy DB support)
        # ---------------------------------------------------
        _execute_optional(
            conn,
            "ALTER TABLE afm.users ADD COLUMN IF NOT EXISTS active_project_id UUID;",
            label="add users.active_project_id",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS project_id UUID REFERENCES afm.projects(project_id);",
            label="add transactions_core.project_id",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS semantic_text TEXT;",
            label="add semantic_text",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS semantic_embedding vector(1024);",
            label="add semantic_embedding",
        )

        # 1b. Fix Missing Server Defaults
        # -------------------------------
        _execute_optional(
            conn,
            "ALTER TABLE afm.format_registry ALTER COLUMN usage_count SET DEFAULT 1;",
            label="fix format_registry.usage_count default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.format_registry ALTER COLUMN first_seen SET DEFAULT now();",
            label="fix format_registry.first_seen default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.format_registry ALTER COLUMN last_seen SET DEFAULT now();",
            label="fix format_registry.last_seen default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ALTER COLUMN confidence_score SET DEFAULT 1.0;",
            label="fix transactions_core.confidence_score default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ALTER COLUMN transaction_category SET DEFAULT 'Прочее';",
            label="fix transactions_core.transaction_category default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ALTER COLUMN category_source SET DEFAULT 'other';",
            label="fix transactions_core.category_source default",
        )
        _execute_optional(
            conn,
            "ALTER TABLE afm.transactions_core ALTER COLUMN needs_review SET DEFAULT false;",
            label="fix transactions_core.needs_review default",
        )

        # 2. Views
        # --------
        _execute_optional(
            conn,
            "DROP VIEW IF EXISTS afm.transactions_view;",
            label="drop transactions_view",
        )
        _execute_optional(
            conn,
            """
        CREATE VIEW afm.transactions_view AS
        SELECT
          tx_id,
          project_id,
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
          sdp_name,
          transaction_category,
          category_confidence,
          category_source,
          category_rule_id,
          needs_review
        FROM afm.transactions_core;
        """,
            label="refresh transactions_view",
        )

        _execute_optional(
            conn,
            "DROP VIEW IF EXISTS afm.transactions_nl_view;",
            label="drop transactions_nl_view",
        )
        _execute_optional(
            conn,
            """
        CREATE VIEW afm.transactions_nl_view AS
        SELECT
          tc.tx_id,
          tc.project_id,
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
          tc.transaction_category,
          tc.category_confidence,
          tc.category_source,
          tc.category_rule_id,
          tc.needs_review,
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

        # 3. Seeding / Backfills
        # ---------------------
        _execute_optional(
            conn,
            """
        INSERT INTO afm.projects (project_id, owner_user_id, name, created_at, updated_at)
        SELECT gen_random_uuid(), u.id, 'Main Project', now(), now()
        FROM afm.users u
        WHERE NOT EXISTS (
          SELECT 1 FROM afm.projects p WHERE p.owner_user_id = u.id
        );
        """,
            label="seed default projects",
        )
        _execute_optional(
            conn,
            """
        UPDATE afm.users u
        SET active_project_id = (
          SELECT project_id FROM afm.projects p WHERE p.owner_user_id = u.id LIMIT 1
        )
        WHERE u.active_project_id IS NULL;
        """,
            label="backfill users.active_project_id",
        )
        _execute_optional(
            conn,
            """
        INSERT INTO afm.transactions_core (
          tx_id, file_id, statement_id, format_id, project_id,
          source_bank, source_sheet, source_block_id, source_row_no, row_hash,
          operation_ts, operation_date, currency, amount_currency, amount_kzt,
          amount_credit, amount_debit, direction, operation_type_raw, sdp_name,
          purpose_code, purpose_text, raw_note, payer_name, payer_iin_bin,
          payer_residency, payer_bank, payer_account, receiver_name, receiver_iin_bin,
          receiver_residency, receiver_bank, receiver_account, confidence_score, parse_warnings,
          raw_row_json, transaction_category, category_confidence, category_source, category_rule_id,
          needs_review, semantic_text
        )
        SELECT
          er.id,
          er.file_id,
          NULL,
          NULL,
          er.project_id,
          'esf',
          er.source_sheet,
          NULL,
          er.source_row_no,
          'esf-shadow:' || er.row_hash,
          COALESCE(er.turnover_date, er.issue_date),
          CAST(COALESCE(er.turnover_date, er.issue_date) AS date),
          COALESCE(NULLIF(er.currency_code, ''), 'KZT'),
          er.total_amount,
          er.total_amount,
          CASE WHEN er.esf_direction = 'purchase' THEN 0 ELSE er.total_amount END,
          CASE WHEN er.esf_direction = 'purchase' THEN er.total_amount ELSE 0 END,
          CASE WHEN er.esf_direction = 'purchase' THEN 'debit' ELSE 'credit' END,
          CONCAT('ЭСФ ', COALESCE(er.esf_status, '')),
          'ЭСФ',
          NULL,
          CONCAT_WS(' | ', NULLIF(er.tru_name, ''), NULLIF(er.registration_number, ''), NULLIF(er.contract_number, '')),
          er.contract_number,
          er.supplier_name,
          er.supplier_iin_bin,
          er.supplier_address,
          NULL,
          '',
          er.buyer_name,
          er.buyer_iin_bin,
          er.buyer_address,
          NULL,
          '',
          1.0,
          NULL,
          er.raw_row_json,
          CASE WHEN er.esf_direction = 'purchase' THEN 'Приобретение' ELSE 'Реализация' END,
          NULL,
          'esf',
          NULL,
          false,
          CONCAT_WS(' | ', 'esf', COALESCE(er.esf_status, ''), COALESCE(er.registration_number, ''), COALESCE(er.tru_name, ''), COALESCE(er.supplier_name, ''), COALESCE(er.buyer_name, ''), COALESCE(er.contract_number, ''))
        FROM afm.esf_records er
        WHERE NOT EXISTS (
          SELECT 1 FROM afm.transactions_core tc WHERE tc.tx_id = er.id
        );
        """,
            label="backfill esf shadow transactions",
        )
