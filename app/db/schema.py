"""
app/db/schema.py  — v4.0
Category columns встроены прямо в transactions_core:
  transaction_category, category_confidence, category_source,
  category_rule_id, needs_review

Убраны:  afm.transaction_classification  (отдельная таблица)
Оставлены: afm.category_dictionary, afm.category_review_log
"""
from __future__ import annotations
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def _ivfflat_lists(conn, table: str, col: str, fallback: int = 1) -> int:
    try:
        n = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL")
        ).scalar() or 0
        return max(1, min(100, int(n ** 0.5)))
    except Exception:
        return fallback


def ensure_schema(engine: Engine) -> None:
    """Create / migrate all tables, indexes, views.  Safe to re-run."""
    with engine.begin() as conn:

        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            _pgvec = True
        except Exception:
            log.warning("pgvector not available — embeddings stored as BYTEA")
            _pgvec = False

        _vtype = "vector(1024)" if _pgvec else "BYTEA"
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS afm;"))

        # ── raw_files ─────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.raw_files (
              file_id UUID PRIMARY KEY, source_bank TEXT NOT NULL,
              original_filename TEXT NOT NULL, sha256 TEXT NOT NULL,
              uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(), parsed_at TIMESTAMPTZ,
              parser_version TEXT NOT NULL, notes TEXT
            );"""))

        # ── format_registry ───────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.format_registry (
              format_id UUID PRIMARY KEY, source_bank TEXT,
              header_fingerprint TEXT UNIQUE, header_sample JSONB,
              embedding_vector BYTEA,
              first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
              usage_count INT NOT NULL DEFAULT 1
            );"""))

        # ── statements ────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.statements (
              statement_id UUID PRIMARY KEY,
              file_id UUID NOT NULL REFERENCES afm.raw_files(file_id),
              source_bank TEXT NOT NULL, source_sheet TEXT, source_block_id INT,
              format_id UUID REFERENCES afm.format_registry(format_id),
              client_name TEXT, client_iin_bin CHAR(12), account_iban TEXT,
              account_type TEXT, currency TEXT, statement_date DATE,
              period_from DATE, period_to DATE,
              opening_balance NUMERIC(18,2), closing_balance NUMERIC(18,2),
              total_debit NUMERIC(18,2), total_credit NUMERIC(18,2), meta_json JSONB
            );"""))

        # ── transactions_core  (category columns встроены) ────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.transactions_core (
              tx_id              UUID        PRIMARY KEY,
              file_id            UUID        NOT NULL REFERENCES afm.raw_files(file_id),
              statement_id       UUID        REFERENCES afm.statements(statement_id),
              format_id          UUID        REFERENCES afm.format_registry(format_id),
              source_bank        TEXT        NOT NULL,
              source_sheet       TEXT,
              source_block_id    INT,
              source_row_no      INT,
              row_hash           TEXT        NOT NULL,
              operation_ts       TIMESTAMPTZ,
              operation_date     DATE,
              currency           TEXT,
              amount_currency    NUMERIC(18,2),
              amount_kzt         NUMERIC(18,2),
              amount_credit      NUMERIC(18,2),
              amount_debit       NUMERIC(18,2),
              direction          TEXT,
              operation_type_raw TEXT,
              sdp_name           TEXT,
              purpose_code       TEXT,
              purpose_text       TEXT,
              raw_note           TEXT,
              payer_name         TEXT,
              payer_iin_bin      CHAR(12),
              payer_residency    TEXT,
              payer_bank         TEXT,
              payer_account      TEXT,
              receiver_name      TEXT,
              receiver_iin_bin   CHAR(12),
              receiver_residency TEXT,
              receiver_bank      TEXT,
              receiver_account   TEXT,
              confidence_score   REAL        NOT NULL DEFAULT 1.0,
              parse_warnings     TEXT,
              raw_row_json       JSONB,
              semantic_text      TEXT,
              -- ── classification (inline) ──────────────────────────────────
              transaction_category   TEXT    DEFAULT 'ПРОЧЕЕ',
              category_confidence    NUMERIC(5,4),
              category_source        TEXT    DEFAULT 'other',
              category_rule_id       TEXT,
              needs_review           BOOLEAN NOT NULL DEFAULT FALSE
            );"""))

        # Migrate existing tables: add category columns if missing
        for col_def in [
            "transaction_category TEXT DEFAULT 'ПРОЧЕЕ'",
            "category_confidence NUMERIC(5,4)",
            "category_source TEXT DEFAULT 'other'",
            "category_rule_id TEXT",
            "needs_review BOOLEAN NOT NULL DEFAULT FALSE",
        ]:
            col_name = col_def.split()[0]
            conn.execute(text(f"""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='afm' AND table_name='transactions_core'
                      AND column_name='{col_name}'
                  ) THEN
                    ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS {col_def};
                  END IF;
                END $$;"""))

        conn.execute(text("""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='uq_tx_rowhash')
              THEN ALTER TABLE afm.transactions_core ADD CONSTRAINT uq_tx_rowhash UNIQUE(row_hash);
              END IF; END $$;"""))

        if _pgvec:
            conn.execute(text("ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS semantic_embedding vector(1024);"))
        else:
            conn.execute(text("ALTER TABLE afm.transactions_core ADD COLUMN IF NOT EXISTS semantic_embedding BYTEA;"))

        # ── transactions_ext ──────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.transactions_ext (
              tx_id UUID PRIMARY KEY REFERENCES afm.transactions_core(tx_id),
              ext_json JSONB NOT NULL
            );"""))

        # ── field_discovery_log ───────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.field_discovery_log (
              id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              file_id UUID REFERENCES afm.raw_files(file_id),
              source_bank TEXT, format_id UUID, raw_column_name TEXT NOT NULL,
              normalized_name TEXT, sample_values JSONB, suggested_field TEXT,
              confidence REAL, status TEXT NOT NULL DEFAULT 'new'
            );"""))

        # ── category_dictionary (справочник 19 категорий) ─────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.category_dictionary (
              category_code    TEXT    PRIMARY KEY,
              category_name_ru TEXT    NOT NULL,
              category_name_en TEXT,
              category_group   TEXT,
              is_active        BOOLEAN NOT NULL DEFAULT TRUE,
              display_order    INTEGER
            );"""))

        conn.execute(text("""
            INSERT INTO afm.category_dictionary
              (category_code, category_name_ru, category_name_en, category_group, display_order)
            VALUES
              ('P2P_ПЕРЕВОД',       'P2P перевод',                  'P2P Transfer',        'Переводы',      1),
              ('ПОКУПКА_В_МАГАЗИНЕ',     'Покупка в магазине',            'Store Purchase',      'Платежи',       2),
              ('ВНУТРЕННЯЯ_ОПЕРАЦИЯ', 'Внутренние операции',           'Internal Operation',  'Переводы',      3),
              ('СНЯТИЕ_НАЛИЧНЫХ',    'Снятие наличных',               'Cash Withdrawal',     'Наличные',      4),
              ('ПОГАШЕНИЕ_КРЕДИТА',     'Погашение кредита',             'Loan Repayment',      'Кредиты',       5),
              ('ГЕМБЛИНГ',           'Онлайн-игры / Гемблинг',        'Gambling',            'Риск',          6),
              ('ОБЯЗАТЕЛЬНЫЙ_ПЛАТЕЖ',  'Обязательные платежи',          'Mandatory Payment',   'Обязательные',  7),
              ('ГОСВЫПЛАТА',      'Госвыплата',                    'State Payment',       'Поступления',   8),
              ('ЗАРПЛАТА',             'Зарплата',                      'Salary',              'Поступления',   9),
              ('ПОПОЛНЕНИЕ_СЧЕТА',      'Пополнение счёта',              'Account Top-up',      'Пополнения',   10),
              ('РАСЧЕТ_ПО_ДОГОВОРУ','Расчёты по договору',           'Contract Settlement', 'Расчёты',      11),
              ('ОПЛАТА_СЧЕТ_ФАКТУРЫ',    'Оплата по счёт-фактуре',        'Invoice Payment',     'Расчёты',      12),
              ('ПЛАТЕЖ_НА_КАРТУ',       'Платёж на карту',               'Card Payment',        'Переводы',     13),
              ('ВАЛЮТНАЯ_ОПЕРАЦИЯ',       'Валютная операция',             'FX Operation',        'Валюта',       14),
              ('ВЫДАЧА_ЗАЙМА',      'Выдача займа',                  'Loan Issuance',       'Кредиты',      15),
              ('АЛИМЕНТЫ',            'Алименты',                      'Alimony',             'Обязательные', 16),
              ('ЦЕННЫЕ_БУМАГИ',         'Операции с ценными бумагами',   'Securities',          'Инвестиции',   17),
              ('ВОЗВРАТ_СРЕДСТВ',             'Возврат средств',               'Refund',              'Корректировки',18),
              ('ПРОЧЕЕ',              'Прочее',                        'Other',               'Прочее',       19)
            ON CONFLICT (category_code) DO NOTHING;"""))

        # ── category_review_log (аудит ручных исправлений) ────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS afm.category_review_log (
              review_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
              tx_id             UUID        NOT NULL REFERENCES afm.transactions_core(tx_id),
              old_category_code TEXT,
              new_category_code TEXT        REFERENCES afm.category_dictionary(category_code),
              old_confidence    NUMERIC(5,4),
              correction_reason TEXT,
              corrected_by      TEXT        NOT NULL,
              corrected_ts      TIMESTAMPTZ NOT NULL DEFAULT now()
            );"""))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rev_tx ON afm.category_review_log(tx_id);"))

        # ── semantic tables ───────────────────────────────────────────────────
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(), type TEXT NOT NULL DEFAULT 'tx',
              text TEXT, tx_id UUID, source_bank TEXT, semantic_text TEXT,
              source_columns JSONB, embedding {_vtype}, meta JSONB,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );"""))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sem_cat_type ON afm.semantic_catalog(type);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sem_cat_bank ON afm.semantic_catalog(source_bank);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sem_cat_txid ON afm.semantic_catalog(tx_id);"))
        conn.execute(text("""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='uq_sem_cat_txid')
              THEN ALTER TABLE afm.semantic_catalog ADD CONSTRAINT uq_sem_cat_txid UNIQUE(tx_id);
              END IF; END $$;"""))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS afm.semantic_clusters (
              cluster_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              source_bank TEXT, cluster_label TEXT, cluster_keywords JSONB,
              centroid_embedding {_vtype}, sample_texts JSONB,
              tx_count INT NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );"""))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sem_cl_bank ON afm.semantic_clusters(source_bank);"))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS afm.query_history (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              question TEXT NOT NULL, generated_sql TEXT,
              execution_success BOOLEAN NOT NULL DEFAULT FALSE,
              user_feedback SMALLINT, embedding {_vtype},
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );"""))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_qh_success ON afm.query_history(execution_success);"))

        # ── indexes ───────────────────────────────────────────────────────────
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fmt_bank   ON afm.format_registry(source_bank);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stmt_file  ON afm.statements(file_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stmt_acct  ON afm.statements(account_iban);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_date    ON afm.transactions_core(operation_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_file    ON afm.transactions_core(file_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_stmt    ON afm.transactions_core(statement_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_bank    ON afm.transactions_core(source_bank);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_cat     ON afm.transactions_core(transaction_category);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_review  ON afm.transactions_core(needs_review) WHERE needs_review = TRUE;"))

        if _pgvec:
            _build_ivfflat_indexes(conn)

        # ── NL2SQL view ───────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE OR REPLACE VIEW afm.transactions_nl_view AS
            SELECT
              tc.tx_id, tc.source_bank, tc.operation_ts, tc.operation_date,
              tc.currency, tc.amount_currency, tc.amount_kzt,
              tc.amount_credit, tc.amount_debit, tc.direction,
              tc.operation_type_raw, tc.sdp_name, tc.purpose_code,
              tc.purpose_text, tc.raw_note,
              tc.payer_name, tc.payer_iin_bin, tc.payer_residency,
              tc.payer_bank, tc.payer_account,
              tc.receiver_name, tc.receiver_iin_bin, tc.receiver_residency,
              tc.receiver_bank, tc.receiver_account,
              st.client_name, st.client_iin_bin, st.account_iban,
              st.account_type, st.statement_date, st.period_from, st.period_to,
              st.opening_balance, st.closing_balance,
              st.total_debit, st.total_credit,
              tc.semantic_text, tc.semantic_embedding,
              -- classification (прямо из transactions_core)
              tc.transaction_category,
              cd.category_name_ru        AS transaction_category_ru,
              cd.category_group,
              tc.category_confidence,
              tc.category_source,
              tc.needs_review,
              -- знаковая сумма для балансовых расчётов
              CASE
                WHEN tc.direction = 'credit' THEN  tc.amount_kzt
                WHEN tc.direction = 'debit'  THEN -tc.amount_kzt
                ELSE NULL
              END AS signed_amount_kzt
            FROM afm.transactions_core tc
            LEFT JOIN afm.statements st ON st.statement_id = tc.statement_id
            LEFT JOIN afm.category_dictionary cd
                   ON cd.category_code = tc.transaction_category;
        """))


def _build_ivfflat_indexes(conn) -> None:
    indexes = [
        ("idx_tx_semantic_emb", "afm.transactions_core", "semantic_embedding", "vector_cosine_ops"),
        ("idx_sem_cat_emb",     "afm.semantic_catalog",  "embedding",          "vector_cosine_ops"),
        ("idx_sem_cl_emb",      "afm.semantic_clusters", "centroid_embedding", "vector_cosine_ops"),
        ("idx_qh_emb",          "afm.query_history",     "embedding",          "vector_cosine_ops"),
    ]
    for idx_name, table, col, ops in indexes:
        exists = conn.execute(
            text("SELECT 1 FROM pg_indexes WHERE indexname = :n"), {"n": idx_name}
        ).fetchone()
        if exists:
            continue
        lists = _ivfflat_lists(conn, table, col)
        if lists < 1:
            continue
        try:
            conn.execute(text(
                f"CREATE INDEX {idx_name} ON {table} "
                f"USING ivfflat ({col} {ops}) WITH (lists = {lists});"
            ))
            log.info("Created IVFFlat index %s (lists=%d)", idx_name, lists)
        except Exception as exc:
            log.warning("Could not create IVFFlat index %s: %s", idx_name, exc)


def rebuild_ivfflat_indexes(engine: Engine) -> None:
    with engine.begin() as conn:
        for idx in ("idx_tx_semantic_emb", "idx_sem_cat_emb",
                    "idx_sem_cl_emb", "idx_qh_emb"):
            try:
                conn.execute(text(f"DROP INDEX IF EXISTS {idx};"))
            except Exception:
                pass
        _build_ivfflat_indexes(conn)
    log.info("IVFFlat indexes rebuilt")
