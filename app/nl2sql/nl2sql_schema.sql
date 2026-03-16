-- ============================================================================
-- NL2SQL layer — database additions
-- Run once after the existing ensure_schema() migration.
-- ============================================================================

-- ── 0. pgvector extension ────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;


-- ── 1. Add semantic columns to transactions_core ─────────────────────────────
ALTER TABLE afm.transactions_core
    ADD COLUMN IF NOT EXISTS semantic_text      TEXT,
    ADD COLUMN IF NOT EXISTS semantic_embedding VECTOR(1024);

-- IVFFlat index for fast ANN search (build after data is loaded)
CREATE INDEX IF NOT EXISTS idx_tx_semantic_emb
    ON afm.transactions_core
    USING ivfflat (semantic_embedding vector_cosine_ops)
    WITH (lists = 100);


-- ── 2. Expanded analytical view (replaces the current narrow view) ───────────
CREATE OR REPLACE VIEW afm.transactions_nl_view AS
SELECT
    -- transaction
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

    -- description
    tc.operation_type_raw,
    tc.sdp_name,
    tc.purpose_code,
    tc.purpose_text,
    tc.raw_note,

    -- payer
    tc.payer_name,
    tc.payer_iin_bin,
    tc.payer_residency,
    tc.payer_bank,
    tc.payer_account,

    -- receiver
    tc.receiver_name,
    tc.receiver_iin_bin,
    tc.receiver_residency,
    tc.receiver_bank,
    tc.receiver_account,

    -- statement (joined from afm.statements)
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

    -- semantic
    tc.semantic_text,
    tc.semantic_embedding

FROM afm.transactions_core  tc
LEFT JOIN afm.statements     st ON st.statement_id = tc.statement_id;


-- ── 3. Semantic catalog ───────────────────────────────────────────────────────
-- Stores: column descriptions, real data sample values, NL→SQL examples.
-- Used by SemanticRetriever for RAG context.
CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
    id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type      TEXT        NOT NULL,          -- 'column' | 'value' | 'example'
    text      TEXT        NOT NULL,
    embedding VECTOR(1024),
    meta      JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sem_cat_type ON afm.semantic_catalog(type);
CREATE INDEX IF NOT EXISTS idx_sem_cat_emb
    ON afm.semantic_catalog
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);


-- ── 4. Query history ──────────────────────────────────────────────────────────
-- Stores past NL→SQL pairs for retrieval-augmented prompting (self-learning).
CREATE TABLE IF NOT EXISTS afm.query_history (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    question          TEXT        NOT NULL,
    generated_sql     TEXT,
    execution_success BOOLEAN     NOT NULL DEFAULT FALSE,
    user_feedback     SMALLINT,              -- 1 = good, -1 = bad, NULL = unknown
    embedding         VECTOR(1024),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qh_success ON afm.query_history(execution_success);
CREATE INDEX IF NOT EXISTS idx_qh_emb
    ON afm.query_history
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);


-- ── 5. Seed semantic_catalog with column descriptions ────────────────────────
-- Embeddings are computed by scripts/seed_catalog.py — this seeds the text.
INSERT INTO afm.semantic_catalog (type, text, meta) VALUES
    ('column', 'operation_date — дата операции',                         '{"column":"operation_date","role":"time"}'),
    ('column', 'amount_kzt — сумма операции в тенге',                    '{"column":"amount_kzt","role":"metric"}'),
    ('column', 'amount_credit — кредитовая сумма входящего платежа',     '{"column":"amount_credit","role":"metric"}'),
    ('column', 'amount_debit — дебетовая сумма исходящего платежа',      '{"column":"amount_debit","role":"metric"}'),
    ('column', 'direction — направление операции credit или debit',       '{"column":"direction","role":"categorical"}'),
    ('column', 'currency — валюта операции KZT USD EUR RUB',             '{"column":"currency","role":"categorical"}'),
    ('column', 'source_bank — банк источник выписки kaspi halyk forte',  '{"column":"source_bank","role":"categorical"}'),
    ('column', 'purpose_text — назначение платежа текстовое описание',   '{"column":"purpose_text","role":"semantic"}'),
    ('column', 'operation_type_raw — вид операции категория документа',  '{"column":"operation_type_raw","role":"semantic"}'),
    ('column', 'sdp_name — наименование СДП платёжной системы',          '{"column":"sdp_name","role":"semantic"}'),
    ('column', 'raw_note — примечание из выписки',                       '{"column":"raw_note","role":"semantic"}'),
    ('column', 'payer_name — наименование ФИО плательщика',              '{"column":"payer_name","role":"entity"}'),
    ('column', 'payer_iin_bin — ИИН БИН плательщика 12 цифр',           '{"column":"payer_iin_bin","role":"identifier"}'),
    ('column', 'payer_bank — банк плательщика',                          '{"column":"payer_bank","role":"entity"}'),
    ('column', 'payer_account — счёт IBAN плательщика',                  '{"column":"payer_account","role":"identifier"}'),
    ('column', 'receiver_name — наименование ФИО получателя',            '{"column":"receiver_name","role":"entity"}'),
    ('column', 'receiver_iin_bin — ИИН БИН получателя 12 цифр',        '{"column":"receiver_iin_bin","role":"identifier"}'),
    ('column', 'receiver_bank — банк получателя',                        '{"column":"receiver_bank","role":"entity"}'),
    ('column', 'receiver_account — счёт IBAN получателя',                '{"column":"receiver_account","role":"identifier"}'),
    ('column', 'client_name — клиент владелец выписки',                  '{"column":"client_name","role":"entity"}'),
    ('column', 'account_iban — IBAN счёта выписки',                      '{"column":"account_iban","role":"identifier"}'),
    ('column', 'opening_balance — входящий остаток на начало периода',   '{"column":"opening_balance","role":"metric"}'),
    ('column', 'closing_balance — исходящий остаток на конец периода',   '{"column":"closing_balance","role":"metric"}'),
    ('column', 'semantic_text — семантический текст операции для поиска','{"column":"semantic_text","role":"semantic"}')
ON CONFLICT DO NOTHING;  
