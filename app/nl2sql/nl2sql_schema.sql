-- ============================================================================
-- NL2SQL layer — database additions
-- All statements are idempotent. Run once after ensure_schema().
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS vector;

-- 1. Semantic columns on transactions_core
ALTER TABLE afm.transactions_core
    ADD COLUMN IF NOT EXISTS semantic_text      TEXT,
    ADD COLUMN IF NOT EXISTS semantic_embedding VECTOR(1024);

CREATE INDEX IF NOT EXISTS idx_tx_semantic_emb
    ON afm.transactions_core
    USING ivfflat (semantic_embedding vector_cosine_ops)
    WITH (lists = 100);


-- 2. Expanded analytical view
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
    st.opening_balance, st.closing_balance, st.total_debit, st.total_credit,
    tc.semantic_text, tc.semantic_embedding
FROM afm.transactions_core tc
LEFT JOIN afm.statements st ON st.statement_id = tc.statement_id;


-- 3. Semantic catalog (unified — serves both seed_catalog and SemanticCatalogBuilder)
CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type           TEXT        NOT NULL DEFAULT 'tx',
    text           TEXT,
    tx_id          UUID,
    source_bank    TEXT,
    semantic_text  TEXT,
    source_columns JSONB,
    embedding      VECTOR(1024),
    meta           JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sem_cat_type ON afm.semantic_catalog(type);
CREATE INDEX IF NOT EXISTS idx_sem_cat_bank ON afm.semantic_catalog(source_bank);
CREATE INDEX IF NOT EXISTS idx_sem_cat_emb
    ON afm.semantic_catalog
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);


-- 4. Semantic clusters (built by ClusterBuilder, used by QueryExpander)
CREATE TABLE IF NOT EXISTS afm.semantic_clusters (
    cluster_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_bank        TEXT,
    cluster_label      TEXT,
    cluster_keywords   JSONB,
    centroid_embedding VECTOR(1024),
    sample_texts       JSONB,
    tx_count           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sem_cl_bank ON afm.semantic_clusters(source_bank);
CREATE INDEX IF NOT EXISTS idx_sem_cl_emb
    ON afm.semantic_clusters
    USING ivfflat (centroid_embedding vector_cosine_ops)
    WITH (lists = 20);


-- 5. Query history (self-learning NL→SQL retrieval)
CREATE TABLE IF NOT EXISTS afm.query_history (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    question          TEXT        NOT NULL,
    generated_sql     TEXT,
    execution_success BOOLEAN     NOT NULL DEFAULT FALSE,
    user_feedback     SMALLINT,
    embedding         VECTOR(1024),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qh_success ON afm.query_history(execution_success);
CREATE INDEX IF NOT EXISTS idx_qh_emb
    ON afm.query_history
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);


-- 6. Seed column descriptions (embeddings computed by seed_catalog.py)
INSERT INTO afm.semantic_catalog (type, text, meta) VALUES
    ('column', 'operation_date — дата операции',                          '{"column":"operation_date","role":"time"}'),
    ('column', 'amount_kzt — сумма операции в тенге',                     '{"column":"amount_kzt","role":"metric"}'),
    ('column', 'amount_credit — кредитовая сумма входящего платежа',      '{"column":"amount_credit","role":"metric"}'),
    ('column', 'amount_debit — дебетовая сумма исходящего платежа',       '{"column":"amount_debit","role":"metric"}'),
    ('column', 'direction — направление операции credit или debit',        '{"column":"direction","role":"categorical"}'),
    ('column', 'currency — валюта операции KZT USD EUR RUB',              '{"column":"currency","role":"categorical"}'),
    ('column', 'source_bank — банк источник выписки kaspi halyk forte',   '{"column":"source_bank","role":"categorical"}'),
    ('column', 'purpose_text — назначение платежа текстовое описание',    '{"column":"purpose_text","role":"semantic"}'),
    ('column', 'operation_type_raw — вид операции категория документа',   '{"column":"operation_type_raw","role":"semantic"}'),
    ('column', 'sdp_name — наименование СДП платёжной системы',           '{"column":"sdp_name","role":"semantic"}'),
    ('column', 'raw_note — примечание из выписки',                        '{"column":"raw_note","role":"semantic"}'),
    ('column', 'semantic_text — объединённый смысловой текст операции',   '{"column":"semantic_text","role":"semantic"}'),
    ('column', 'payer_name — наименование ФИО плательщика',               '{"column":"payer_name","role":"entity"}'),
    ('column', 'payer_iin_bin — ИИН БИН плательщика 12 цифр',            '{"column":"payer_iin_bin","role":"identifier"}'),
    ('column', 'payer_bank — банк плательщика',                           '{"column":"payer_bank","role":"entity"}'),
    ('column', 'receiver_name — наименование ФИО получателя',             '{"column":"receiver_name","role":"entity"}'),
    ('column', 'receiver_iin_bin — ИИН БИН получателя 12 цифр',         '{"column":"receiver_iin_bin","role":"identifier"}'),
    ('column', 'receiver_bank — банк получателя',                         '{"column":"receiver_bank","role":"entity"}'),
    ('column', 'client_name — клиент владелец выписки',                   '{"column":"client_name","role":"entity"}'),
    ('column', 'account_iban — IBAN счёта выписки',                       '{"column":"account_iban","role":"identifier"}'),
    ('column', 'opening_balance — входящий остаток на начало периода',    '{"column":"opening_balance","role":"metric"}'),
    ('column', 'closing_balance — исходящий остаток на конец периода',    '{"column":"closing_balance","role":"metric"}')
ON CONFLICT DO NOTHING;
