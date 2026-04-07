-- =============================================================
-- init_schema.sql  –  Exact replica of the GCP Cloud SQL schema
-- Run via:  python db_fix/init_db.py
-- =============================================================

-- 0. pgvector extension (requires superuser the first time)
CREATE EXTENSION IF NOT EXISTS vector;

-- 1. Schema
DROP SCHEMA IF EXISTS afm CASCADE;
CREATE SCHEMA afm;

-- ========================  TABLES  ========================

-- field_discovery_log
CREATE TABLE afm.field_discovery_log (
    id              bigint NOT NULL,
    created_at      timestamptz DEFAULT now() NOT NULL,
    file_id         uuid,
    source_bank     text,
    format_id       uuid,
    raw_column_name text NOT NULL,
    normalized_name text,
    sample_values   jsonb,
    suggested_field text,
    confidence      real,
    status          text DEFAULT 'new'::text NOT NULL
);

CREATE SEQUENCE afm.field_discovery_log_id_seq
    START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

ALTER SEQUENCE afm.field_discovery_log_id_seq
    OWNED BY afm.field_discovery_log.id;

ALTER TABLE ONLY afm.field_discovery_log
    ALTER COLUMN id SET DEFAULT nextval('afm.field_discovery_log_id_seq'::regclass);


-- format_registry
CREATE TABLE afm.format_registry (
    format_id          uuid NOT NULL,
    source_bank        text,
    header_fingerprint text,
    header_sample      jsonb,
    embedding_vector   bytea,
    first_seen         timestamptz DEFAULT now() NOT NULL,
    last_seen          timestamptz DEFAULT now() NOT NULL,
    usage_count        integer DEFAULT 1 NOT NULL
);


-- pending_registrations
CREATE TABLE afm.pending_registrations (
    id                integer NOT NULL,
    email             varchar NOT NULL,
    password_hash     varchar NOT NULL,
    verification_code varchar(6) NOT NULL,
    expires_at        timestamp NOT NULL,
    created_at        timestamp NOT NULL
);

CREATE SEQUENCE afm.pending_registrations_id_seq
    AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

ALTER SEQUENCE afm.pending_registrations_id_seq
    OWNED BY afm.pending_registrations.id;

ALTER TABLE ONLY afm.pending_registrations
    ALTER COLUMN id SET DEFAULT nextval('afm.pending_registrations_id_seq'::regclass);


-- query_history
CREATE TABLE afm.query_history (
    id                uuid DEFAULT gen_random_uuid() NOT NULL,
    question          text NOT NULL,
    generated_sql     text,
    execution_success boolean DEFAULT false NOT NULL,
    user_feedback     smallint,
    embedding         public.vector(1024),
    created_at        timestamptz DEFAULT now() NOT NULL,
    feedback_note     text,
    feedback_at       timestamptz,
    app_source        text,
    attempt_count     integer,
    execution_time_ms integer,
    row_count         integer,
    repaired          boolean DEFAULT false NOT NULL,
    query_mode        text,
    max_rows          integer,
    llm_backend       text,
    llm_model         text,
    edited_sql        boolean DEFAULT false NOT NULL,
    error_text        text
);


-- raw_files
CREATE TABLE afm.raw_files (
    file_id        uuid NOT NULL,
    source_bank    text NOT NULL,
    original_filename text NOT NULL,
    sha256         text NOT NULL,
    uploaded_at    timestamptz DEFAULT now() NOT NULL,
    parsed_at      timestamptz,
    parser_version text NOT NULL,
    notes          text
);


-- semantic_catalog
CREATE TABLE afm.semantic_catalog (
    id              uuid DEFAULT gen_random_uuid() NOT NULL,
    type            text DEFAULT 'tx'::text NOT NULL,
    text            text,
    tx_id           uuid,
    source_bank     text,
    semantic_text   text,
    source_columns  jsonb,
    embedding       public.vector(1024),
    meta            jsonb,
    created_at      timestamptz DEFAULT now() NOT NULL
);


-- semantic_clusters
CREATE TABLE afm.semantic_clusters (
    cluster_id          uuid DEFAULT gen_random_uuid() NOT NULL,
    source_bank         text,
    cluster_label       text,
    cluster_keywords    jsonb,
    centroid_embedding  public.vector(1024),
    sample_texts        jsonb,
    tx_count            integer DEFAULT 0 NOT NULL,
    created_at          timestamptz DEFAULT now() NOT NULL
);


-- statements
CREATE TABLE afm.statements (
    statement_id    uuid NOT NULL,
    file_id         uuid NOT NULL,
    source_bank     text NOT NULL,
    source_sheet    text,
    source_block_id integer,
    format_id       uuid,
    client_name     text,
    client_iin_bin  char(12),
    account_iban    text,
    account_type    text,
    currency        text,
    statement_date  date,
    period_from     date,
    period_to       date,
    opening_balance numeric(18,2),
    closing_balance numeric(18,2),
    total_debit     numeric(18,2),
    total_credit    numeric(18,2),
    meta_json       jsonb
);


-- transaction_upload_meta
CREATE TABLE afm.transaction_upload_meta (
    tx_id              uuid NOT NULL,
    uploaded_by_email  varchar NOT NULL,
    created_at         timestamp NOT NULL
);


-- transactions_core
CREATE TABLE afm.transactions_core (
    tx_id               uuid NOT NULL,
    file_id             uuid NOT NULL,
    statement_id        uuid,
    format_id           uuid,
    source_bank         text NOT NULL,
    source_sheet        text,
    source_block_id     integer,
    source_row_no       integer,
    row_hash            text NOT NULL,
    operation_ts        timestamptz,
    operation_date      date,
    currency            text,
    amount_currency     numeric(18,2),
    amount_kzt          numeric(18,2),
    amount_credit       numeric(18,2),
    amount_debit        numeric(18,2),
    direction           text,
    operation_type_raw  text,
    sdp_name            text,
    purpose_code        text,
    purpose_text        text,
    raw_note            text,
    payer_name          text,
    payer_iin_bin       char(12),
    payer_residency     text,
    payer_bank          text,
    payer_account       text,
    receiver_name       text,
    receiver_iin_bin    char(12),
    receiver_residency  text,
    receiver_bank       text,
    receiver_account    text,
    confidence_score    real DEFAULT 1.0 NOT NULL,
    parse_warnings      text,
    raw_row_json        jsonb,
    semantic_text       text,
    semantic_embedding  public.vector(1024)
);


-- transactions_ext
CREATE TABLE afm.transactions_ext (
    tx_id    uuid NOT NULL,
    ext_json jsonb NOT NULL
);


-- users
CREATE TABLE afm.users (
    id            integer NOT NULL,
    email         varchar NOT NULL,
    password_hash varchar NOT NULL,
    role          varchar NOT NULL
);

CREATE SEQUENCE afm.users_id_seq
    AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

ALTER SEQUENCE afm.users_id_seq OWNED BY afm.users.id;

ALTER TABLE ONLY afm.users
    ALTER COLUMN id SET DEFAULT nextval('afm.users_id_seq'::regclass);


-- ========================  VIEWS  ========================

CREATE VIEW afm.transactions_nl_view AS
 SELECT tc.tx_id,
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
        concat_ws(' | ',
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


CREATE VIEW afm.transactions_view AS
 SELECT tx_id, source_bank, operation_ts, operation_date,
        currency, amount_currency, amount_kzt, amount_credit,
        amount_debit, direction, payer_name, payer_iin_bin,
        receiver_name, receiver_iin_bin, purpose_text, sdp_name
   FROM afm.transactions_core;


-- ====================  PRIMARY KEYS  =====================

ALTER TABLE ONLY afm.field_discovery_log
    ADD CONSTRAINT field_discovery_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY afm.format_registry
    ADD CONSTRAINT format_registry_pkey PRIMARY KEY (format_id);

ALTER TABLE ONLY afm.format_registry
    ADD CONSTRAINT format_registry_header_fingerprint_key UNIQUE (header_fingerprint);

ALTER TABLE ONLY afm.pending_registrations
    ADD CONSTRAINT pending_registrations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY afm.query_history
    ADD CONSTRAINT query_history_pkey PRIMARY KEY (id);

ALTER TABLE ONLY afm.raw_files
    ADD CONSTRAINT raw_files_pkey PRIMARY KEY (file_id);

ALTER TABLE ONLY afm.semantic_catalog
    ADD CONSTRAINT semantic_catalog_pkey PRIMARY KEY (id);

ALTER TABLE ONLY afm.semantic_clusters
    ADD CONSTRAINT semantic_clusters_pkey PRIMARY KEY (cluster_id);

ALTER TABLE ONLY afm.statements
    ADD CONSTRAINT statements_pkey PRIMARY KEY (statement_id);

ALTER TABLE ONLY afm.transaction_upload_meta
    ADD CONSTRAINT transaction_upload_meta_pkey PRIMARY KEY (tx_id);

ALTER TABLE ONLY afm.transactions_core
    ADD CONSTRAINT transactions_core_pkey PRIMARY KEY (tx_id);

ALTER TABLE ONLY afm.transactions_ext
    ADD CONSTRAINT transactions_ext_pkey PRIMARY KEY (tx_id);

ALTER TABLE ONLY afm.transactions_core
    ADD CONSTRAINT uq_tx_rowhash UNIQUE (row_hash);

ALTER TABLE ONLY afm.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


-- =======================  INDEXES  =======================

CREATE INDEX idx_fmt_bank        ON afm.format_registry   USING btree (source_bank);
CREATE INDEX idx_qh_emb          ON afm.query_history     USING ivfflat (embedding public.vector_cosine_ops) WITH (lists='50');
CREATE INDEX idx_qh_success      ON afm.query_history     USING btree (execution_success);
CREATE INDEX idx_sem_cat_bank    ON afm.semantic_catalog   USING btree (source_bank);
CREATE INDEX idx_sem_cat_emb     ON afm.semantic_catalog   USING ivfflat (embedding public.vector_cosine_ops) WITH (lists='100');
CREATE INDEX idx_sem_cat_type    ON afm.semantic_catalog   USING btree (type);
CREATE INDEX idx_sem_cl_bank     ON afm.semantic_clusters  USING btree (source_bank);
CREATE INDEX idx_sem_cl_emb      ON afm.semantic_clusters  USING ivfflat (centroid_embedding public.vector_cosine_ops) WITH (lists='20');
CREATE INDEX idx_stmt_account    ON afm.statements         USING btree (account_iban);
CREATE INDEX idx_stmt_file       ON afm.statements         USING btree (file_id);
CREATE INDEX idx_tx_core_date    ON afm.transactions_core  USING btree (operation_date);
CREATE INDEX idx_tx_core_file    ON afm.transactions_core  USING btree (file_id);
CREATE INDEX idx_tx_format       ON afm.transactions_core  USING btree (format_id);
CREATE INDEX idx_tx_semantic_emb ON afm.transactions_core  USING ivfflat (semantic_embedding public.vector_cosine_ops) WITH (lists='100');
CREATE INDEX idx_tx_stmt         ON afm.transactions_core  USING btree (statement_id);

CREATE UNIQUE INDEX ix_afm_pending_registrations_email      ON afm.pending_registrations USING btree (email);
CREATE INDEX        ix_afm_pending_registrations_expires_at  ON afm.pending_registrations USING btree (expires_at);
CREATE INDEX        ix_afm_pending_registrations_id          ON afm.pending_registrations USING btree (id);
CREATE INDEX        ix_afm_transaction_upload_meta_tx_id     ON afm.transaction_upload_meta USING btree (tx_id);
CREATE INDEX        ix_afm_transaction_upload_meta_uploaded_by_email ON afm.transaction_upload_meta USING btree (uploaded_by_email);
CREATE UNIQUE INDEX ix_afm_users_email ON afm.users USING btree (email);
CREATE INDEX        ix_afm_users_id    ON afm.users USING btree (id);


-- =================  FOREIGN KEYS  =======================

ALTER TABLE ONLY afm.field_discovery_log
    ADD CONSTRAINT field_discovery_log_file_id_fkey
    FOREIGN KEY (file_id) REFERENCES afm.raw_files(file_id);

ALTER TABLE ONLY afm.statements
    ADD CONSTRAINT statements_file_id_fkey
    FOREIGN KEY (file_id) REFERENCES afm.raw_files(file_id);

ALTER TABLE ONLY afm.statements
    ADD CONSTRAINT statements_format_id_fkey
    FOREIGN KEY (format_id) REFERENCES afm.format_registry(format_id);

ALTER TABLE ONLY afm.transactions_core
    ADD CONSTRAINT transactions_core_file_id_fkey
    FOREIGN KEY (file_id) REFERENCES afm.raw_files(file_id);

ALTER TABLE ONLY afm.transactions_core
    ADD CONSTRAINT transactions_core_format_id_fkey
    FOREIGN KEY (format_id) REFERENCES afm.format_registry(format_id);

ALTER TABLE ONLY afm.transactions_core
    ADD CONSTRAINT transactions_core_statement_id_fkey
    FOREIGN KEY (statement_id) REFERENCES afm.statements(statement_id);

ALTER TABLE ONLY afm.transactions_ext
    ADD CONSTRAINT transactions_ext_tx_id_fkey
    FOREIGN KEY (tx_id) REFERENCES afm.transactions_core(tx_id);

-- =============================================================
-- Done. Local DB now mirrors GCP Cloud SQL schema exactly.
-- =============================================================
