-- ============================================================================
-- Semantic Intelligence Layer — DB additions
-- Run once, idempotent.
-- Requires pgvector extension (already created by ensure_schema).
-- ============================================================================

-- ── 1. semantic_catalog ───────────────────────────────────────────────────────
-- One record per transaction's semantic_text, with its embedding.
-- Used for: cluster building, fallback ANN search, catalog seeding.
CREATE TABLE IF NOT EXISTS afm.semantic_catalog (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_id          UUID        REFERENCES afm.transactions_core(tx_id),
    source_bank    TEXT,
    semantic_text  TEXT        NOT NULL,
    source_columns JSONB,
    embedding      VECTOR(1024),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sem_cat_tx      ON afm.semantic_catalog(tx_id);
CREATE INDEX IF NOT EXISTS idx_sem_cat_bank    ON afm.semantic_catalog(source_bank);
CREATE INDEX IF NOT EXISTS idx_sem_cat_emb
    ON afm.semantic_catalog
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);


-- ── 2. semantic_clusters ─────────────────────────────────────────────────────
-- Computed cluster centroids with labels and sample texts.
-- Rebuilt offline by ClusterBuilder.run().
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


-- ── 3. query_history (if not yet created by nl2sql schema) ───────────────────
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
