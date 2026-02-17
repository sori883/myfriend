-- ==========================================================
-- Short-term Memory Tables
-- ==========================================================

-- Banks: メモリバンク（1ユーザー=1バンク）
CREATE TABLE banks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    mission TEXT,
    background TEXT,
    disposition JSONB DEFAULT '{"skepticism": 3, "literalism": 3, "empathy": 3}'::jsonb,
    directives TEXT[],
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Documents: ソーストラッキング
CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    external_id TEXT,
    content_hash TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(bank_id, external_id)
);

-- Memory Units: 全記憶の統合テーブル
CREATE TABLE memory_units (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,

    -- Content
    text TEXT NOT NULL,
    context TEXT,
    embedding vector(1024),

    -- Classification
    fact_type TEXT NOT NULL CHECK (fact_type IN ('world', 'experience', 'observation')),
    fact_kind TEXT CHECK (fact_kind IN ('event', 'conversation')),

    -- 5W1H Structure
    what TEXT,
    who TEXT[],
    when_description TEXT,
    where_description TEXT,
    why_description TEXT,

    -- Temporal metadata
    event_date TIMESTAMPTZ,
    occurred_start TIMESTAMPTZ,
    occurred_end TIMESTAMPTZ,
    mentioned_at TIMESTAMPTZ,

    -- Full-text search (auto-generated)
    -- TODO: 日本語対応が必要な場合は pg_bigm / pgroonga を検討。
    -- 現在は english トークナイザー。セマンティック検索(Embedding)がメインのため初期は許容。
    search_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', COALESCE(text, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(context, '')), 'B')
    ) STORED,

    -- Observation-specific
    proof_count INTEGER DEFAULT 0,
    source_memory_ids UUID[],
    history JSONB DEFAULT '[]'::jsonb,
    confidence_score FLOAT DEFAULT 1.0,

    -- Consolidation
    consolidated_at TIMESTAMPTZ,

    -- Access control
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Entities: 正規化エンティティマスタ
CREATE TABLE entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    canonical_name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN (
        'person', 'organization', 'location', 'concept', 'event', 'other'
    )),
    metadata JSONB DEFAULT '{}'::jsonb,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    mention_count INTEGER DEFAULT 1
);

-- 関数式ベースのユニーク制約（インラインUNIQUEでは不可）
CREATE UNIQUE INDEX idx_entities_bank_canonical ON entities
    (bank_id, LOWER(canonical_name));

-- Unit Entities: memory_units <-> entities 中間テーブル
CREATE TABLE unit_entities (
    unit_id UUID NOT NULL REFERENCES memory_units(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (unit_id, entity_id)
);

-- ==========================================================
-- Indexes
-- ==========================================================

-- HNSW Vector Index（セマンティック検索）
CREATE INDEX idx_memory_units_embedding ON memory_units
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 512);

-- fact_type別パーシャルHNSW
CREATE INDEX idx_memory_units_embedding_world ON memory_units
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 256)
    WHERE fact_type = 'world' AND embedding IS NOT NULL;

CREATE INDEX idx_memory_units_embedding_experience ON memory_units
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 256)
    WHERE fact_type = 'experience' AND embedding IS NOT NULL;

-- GIN Index（全文検索）
CREATE INDEX idx_memory_units_search_vector ON memory_units
    USING gin (search_vector);

-- B-tree Composite: 主検索パターン
CREATE INDEX idx_memory_units_bank_type_date ON memory_units
    (bank_id, fact_type, event_date DESC NULLS LAST);

-- エンティティ検索
CREATE INDEX idx_entities_bank_name ON entities
    (bank_id, LOWER(canonical_name));

CREATE INDEX idx_unit_entities_entity ON unit_entities (entity_id);
CREATE INDEX idx_unit_entities_unit ON unit_entities (unit_id);

-- NOTE: 以下のインデックスは後続フェーズで追加
-- Phase 2 (003_mid_term.sql): idx_memory_units_embedding_observation, idx_memory_units_unconsolidated,
--   idx_memory_units_tags, idx_memory_units_occurred, idx_memory_units_mentioned,
--   idx_memory_links_*, idx_cooccurrences_*, idx_async_ops_pending
-- Phase 3 (004_long_term.sql): idx_mental_models_embedding, idx_mental_models_bank

-- ==========================================================
-- Triggers: updated_at 自動更新
-- ==========================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_banks_updated_at
    BEFORE UPDATE ON banks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_memory_units_updated_at
    BEFORE UPDATE ON memory_units
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
