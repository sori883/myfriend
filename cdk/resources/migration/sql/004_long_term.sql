-- ==========================================================
-- Long-term Memory Tables (Phase 3: Mental Models)
-- ==========================================================

-- Mental Models: キュレーション済みサマリ（最高信頼度の知識）
CREATE TABLE mental_models (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    content TEXT,
    source_query TEXT,
    embedding vector(1024),
    entity_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    source_observation_ids UUID[] DEFAULT '{}',
    tags TEXT[] DEFAULT '{}',
    max_tokens INTEGER DEFAULT 2048,
    trigger JSONB DEFAULT '{"refresh_after_consolidation": false}'::jsonb,
    last_refreshed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Chunks: ソーステキストチャンク（expand 用）
CREATE TABLE chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    memory_unit_id UUID NOT NULL REFERENCES memory_units(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    text TEXT NOT NULL,
    embedding vector(1024),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==========================================================
-- Indexes
-- ==========================================================

-- Mental Models: bank 検索
CREATE INDEX idx_mental_models_bank ON mental_models (bank_id);

-- Mental Models: セマンティック検索（HNSW）
CREATE INDEX idx_mental_models_embedding ON mental_models
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 256)
    WHERE embedding IS NOT NULL;

-- Mental Models: タグフィルタリング（GIN）
CREATE INDEX idx_mental_models_tags ON mental_models USING gin (tags);

-- Mental Models: エンティティ検索 + 重複防止（bank 内で entity_id は一意）
CREATE UNIQUE INDEX idx_mental_models_bank_entity ON mental_models (bank_id, entity_id)
    WHERE entity_id IS NOT NULL;

-- Chunks: memory_unit 逆引き
CREATE INDEX idx_chunks_memory_unit ON chunks (memory_unit_id, chunk_index);

-- Chunks: セマンティック検索（HNSW）
CREATE INDEX idx_chunks_embedding ON chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 256)
    WHERE embedding IS NOT NULL;

-- ==========================================================
-- Triggers
-- ==========================================================

CREATE TRIGGER update_mental_models_updated_at
    BEFORE UPDATE ON mental_models
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
