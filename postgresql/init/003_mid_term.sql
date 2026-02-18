-- ==========================================================
-- Mid-term Memory Tables (Phase 2: Observations)
-- ==========================================================

-- Memory Links: グラフエッジ（temporal/semantic/entity/causes/caused_by）
CREATE TABLE memory_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    from_unit_id UUID NOT NULL REFERENCES memory_units(id) ON DELETE CASCADE,
    to_unit_id UUID NOT NULL REFERENCES memory_units(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL CHECK (link_type IN (
        'temporal', 'semantic', 'entity', 'causes', 'caused_by'
    )),
    entity_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    weight FLOAT NOT NULL DEFAULT 1.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(from_unit_id, to_unit_id, link_type, entity_id)
);

-- Entity Co-occurrences: 共起頻度キャッシュ
CREATE TABLE entity_cooccurrences (
    entity_id_1 UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    entity_id_2 UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    cooccurrence_count INTEGER DEFAULT 1,
    last_cooccurred TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (entity_id_1, entity_id_2),
    CHECK (entity_id_1 < entity_id_2)
);

-- Async Operations: バックグラウンドジョブ追跡
CREATE TABLE async_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    operation_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'processing', 'completed', 'failed'
    )),
    worker_id TEXT,
    payload JSONB DEFAULT '{}'::jsonb,
    result JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- ==========================================================
-- Indexes
-- ==========================================================

-- Observation 用パーシャル HNSW（fact_type = 'observation' のセマンティック検索高速化）
CREATE INDEX idx_memory_units_embedding_observation ON memory_units
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 256)
    WHERE fact_type = 'observation' AND embedding IS NOT NULL;

-- グラフトラバーサル（順方向）
CREATE INDEX idx_memory_links_traversal ON memory_links
    (from_unit_id, link_type, weight DESC);

-- グラフトラバーサル（weight >= 0.1 フィルタ付き、MPFP 検索用）
CREATE INDEX idx_memory_links_weighted ON memory_links
    (from_unit_id, link_type, weight DESC)
    WHERE weight >= 0.1;

-- 逆方向グラフトラバーサル
CREATE INDEX idx_memory_links_reverse ON memory_links
    (to_unit_id, link_type, weight DESC);

-- 共起検索（entity1 起点）
CREATE INDEX idx_cooccurrences_entity1 ON entity_cooccurrences (entity_id_1);

-- 共起検索（entity2 起点）
CREATE INDEX idx_cooccurrences_entity2 ON entity_cooccurrences (entity_id_2);

-- Consolidation Worker: 未統合 Fact 検索
CREATE INDEX idx_memory_units_unconsolidated ON memory_units
    (bank_id, created_at ASC)
    WHERE consolidated_at IS NULL;

-- 非同期ジョブポーリング
CREATE INDEX idx_async_ops_pending ON async_operations
    (status, created_at ASC)
    WHERE status = 'pending';

-- 時間範囲クエリ（occurred_start/occurred_end）
CREATE INDEX idx_memory_units_occurred ON memory_units
    (bank_id, occurred_start, occurred_end)
    WHERE occurred_start IS NOT NULL;

-- mentioned_at クエリ
CREATE INDEX idx_memory_units_mentioned ON memory_units
    (bank_id, mentioned_at DESC)
    WHERE mentioned_at IS NOT NULL;

-- タグフィルタリング（GIN）
CREATE INDEX idx_memory_units_tags ON memory_units USING gin (tags);

-- NOTE: async_operations は updated_at を持たない
-- （status 変更は started_at / completed_at で追跡する）
