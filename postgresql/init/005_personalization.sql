-- ==========================================================
-- Personalization Tables (Phase: Preference Extraction)
-- ==========================================================

-- preference_profiles: 構造化嗜好プロファイル
CREATE TABLE preference_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,

    -- 構造化嗜好
    category TEXT NOT NULL CHECK (category IN (
        'food', 'music', 'entertainment', 'hobby', 'sport',
        'place', 'work', 'lifestyle', 'social', 'value',
        'fashion', 'learning'
    )),
    item TEXT NOT NULL,
    sentiment TEXT NOT NULL DEFAULT 'positive' CHECK (sentiment IN (
        'positive', 'negative', 'neutral'
    )),
    intensity FLOAT NOT NULL DEFAULT 0.5 CHECK (intensity >= 0.0 AND intensity <= 1.0),
    context TEXT,

    -- 証拠
    source_memory_ids UUID[] NOT NULL DEFAULT '{}',
    evidence_count INTEGER NOT NULL DEFAULT 1,

    -- 時系列
    first_mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- 同一 entity x category x item で 1 レコード
    UNIQUE(bank_id, entity_id, category, item)
);

-- ==========================================================
-- Indexes
-- ==========================================================

CREATE INDEX idx_pref_bank_entity
    ON preference_profiles(bank_id, entity_id);

CREATE INDEX idx_pref_category
    ON preference_profiles(bank_id, entity_id, category);

-- item 名寄せ用 pg_trgm GIN インデックス
CREATE INDEX idx_pref_item_trgm
    ON preference_profiles USING gin (item gin_trgm_ops);

-- ==========================================================
-- Banks extension
-- ==========================================================

ALTER TABLE banks
    ADD COLUMN IF NOT EXISTS owner_entity_id UUID REFERENCES entities(id);

-- ==========================================================
-- Triggers
-- ==========================================================

CREATE TRIGGER update_preference_profiles_updated_at
    BEFORE UPDATE ON preference_profiles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================================
-- Recommendation History (Phase: Recommendation Engine)
-- ==========================================================

CREATE TABLE recommendation_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id UUID NOT NULL REFERENCES banks(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,

    -- 推薦内容
    category TEXT NOT NULL CHECK (category IN (
        'food', 'music', 'entertainment', 'hobby', 'sport',
        'place', 'work', 'lifestyle', 'social', 'value',
        'fashion', 'learning'
    )),
    recommended_items TEXT[] NOT NULL,
    context TEXT,

    -- フィードバック
    accepted BOOLEAN,
    accepted_item TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==========================================================
-- Recommendation History Indexes
-- ==========================================================

CREATE INDEX idx_rec_history_bank_entity_category
    ON recommendation_history(bank_id, entity_id, category);

CREATE INDEX idx_rec_history_recent
    ON recommendation_history(bank_id, entity_id, created_at DESC);
