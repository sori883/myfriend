-- ==========================================================
-- Extensions (Aurora PostgreSQL compatible)
-- pg_bigm, age は Aurora 非対応のため除外
-- ==========================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
