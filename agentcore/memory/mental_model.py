"""Mental Model CRUD 操作

キュレーション済みサマリの作成・検索・更新・削除を提供する。
Embedding によるセマンティック検索、タグフィルタリングに対応。
"""

import json
import logging
from datetime import datetime, timezone

import asyncpg

from memory.embedding import generate_embedding
from memory.visibility import build_tags_where_clause

logger = logging.getLogger(__name__)

# セマンティック検索の類似度しきい値
SEARCH_SIMILARITY_THRESHOLD = 0.1

# is_stale 判定: last_refreshed_at から 7 日以上経過
STALE_THRESHOLD_DAYS = 7


async def create_mental_model(
    pool: asyncpg.Pool,
    bank_id: str,
    name: str,
    content: str,
    *,
    description: str | None = None,
    source_query: str | None = None,
    tags: list[str] | None = None,
    trigger: dict | None = None,
    max_tokens: int = 2048,
    entity_id: str | None = None,
) -> dict:
    """Mental Model を新規作成する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        name: タイトル
        content: サマリ本文
        description: 一行要約（省略時は None）
        source_query: リフレッシュ用クエリ
        tags: タグリスト
        trigger: トリガー設定（例: {"refresh_after_consolidation": true}）
        max_tokens: 最大トークン数
        entity_id: 紐づくエンティティID（自動生成時に設定）

    Returns:
        作成された Mental Model の dict
    """
    embedding = await generate_embedding(content)

    trigger_json = json.dumps(
        trigger or {"refresh_after_consolidation": False}
    )
    tags_value = tags or []

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO mental_models (
                bank_id, name, description, content, source_query,
                embedding, entity_id, tags, max_tokens, trigger, last_refreshed_at
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6::vector, $7::uuid, $8, $9, $10::jsonb, NOW()
            )
            RETURNING id, bank_id, name, description, content, source_query,
                      entity_id, tags, max_tokens, trigger, last_refreshed_at,
                      created_at, updated_at
            """,
            bank_id,
            name,
            description,
            content,
            source_query,
            embedding,
            entity_id,
            tags_value,
            max_tokens,
            trigger_json,
        )

    return _row_to_dict(row)


async def get_mental_model(
    pool: asyncpg.Pool,
    bank_id: str,
    mental_model_id: str,
) -> dict | None:
    """Mental Model を ID で取得する"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, bank_id, name, description, content, source_query,
                   entity_id, source_observation_ids, tags, max_tokens,
                   trigger, last_refreshed_at, created_at, updated_at
            FROM mental_models
            WHERE id = $1::uuid AND bank_id = $2::uuid
            """,
            mental_model_id,
            bank_id,
        )

    if not row:
        return None
    return _row_to_dict(row)


async def list_mental_models(
    pool: asyncpg.Pool,
    bank_id: str,
    *,
    tags: list[str] | None = None,
    tags_match: str = "any",
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    """Mental Model の一覧を取得する"""
    base_query = """
        SELECT id, bank_id, name, description, content, source_query,
               tags, max_tokens, trigger, last_refreshed_at,
               created_at, updated_at
        FROM mental_models
        WHERE bank_id = $1::uuid
    """
    params: list = [bank_id]
    next_param = 2

    if tags:
        tag_clause, tag_params, next_param = build_tags_where_clause(
            tags, next_param, tags_match
        )
        base_query += f" AND {tag_clause}"
        params.extend(tag_params)

    base_query += f" ORDER BY created_at DESC LIMIT ${next_param} OFFSET ${next_param + 1}"
    params.extend([limit, offset])

    async with pool.acquire() as conn:
        rows = await conn.fetch(base_query, *params)

    return [_row_to_dict(r) for r in rows]


async def search_mental_models(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
    *,
    tags: list[str] | None = None,
    tags_match: str = "any",
    max_results: int = 5,
    exclude_ids: list[str] | None = None,
) -> list[dict]:
    """Mental Model をセマンティック検索する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        query: 検索クエリ
        tags: タグフィルタ
        tags_match: マッチモード
        max_results: 最大結果数
        exclude_ids: 除外する Mental Model ID リスト

    Returns:
        検索結果のリスト（similarity 降順）
    """
    query_embedding = await generate_embedding(query)

    base_query = """
        SELECT id, bank_id, name, description, content, source_query,
               tags, max_tokens, trigger, last_refreshed_at,
               created_at, updated_at,
               1 - (embedding <=> $1::vector) AS similarity
        FROM mental_models
        WHERE bank_id = $2::uuid
          AND embedding IS NOT NULL
          AND (1 - (embedding <=> $1::vector)) >= $3
    """
    params: list = [query_embedding, bank_id, SEARCH_SIMILARITY_THRESHOLD]
    next_param = 4

    if tags:
        tag_clause, tag_params, next_param = build_tags_where_clause(
            tags, next_param, tags_match
        )
        base_query += f" AND {tag_clause}"
        params.extend(tag_params)

    if exclude_ids:
        base_query += f" AND id != ALL(${next_param}::uuid[])"
        params.append(exclude_ids)
        next_param += 1

    base_query += f" ORDER BY embedding <=> $1::vector LIMIT ${next_param}"
    params.append(max_results)

    async with pool.acquire() as conn:
        rows = await conn.fetch(base_query, *params)

    now = datetime.now(timezone.utc)
    results = []
    for row in rows:
        d = _row_to_dict(row)
        d["similarity"] = float(row["similarity"])
        d["is_stale"] = _is_stale(row, now)
        results.append(d)

    return results


async def update_mental_model(
    pool: asyncpg.Pool,
    bank_id: str,
    mental_model_id: str,
    *,
    name: str | None = None,
    description: str | None = None,
    content: str | None = None,
    source_query: str | None = None,
    tags: list[str] | None = None,
    trigger: dict | None = None,
    max_tokens: int | None = None,
    source_observation_ids: list[str] | None = None,
) -> dict | None:
    """Mental Model を更新する

    指定されたフィールドのみ更新する。content が更新された場合、
    Embedding も再生成する。
    """
    set_clauses = []
    params: list = []
    param_idx = 1

    if name is not None:
        set_clauses.append(f"name = ${param_idx}")
        params.append(name)
        param_idx += 1

    if description is not None:
        set_clauses.append(f"description = ${param_idx}")
        params.append(description)
        param_idx += 1

    if content is not None:
        set_clauses.append(f"content = ${param_idx}")
        params.append(content)
        param_idx += 1

        embedding = await generate_embedding(content)
        set_clauses.append(f"embedding = ${param_idx}::vector")
        params.append(embedding)
        param_idx += 1

        set_clauses.append(f"last_refreshed_at = ${param_idx}")
        params.append(datetime.now(timezone.utc))
        param_idx += 1

    if source_query is not None:
        set_clauses.append(f"source_query = ${param_idx}")
        params.append(source_query)
        param_idx += 1

    if tags is not None:
        set_clauses.append(f"tags = ${param_idx}")
        params.append(tags)
        param_idx += 1

    if trigger is not None:
        set_clauses.append(f"trigger = ${param_idx}::jsonb")
        params.append(json.dumps(trigger))
        param_idx += 1

    if max_tokens is not None:
        set_clauses.append(f"max_tokens = ${param_idx}")
        params.append(max_tokens)
        param_idx += 1

    if source_observation_ids is not None:
        set_clauses.append(f"source_observation_ids = ${param_idx}::uuid[]")
        params.append(source_observation_ids)
        param_idx += 1

    if not set_clauses:
        return await get_mental_model(pool, bank_id, mental_model_id)

    set_sql = ", ".join(set_clauses)
    params.extend([mental_model_id, bank_id])

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE mental_models
            SET {set_sql}
            WHERE id = ${param_idx}::uuid AND bank_id = ${param_idx + 1}::uuid
            RETURNING id, bank_id, name, description, content, source_query,
                      entity_id, source_observation_ids, tags, max_tokens,
                      trigger, last_refreshed_at, created_at, updated_at
            """,
            *params,
        )

    if not row:
        return None
    return _row_to_dict(row)


async def delete_mental_model(
    pool: asyncpg.Pool,
    bank_id: str,
    mental_model_id: str,
) -> bool:
    """Mental Model を削除する

    Returns:
        削除成功なら True、存在しなければ False
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM mental_models
            WHERE id = $1::uuid AND bank_id = $2::uuid
            """,
            mental_model_id,
            bank_id,
        )

    return result == "DELETE 1"


async def get_refreshable_models(
    pool: asyncpg.Pool,
    bank_id: str,
) -> list[dict]:
    """refresh_after_consolidation が true の Mental Model を取得する"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, bank_id, name, source_query, tags, max_tokens,
                   trigger, last_refreshed_at
            FROM mental_models
            WHERE bank_id = $1::uuid
              AND (trigger->>'refresh_after_consolidation')::boolean = true
              AND source_query IS NOT NULL
            ORDER BY created_at ASC
            """,
            bank_id,
        )

    return [_row_to_dict(r) for r in rows]


# ---------- 内部ヘルパー ----------


def _row_to_dict(row: asyncpg.Record) -> dict:
    """asyncpg.Record を dict に変換する"""
    d = dict(row)

    # UUID → str
    for key in ("id", "bank_id", "entity_id"):
        if key in d and d[key] is not None:
            d[key] = str(d[key])

    # UUID[] → str[]
    for key in ("source_observation_ids",):
        if key in d and d[key] is not None:
            d[key] = [str(uid) for uid in d[key]]

    # datetime → isoformat
    for key in ("last_refreshed_at", "created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()

    # JSONB → dict（trigger）
    if "trigger" in d and isinstance(d["trigger"], str):
        d["trigger"] = json.loads(d["trigger"])

    # embedding は返さない（大きいため）
    d.pop("embedding", None)

    return d


def _is_stale(row: asyncpg.Record, now: datetime) -> bool:
    """Mental Model が stale かどうかを判定する"""
    last_refreshed = row.get("last_refreshed_at")
    if last_refreshed is None:
        return True

    if last_refreshed.tzinfo is None:
        last_refreshed = last_refreshed.replace(tzinfo=timezone.utc)

    days_since = (now - last_refreshed).days
    return days_since >= STALE_THRESHOLD_DAYS
