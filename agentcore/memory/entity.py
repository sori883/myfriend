"""エンティティ抽出・解決モジュール

ファクトの who フィールドからエンティティを抽出し、pg_trgm による名前類似度で
既存エンティティとの照合・統合を行う。
"""

import logging
from dataclasses import dataclass

import asyncpg

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.6


@dataclass(frozen=True)
class ResolvedEntity:
    """解決済みエンティティ"""

    entity_id: str
    canonical_name: str
    entity_type: str
    is_new: bool


async def _find_matching_entity(
    conn: asyncpg.Connection,
    bank_id: str,
    name: str,
) -> asyncpg.Record | None:
    """pg_trgm の similarity() で既存エンティティを検索する"""
    return await conn.fetchrow(
        """
        SELECT id, canonical_name,
               similarity(LOWER(canonical_name), LOWER($1)) AS score
        FROM entities
        WHERE bank_id = $2::uuid
          AND similarity(LOWER(canonical_name), LOWER($1)) >= $3
        ORDER BY score DESC
        LIMIT 1
        """,
        name,
        bank_id,
        SIMILARITY_THRESHOLD,
    )


async def _create_entity(
    conn: asyncpg.Connection,
    bank_id: str,
    name: str,
    entity_type: str = "person",
) -> str:
    """新規エンティティを作成し、ID を返す"""
    row = await conn.fetchrow(
        """
        INSERT INTO entities (bank_id, canonical_name, entity_type)
        VALUES ($1::uuid, $2, $3)
        ON CONFLICT (bank_id, LOWER(canonical_name)) DO UPDATE
            SET mention_count = entities.mention_count + 1,
                last_seen = NOW()
        RETURNING id
        """,
        bank_id,
        name,
        entity_type,
    )
    return str(row["id"])


async def _update_entity_stats(
    conn: asyncpg.Connection,
    entity_id: str,
) -> None:
    """既存エンティティの統計を更新する"""
    await conn.execute(
        """
        UPDATE entities
        SET mention_count = mention_count + 1,
            last_seen = NOW()
        WHERE id = $1::uuid
        """,
        entity_id,
    )


async def resolve_entities(
    conn: asyncpg.Connection,
    bank_id: str,
    names: list[str],
) -> list[ResolvedEntity]:
    """名前リストからエンティティを解決する

    Phase 1 では名前類似度（pg_trgm）のみでマッチングする。
    スコア >= 0.6 で既存エンティティにマッチ、未満なら新規作成。

    Args:
        conn: DB コネクション（トランザクション内で使用）
        bank_id: メモリバンクID
        names: エンティティ名のリスト

    Returns:
        解決済みエンティティのリスト
    """
    if not names:
        return []

    resolved = []
    seen_names: dict[str, ResolvedEntity] = {}

    for name in names:
        name = name.strip()
        if not name:
            continue

        lower_name = name.lower()
        if lower_name in seen_names:
            resolved.append(seen_names[lower_name])
            continue

        match = await _find_matching_entity(conn, bank_id, name)

        if match is not None:
            await _update_entity_stats(conn, str(match["id"]))
            entity = ResolvedEntity(
                entity_id=str(match["id"]),
                canonical_name=match["canonical_name"],
                entity_type="person",
                is_new=False,
            )
        else:
            entity_id = await _create_entity(conn, bank_id, name)
            entity = ResolvedEntity(
                entity_id=entity_id,
                canonical_name=name,
                entity_type="person",
                is_new=True,
            )

        seen_names[lower_name] = entity
        resolved.append(entity)

    new_count = sum(1 for e in resolved if e.is_new)
    logger.info(
        "Resolved %d entities (%d new, %d existing)",
        len(resolved),
        new_count,
        len(resolved) - new_count,
    )
    return resolved
