"""嗜好クエリモジュール

preference_profiles テーブルからユーザーの嗜好データを取得し、
JSON 形式で整形して返す。get_user_profile ツール用。
"""

import logging

import asyncpg

logger = logging.getLogger(__name__)

MAX_PREFERENCES_PER_QUERY = 100


async def query_preferences(
    pool: asyncpg.Pool,
    bank_id: str,
    entity_id: str,
    category: str = "",
) -> dict:
    """嗜好プロファイルを取得する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        entity_id: エンティティID
        category: カテゴリ（空文字で全カテゴリ）

    Returns:
        整形済みの嗜好データ辞書
    """
    async with pool.acquire() as conn:
        if category:
            rows = await conn.fetch(
                """SELECT category, item, sentiment, intensity,
                          evidence_count, last_mentioned_at
                   FROM preference_profiles
                   WHERE bank_id = $1::uuid
                     AND entity_id = $2::uuid
                     AND category = $3
                   ORDER BY intensity DESC, evidence_count DESC
                   LIMIT $4""",
                bank_id,
                entity_id,
                category,
                MAX_PREFERENCES_PER_QUERY,
            )
        else:
            rows = await conn.fetch(
                """SELECT category, item, sentiment, intensity,
                          evidence_count, last_mentioned_at
                   FROM preference_profiles
                   WHERE bank_id = $1::uuid
                     AND entity_id = $2::uuid
                   ORDER BY category, intensity DESC
                   LIMIT $3""",
                bank_id,
                entity_id,
                MAX_PREFERENCES_PER_QUERY,
            )

    preferences: dict[str, list[dict]] = {}
    for row in rows:
        cat = row["category"]
        preferences.setdefault(cat, []).append({
            "item": row["item"],
            "sentiment": row["sentiment"],
            "intensity": round(row["intensity"], 2),
            "evidence_count": row["evidence_count"],
            "last_mentioned": row["last_mentioned_at"].strftime("%Y-%m-%d"),
        })

    return {
        "preferences": preferences,
        "total_count": len(rows),
    }


async def get_owner_entity(
    pool: asyncpg.Pool,
    bank_id: str,
) -> tuple[str, str] | None:
    """bank の owner entity の (id, canonical_name) を取得する"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT e.id, e.canonical_name
               FROM banks b
               JOIN entities e ON b.owner_entity_id = e.id
               WHERE b.id = $1::uuid""",
            bank_id,
        )
        if row:
            return (str(row["id"]), row["canonical_name"])
        return None
