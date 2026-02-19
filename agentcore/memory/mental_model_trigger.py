"""Mental Model 自動リフレッシュ・自動生成

Consolidation 後に呼び出され、Mental Model の自動リフレッシュと
エンティティベースの自動生成を行う。

- リフレッシュ: refresh_after_consolidation=true のモデルを更新
- 自動生成: Observation 数が閾値以上のエンティティに新規作成
"""

import logging

import asyncpg

logger = logging.getLogger(__name__)


# ---------- 自動リフレッシュ ----------


MAX_REFRESH_PER_CONSOLIDATION = 3


async def trigger_mental_model_refresh(
    pool: asyncpg.Pool,
    bank_id: str,
) -> int:
    """refresh_after_consolidation が true の Mental Model をリフレッシュする

    1回の Consolidation あたり最大 MAX_REFRESH_PER_CONSOLIDATION 件まで。

    Returns:
        リフレッシュされた Mental Model の数
    """
    from memory.mental_model import get_refreshable_models, update_mental_model
    from memory.reflect import reflect

    models = await get_refreshable_models(pool, bank_id)
    if not models:
        return 0

    models = models[:MAX_REFRESH_PER_CONSOLIDATION]

    refreshed = 0
    for model in models:
        source_query = model.get("source_query")
        if not source_query:
            continue

        model_id = model["id"]
        model_tags = model.get("tags") or []

        # タグセキュリティ: タグ付きモデルは all_strict で検索
        tags_match = "all_strict" if model_tags else "any"

        try:
            result = await reflect(
                pool,
                bank_id,
                source_query,
                tags=model_tags if model_tags else None,
                tags_match=tags_match,
                exclude_mental_model_ids=[model_id],
            )

            answer = result.get("answer", "")
            if answer:
                await update_mental_model(
                    pool,
                    bank_id,
                    model_id,
                    content=answer,
                    source_observation_ids=result.get("observation_ids"),
                )
                refreshed += 1
                logger.info(
                    "Refreshed mental model %s for bank %s",
                    model_id, bank_id,
                )
        except Exception:
            logger.exception(
                "Failed to refresh mental model %s for bank %s",
                model_id, bank_id,
            )

    return refreshed


# ---------- 自動生成 ----------


MIN_OBSERVATIONS_FOR_GENERATION = 5

MAX_GENERATION_PER_CONSOLIDATION = 2


async def trigger_mental_model_generation(
    pool: asyncpg.Pool,
    bank_id: str,
    affected_observation_ids: list[str],
    mission: str,
) -> int:
    """Observation が十分蓄積されたエンティティに Mental Model を自動生成する

    affected_observation_ids に紐づくエンティティのうち、
    Observation 数が閾値以上かつ Mental Model が未作成のものを選定し、
    Reflect で content を生成して保存する。

    Returns:
        生成された Mental Model の数
    """
    if not affected_observation_ids:
        return 0

    candidates = await _find_generation_candidates(
        pool, bank_id, affected_observation_ids,
    )
    if not candidates:
        return 0

    from memory.mental_model import create_mental_model
    from memory.reflect import reflect

    generated = 0
    for candidate in candidates:
        entity_id = str(candidate["entity_id"])
        entity_name = candidate["canonical_name"]

        try:
            # 重複チェック（entity_id + 名前類似度）
            if await _check_mental_model_exists(pool, bank_id, entity_id, entity_name):
                logger.debug(
                    "Mental model already exists for entity %s (%s), skipping",
                    entity_id, entity_name,
                )
                continue

            # Reflect で content を生成（軽量化: max_iterations=5）
            source_query = _build_source_query(entity_name, mission)
            result = await reflect(
                pool,
                bank_id,
                source_query,
                max_iterations=5,
            )

            answer = result.get("answer", "")
            if len(answer) < 50:
                logger.debug(
                    "Generated content too short for entity %s (%d chars), skipping",
                    entity_name, len(answer),
                )
                continue

            # Mental Model を作成
            await create_mental_model(
                pool,
                bank_id,
                name=entity_name,
                content=answer,
                description=f"{entity_name}に関する自動生成サマリ",
                source_query=source_query,
                trigger={"refresh_after_consolidation": True},
                entity_id=entity_id,
            )
            generated += 1
            logger.info(
                "Auto-generated mental model for entity %s (%s) in bank %s",
                entity_id, entity_name, bank_id,
            )
        except Exception:
            logger.exception(
                "Failed to generate mental model for entity %s (%s)",
                entity_id, entity_name,
            )

    return generated


async def _find_generation_candidates(
    pool: asyncpg.Pool,
    bank_id: str,
    affected_observation_ids: list[str],
) -> list[dict]:
    """自動生成の候補エンティティを検索する

    条件:
    - affected_observation_ids に紐づくエンティティ
    - bank 内の Observation リンク数 >= MIN_OBSERVATIONS_FOR_GENERATION
    - まだ Mental Model が entity_id で紐づいていない

    Returns:
        候補エンティティのリスト（obs_count 降順、最大 MAX_GENERATION_PER_CONSOLIDATION 件）
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH affected_entities AS (
                SELECT DISTINCT ue.entity_id
                FROM unit_entities ue
                WHERE ue.unit_id = ANY($1::uuid[])
            ),
            entity_obs_counts AS (
                SELECT ue.entity_id, e.canonical_name, COUNT(DISTINCT ue.unit_id) AS obs_count
                FROM unit_entities ue
                JOIN memory_units mu ON ue.unit_id = mu.id
                JOIN entities e ON ue.entity_id = e.id
                WHERE mu.bank_id = $2::uuid
                  AND mu.fact_type = 'observation'
                  AND ue.entity_id IN (SELECT entity_id FROM affected_entities)
                GROUP BY ue.entity_id, e.canonical_name
                HAVING COUNT(DISTINCT ue.unit_id) >= $3
            )
            SELECT eoc.entity_id, eoc.canonical_name, eoc.obs_count
            FROM entity_obs_counts eoc
            LEFT JOIN mental_models mm
                ON mm.entity_id = eoc.entity_id AND mm.bank_id = $2::uuid
            WHERE mm.id IS NULL
            ORDER BY eoc.obs_count DESC
            LIMIT $4
            """,
            affected_observation_ids,
            bank_id,
            MIN_OBSERVATIONS_FOR_GENERATION,
            MAX_GENERATION_PER_CONSOLIDATION,
        )

    return [dict(r) for r in rows]


async def _check_mental_model_exists(
    pool: asyncpg.Pool,
    bank_id: str,
    entity_id: str,
    entity_name: str,
) -> bool:
    """Mental Model の重複をチェックする

    1. entity_id による厳密チェック（DB）
    2. pg_trgm の similarity() による名前類似度チェック（手動作成との重複防止）

    Returns:
        True なら既に存在（スキップすべき）
    """
    async with pool.acquire() as conn:
        # 1. entity_id チェック
        row = await conn.fetchrow(
            """
            SELECT id FROM mental_models
            WHERE bank_id = $1::uuid AND entity_id = $2::uuid
            LIMIT 1
            """,
            bank_id,
            entity_id,
        )
        if row:
            return True

        # 2. 名前の文字列類似度チェック（pg_trgm）
        row = await conn.fetchrow(
            """
            SELECT id FROM mental_models
            WHERE bank_id = $1::uuid
              AND similarity(name, $2) >= 0.8
            LIMIT 1
            """,
            bank_id,
            entity_name,
        )
        if row:
            return True

    return False


def _build_source_query(entity_name: str, mission: str) -> str:
    """Reflect 用の source_query を生成する"""
    if mission:
        return (
            f"{entity_name}について、これまでの全ての記憶から包括的にまとめてください。"
            f"ミッション「{mission}」の観点を含めてください。"
        )
    return f"{entity_name}について、これまでの全ての記憶から包括的にまとめてください。"
