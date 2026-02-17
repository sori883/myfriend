"""Retain パイプライン

会話テキストからファクトを抽出し、Embedding 付きで DB に永続化する。
重複チェック・エンティティ解決を含む完全なパイプライン。
"""

import logging
from datetime import timedelta

import asyncpg

from memory.embedding import generate_embeddings
from memory.entity import resolve_entities
from memory.extraction import Fact, extract_facts

logger = logging.getLogger(__name__)

DUPLICATE_SIMILARITY_THRESHOLD = 0.9
DUPLICATE_BUCKET_HOURS = 12


def _build_embedding_text(fact: Fact) -> str:
    """Embedding 用のテキストを生成する（日時情報で拡張）"""
    text = fact.text
    if fact.event_date:
        text += f" (happened on {fact.event_date.strftime('%Y-%m-%d')})"
    return text


async def _check_duplicate_event(
    conn: asyncpg.Connection,
    bank_id: str,
    embedding: list[float],
    fact: Fact,
) -> bool:
    """12時間バケット + コサイン類似度で event ファクトの重複チェック"""
    bucket_start = fact.event_date.replace(
        hour=(fact.event_date.hour // DUPLICATE_BUCKET_HOURS) * DUPLICATE_BUCKET_HOURS,
        minute=0,
        second=0,
        microsecond=0,
    )
    bucket_end = bucket_start + timedelta(hours=DUPLICATE_BUCKET_HOURS)

    row = await conn.fetchrow(
        """
        SELECT id, 1 - (embedding <=> $1::vector) AS similarity
        FROM memory_units
        WHERE bank_id = $2::uuid
          AND event_date >= $3
          AND event_date < $4
          AND embedding IS NOT NULL
          AND (1 - (embedding <=> $1::vector)) >= $5
        LIMIT 1
        """,
        embedding,
        bank_id,
        bucket_start,
        bucket_end,
        DUPLICATE_SIMILARITY_THRESHOLD,
    )
    return row is not None


async def _check_duplicate_conversation(
    conn: asyncpg.Connection,
    bank_id: str,
    embedding: list[float],
) -> bool:
    """コサイン類似度のみで conversation ファクトの重複チェック"""
    row = await conn.fetchrow(
        """
        SELECT id, 1 - (embedding <=> $1::vector) AS similarity
        FROM memory_units
        WHERE bank_id = $2::uuid
          AND fact_kind = 'conversation'
          AND embedding IS NOT NULL
          AND (1 - (embedding <=> $1::vector)) >= $3
        LIMIT 1
        """,
        embedding,
        bank_id,
        DUPLICATE_SIMILARITY_THRESHOLD,
    )
    return row is not None


async def _check_duplicate(
    conn: asyncpg.Connection,
    bank_id: str,
    embedding: list[float],
    fact: Fact,
) -> bool:
    """ファクトの重複をチェックする"""
    if fact.event_date is not None:
        is_dup = await _check_duplicate_event(conn, bank_id, embedding, fact)
    else:
        is_dup = await _check_duplicate_conversation(conn, bank_id, embedding)

    if is_dup:
        logger.debug("Duplicate detected: %s", fact.text[:80])
    return is_dup


async def _insert_memory_unit(
    conn: asyncpg.Connection,
    bank_id: str,
    fact: Fact,
    embedding: list[float],
    context: str | None,
) -> str:
    """memory_units にファクトを INSERT し、ID を返す"""
    row = await conn.fetchrow(
        """
        INSERT INTO memory_units (
            bank_id, text, context, embedding,
            fact_type, fact_kind,
            what, who, when_description, where_description, why_description,
            event_date, occurred_start, occurred_end, mentioned_at
        ) VALUES (
            $1::uuid, $2, $3, $4::vector,
            $5, $6,
            $7, $8, $9, $10, $11,
            $12, $13, $14, NOW()
        )
        RETURNING id
        """,
        bank_id,
        fact.text,
        context,
        embedding,
        fact.fact_type,
        fact.fact_kind,
        fact.what,
        list(fact.who) if fact.who else None,
        fact.when_description,
        fact.where_description,
        fact.why_description,
        fact.event_date,
        fact.occurred_start,
        fact.occurred_end,
    )
    return str(row["id"])


async def _insert_unit_entities(
    conn: asyncpg.Connection,
    unit_id: str,
    entity_ids: list[str],
) -> None:
    """unit_entities 中間テーブルに INSERT"""
    if not entity_ids:
        return

    await conn.executemany(
        """
        INSERT INTO unit_entities (unit_id, entity_id)
        VALUES ($1::uuid, $2::uuid)
        ON CONFLICT DO NOTHING
        """,
        [(unit_id, eid) for eid in entity_ids],
    )


async def retain(
    pool: asyncpg.Pool,
    bank_id: str,
    content: str,
    context: str = "",
) -> dict:
    """Retain パイプラインを実行する

    処理フロー:
    1. LLM ファクト抽出
    2. Embedding 生成（バッチ）
    3. 重複チェック
    4. DB トランザクション（エンティティ解決 + INSERT）
    5. Consolidation ジョブキュー（Phase 2 スタブ）

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        content: 会話テキスト
        context: 追加コンテキスト

    Returns:
        保存結果
    """
    # 1. ファクト抽出
    facts = await extract_facts(content, context)
    if not facts:
        return {"stored": 0, "duplicates": 0, "fact_ids": []}

    # 2. Embedding 生成
    embedding_texts = [_build_embedding_text(f) for f in facts]
    embeddings = await generate_embeddings(embedding_texts)

    # 3-4. 重複チェック + DB 保存（トランザクション内）
    stored_ids = []
    duplicate_count = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            for fact, embedding in zip(facts, embeddings, strict=True):
                # 重複チェック
                is_dup = await _check_duplicate(conn, bank_id, embedding, fact)
                if is_dup:
                    duplicate_count += 1
                    continue

                # ファクト INSERT
                unit_id = await _insert_memory_unit(
                    conn, bank_id, fact, embedding, context or None
                )

                # エンティティ解決 + リンク
                if fact.who:
                    entities = await resolve_entities(
                        conn, bank_id, list(fact.who)
                    )
                    entity_ids = [e.entity_id for e in entities]
                    await _insert_unit_entities(conn, unit_id, entity_ids)

                stored_ids.append(unit_id)

    # 5. Consolidation ジョブキュー（Phase 2 で実装）
    if stored_ids:
        logger.info(
            "Consolidation job queued for %d new facts (stub - Phase 2)",
            len(stored_ids),
        )

    logger.info(
        "Retain complete: stored=%d, duplicates=%d",
        len(stored_ids),
        duplicate_count,
    )

    return {
        "stored": len(stored_ids),
        "duplicates": duplicate_count,
        "fact_ids": stored_ids,
    }
