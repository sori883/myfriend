"""Retain パイプライン

会話テキストからファクトを抽出し、Embedding 付きで DB に永続化する。
重複チェック・エンティティ解決を含む完全なパイプライン。
"""

import logging
import re
import uuid as _uuid
from datetime import timedelta

import asyncpg

from memory.embedding import generate_embeddings
from memory.entity import ResolvedEntity, resolve_entities
from memory.extraction import Fact, extract_facts
from memory.graph import build_links_for_units

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


_SELF_INTRO_PATTERNS = [
    re.compile(
        r"(?:私|僕|俺|自分|わたし|ぼく|おれ)(?:の(?:名前|こと))?[はって、]"
        r"(.+?)(?:です|だ|だよ|と申します|って言います|と言う|っていう)"
    ),
    re.compile(r"名前は(.+?)(?:です|だ|と申します)"),
    re.compile(
        r"(?:私|僕|俺|自分|わたし|ぼく|おれ)(?:のこと(?:を|は))?"
        r"(.+?)(?:と呼んで|って呼んで|と呼んでください)"
    ),
]


def _detect_owner_name(fact_text: str, who_names: tuple[str, ...]) -> str | None:
    """ファクトテキストから自己紹介の名前を検出する。

    who に含まれる名前のうち、自己紹介パターンにマッチするものを返す。
    完全一致を優先し、フォールバックで包含チェック（2文字以上）を行う。
    """
    for pattern in _SELF_INTRO_PATTERNS:
        match = pattern.search(fact_text)
        if match:
            detected = match.group(1).strip()
            if not detected:
                continue
            # 完全一致を優先
            for name in who_names:
                if name == detected:
                    return name
            # 包含チェック（最低2文字以上の共通部分）
            for name in who_names:
                if len(name) >= 2 and (name in detected or detected in name):
                    return name
    return None


async def _update_owner_name(
    conn: asyncpg.Connection,
    bank_id: str,
    new_name: str,
    resolved_entities: list[ResolvedEntity],
    owner_entity_id: _uuid.UUID,
) -> None:
    """owner entity の canonical_name を更新し、重複 entity があればマージする"""
    # new_name に対応する別 entity が作られていたらマージ
    new_entity = next(
        (
            e
            for e in resolved_entities
            if e.canonical_name == new_name
            and str(e.entity_id) != str(owner_entity_id)
        ),
        None,
    )

    if new_entity is not None:
        new_eid = _uuid.UUID(new_entity.entity_id)
        # unit_entities の参照を owner entity に付け替え
        await conn.execute(
            """UPDATE unit_entities SET entity_id = $1
               WHERE entity_id = $2
               AND unit_id NOT IN (
                   SELECT unit_id FROM unit_entities WHERE entity_id = $1
               )""",
            owner_entity_id,
            new_eid,
        )
        await conn.execute(
            "DELETE FROM unit_entities WHERE entity_id = $1",
            new_eid,
        )
        # entity_cooccurrences は制約違反を避けるため削除
        # （後続の build_links_for_units で再構築される）
        await conn.execute(
            """DELETE FROM entity_cooccurrences
               WHERE bank_id = $1::uuid
               AND (entity_id_1 = $2 OR entity_id_2 = $2)""",
            bank_id,
            new_eid,
        )
        # memory_links の参照を付け替え（ON DELETE SET NULL を防ぐ）
        await conn.execute(
            """UPDATE memory_links SET entity_id = $1
               WHERE entity_id = $2 AND bank_id = $3::uuid""",
            owner_entity_id,
            new_eid,
            bank_id,
        )
        # 重複 entity を削除
        await conn.execute("DELETE FROM entities WHERE id = $1", new_eid)

    # owner entity の canonical_name を更新
    await conn.execute(
        "UPDATE entities SET canonical_name = $1 WHERE id = $2",
        new_name,
        owner_entity_id,
    )
    logger.info(
        "Updated owner entity name to '%s' for bank %s", new_name, bank_id[:8]
    )


async def ensure_bank_with_owner(pool: asyncpg.Pool, bank_id: str) -> None:
    """bank + owner entity が存在することを保証する

    並列パイプライン実行前に呼び出し、レース条件を防ぐ。
    ON CONFLICT DO NOTHING で冪等。
    """
    bank_uuid = _uuid.UUID(bank_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            bank_created = await conn.fetchrow(
                """
                INSERT INTO banks (id, name)
                VALUES ($1, $2)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                bank_uuid,
                f"auto-{bank_id[:8]}",
            )
            if bank_created is not None:
                entity_row = await conn.fetchrow(
                    """
                    INSERT INTO entities (bank_id, canonical_name, entity_type)
                    VALUES ($1, 'ご主人様', 'person')
                    RETURNING id
                    """,
                    bank_uuid,
                )
                await conn.execute(
                    "UPDATE banks SET owner_entity_id = $1 WHERE id = $2",
                    entity_row["id"],
                    bank_uuid,
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
    stored_embeddings: list[list[float]] = []
    duplicate_count = 0

    # bank + owner entity を事前確保（冪等）
    await ensure_bank_with_owner(pool, bank_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            bank_uuid = _uuid.UUID(bank_id)

            # owner_entity_id を1回だけ取得（名前学習用）
            owner_eid_row = await conn.fetchrow(
                "SELECT owner_entity_id FROM banks WHERE id = $1",
                bank_uuid,
            )
            owner_entity_id = (
                owner_eid_row["owner_entity_id"]
                if owner_eid_row and owner_eid_row["owner_entity_id"]
                else None
            )

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
                        conn, bank_id, list(fact.who),
                        event_date=fact.event_date,
                    )
                    entity_ids = [e.entity_id for e in entities]
                    await _insert_unit_entities(conn, unit_id, entity_ids)

                    # 自己紹介検出: owner entity の名前を更新
                    if owner_entity_id is not None:
                        detected_name = _detect_owner_name(fact.text, fact.who)
                        if detected_name is not None:
                            await _update_owner_name(
                                conn, bank_id, detected_name, entities,
                                owner_entity_id,
                            )

                stored_ids.append(unit_id)
                stored_embeddings.append(embedding)

    # 5. グラフリンク構築（メイントランザクション外）
    link_stats: dict = {}
    if stored_ids:
        try:
            link_stats = await build_links_for_units(
                pool, bank_id, stored_ids, stored_embeddings
            )
        except Exception:
            logger.exception("Failed to build graph links (non-fatal)")

    logger.info(
        "Retain complete: stored=%d, duplicates=%d",
        len(stored_ids),
        duplicate_count,
    )

    return {
        "stored": len(stored_ids),
        "duplicates": duplicate_count,
        "fact_ids": stored_ids,
        "links": link_stats,
    }
