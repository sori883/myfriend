"""グラフリンク構築モジュール

Retain パイプライン後に memory_units 間のグラフエッジを自動構築する。
3種のリンク（temporal/semantic/entity）を作成し、
entity_cooccurrences テーブルを更新する。

causes/caused_by リンクは将来タスク（LLM による因果関係抽出が必要）。
"""

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

logger = logging.getLogger(__name__)

# ---------- 定数 ----------

TEMPORAL_WINDOW_HOURS = 24
TEMPORAL_WEIGHT_MIN = 0.3
MAX_TEMPORAL_LINKS_PER_UNIT = 10
MAX_TEMPORAL_CANDIDATES = 200

SEMANTIC_TOP_K = 5
SEMANTIC_THRESHOLD = 0.7

MAX_LINKS_PER_ENTITY = 50

_INSERT_BATCH_SIZE = 500

# ON CONFLICT 用の定数（NULL entity_id をセンチネル UUID に変換）
_NIL_UUID = "00000000-0000-0000-0000-000000000000"

_INSERT_LINK_SQL = f"""\
INSERT INTO memory_links (bank_id, from_unit_id, to_unit_id, link_type, weight, entity_id)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::uuid)
ON CONFLICT (from_unit_id, to_unit_id, link_type,
             COALESCE(entity_id, '{_NIL_UUID}'::uuid))
DO NOTHING"""


# ---------- データ構造 ----------


@dataclass(frozen=True)
class LinkRecord:
    """挿入するリンク1件"""

    from_unit_id: str
    to_unit_id: str
    link_type: str
    weight: float
    entity_id: str | None


# ---------- 公開 API ----------


async def build_links_for_units(
    pool: asyncpg.Pool,
    bank_id: str,
    unit_ids: list[str],
    embeddings: list[list[float]],
) -> dict:
    """新規保存ユニットに対してグラフエッジを構築する

    Retain パイプラインのメイントランザクション外で呼び出す。
    リンク構築の失敗はファクト保存に影響しない。

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        unit_ids: 新規保存された memory_units の ID リスト
        embeddings: 対応する Embedding ベクトルのリスト

    Returns:
        {"temporal": int, "semantic": int, "entity": int, "cooccurrences": int}
    """
    if not unit_ids:
        return {"temporal": 0, "semantic": 0, "entity": 0, "cooccurrences": 0}

    async with pool.acquire() as conn:
        temporal_links = await _build_temporal_links(conn, bank_id, unit_ids)
        semantic_links = await _build_semantic_links(
            conn, bank_id, unit_ids, embeddings
        )
        entity_links = await _build_entity_links(conn, bank_id, unit_ids)

        all_links = [*temporal_links, *semantic_links, *entity_links]
        await _insert_links_batch(conn, bank_id, all_links)

        cooccurrence_count = await _update_entity_cooccurrences(
            conn, bank_id, unit_ids
        )

    stats = {
        "temporal": len(temporal_links),
        "semantic": len(semantic_links),
        "entity": len(entity_links),
        "cooccurrences": cooccurrence_count,
    }

    logger.info(
        "Graph links built for %d units: temporal=%d, semantic=%d, entity=%d, cooccurrences=%d",
        len(unit_ids),
        stats["temporal"],
        stats["semantic"],
        stats["entity"],
        stats["cooccurrences"],
    )

    return stats


# ---------- ユーティリティ ----------


def _normalize_datetime(dt: datetime | None) -> datetime | None:
    """タイムゾーンなし datetime を UTC に正規化する"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _best_event_time(row: asyncpg.Record) -> datetime | None:
    """ユニットの最適なイベント時刻を返す"""
    for col in ("event_date", "occurred_start", "mentioned_at"):
        val = row.get(col)
        if val is not None:
            return _normalize_datetime(val)
    return None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """numpy なしでコサイン類似度を計算する"""
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# ---------- Temporal リンク ----------


async def _fetch_temporal_candidates(
    conn: asyncpg.Connection,
    bank_id: str,
    unit_ids: list[str],
    min_date: datetime,
    max_date: datetime,
) -> list[asyncpg.Record]:
    """temporal リンク候補を取得する（最新 MAX_TEMPORAL_CANDIDATES 件）"""
    return await conn.fetch(
        """
        SELECT id, event_date, occurred_start, mentioned_at
        FROM memory_units
        WHERE bank_id = $1::uuid
          AND id != ALL($2::uuid[])
          AND (
              (event_date IS NOT NULL AND event_date BETWEEN $3 AND $4)
              OR (occurred_start IS NOT NULL AND occurred_start BETWEEN $3 AND $4)
              OR (mentioned_at IS NOT NULL AND mentioned_at BETWEEN $3 AND $4)
          )
        ORDER BY COALESCE(event_date, occurred_start, mentioned_at) DESC
        LIMIT $5
        """,
        bank_id,
        unit_ids,
        min_date,
        max_date,
        MAX_TEMPORAL_CANDIDATES,
    )


def _match_temporal_candidates(
    unit_times: dict[str, datetime],
    candidates: list[asyncpg.Record],
) -> list[LinkRecord]:
    """新規ユニット ↔ 既存候補の temporal リンクを生成する"""
    links: list[LinkRecord] = []
    for uid, ut in unit_times.items():
        matched = 0
        for cand in candidates:
            ct = _best_event_time(cand)
            if ct is None:
                continue
            diff_hours = abs((ut - ct).total_seconds()) / 3600
            if diff_hours > TEMPORAL_WINDOW_HOURS:
                continue
            weight = max(
                TEMPORAL_WEIGHT_MIN, 1.0 - (diff_hours / TEMPORAL_WINDOW_HOURS)
            )
            cid = str(cand["id"])
            links.append(LinkRecord(uid, cid, "temporal", weight, None))
            links.append(LinkRecord(cid, uid, "temporal", weight, None))
            matched += 1
            if matched >= MAX_TEMPORAL_LINKS_PER_UNIT:
                break
    return links


def _match_temporal_within_batch(
    unit_times: dict[str, datetime],
) -> list[LinkRecord]:
    """新規ユニット同士の temporal リンクを生成する"""
    links: list[LinkRecord] = []
    items = list(unit_times.items())
    for i, (uid1, ut1) in enumerate(items):
        for uid2, ut2 in items[i + 1 :]:
            diff_hours = abs((ut1 - ut2).total_seconds()) / 3600
            if diff_hours > TEMPORAL_WINDOW_HOURS:
                continue
            weight = max(
                TEMPORAL_WEIGHT_MIN, 1.0 - (diff_hours / TEMPORAL_WINDOW_HOURS)
            )
            links.append(LinkRecord(uid1, uid2, "temporal", weight, None))
            links.append(LinkRecord(uid2, uid1, "temporal", weight, None))
    return links


async def _build_temporal_links(
    conn: asyncpg.Connection,
    bank_id: str,
    unit_ids: list[str],
) -> list[LinkRecord]:
    """時間的近接に基づく temporal リンクを構築する"""
    rows = await conn.fetch(
        """
        SELECT id, event_date, occurred_start, mentioned_at
        FROM memory_units
        WHERE id = ANY($1::uuid[])
        """,
        unit_ids,
    )

    unit_times: dict[str, datetime] = {}
    for row in rows:
        best = _best_event_time(row)
        if best is not None:
            unit_times[str(row["id"])] = best

    if not unit_times:
        return []

    all_times = list(unit_times.values())
    window = timedelta(hours=TEMPORAL_WINDOW_HOURS)
    min_date = min(all_times) - window
    max_date = max(all_times) + window

    candidates = await _fetch_temporal_candidates(
        conn, bank_id, unit_ids, min_date, max_date
    )

    links = _match_temporal_candidates(unit_times, candidates)
    links.extend(_match_temporal_within_batch(unit_times))
    return links


# ---------- Semantic リンク ----------


async def _build_semantic_links(
    conn: asyncpg.Connection,
    bank_id: str,
    unit_ids: list[str],
    embeddings: list[list[float]],
) -> list[LinkRecord]:
    """コサイン類似度に基づく semantic リンクを構築する"""
    links: list[LinkRecord] = []

    # 新規ユニット ↔ 既存ユニット（HNSW インデックス活用）
    for uid, emb in zip(unit_ids, embeddings, strict=True):
        rows = await conn.fetch(
            """
            SELECT id, 1 - (embedding <=> $1::vector) AS similarity
            FROM memory_units
            WHERE bank_id = $2::uuid
              AND embedding IS NOT NULL
              AND id != $3::uuid
              AND (1 - (embedding <=> $1::vector)) >= $4
            ORDER BY embedding <=> $1::vector
            LIMIT $5
            """,
            emb,
            bank_id,
            uid,
            SEMANTIC_THRESHOLD,
            SEMANTIC_TOP_K,
        )
        for row in rows:
            target_id = str(row["id"])
            sim = float(row["similarity"])
            links.append(LinkRecord(uid, target_id, "semantic", sim, None))
            links.append(LinkRecord(target_id, uid, "semantic", sim, None))

    # 新規ユニット同士（Python で計算、DB ラウンドトリップ不要）
    if len(unit_ids) > 1:
        for i, (uid1, emb1) in enumerate(
            zip(unit_ids, embeddings, strict=True)
        ):
            for uid2, emb2 in zip(
                unit_ids[i + 1 :], embeddings[i + 1 :], strict=True
            ):
                sim = _cosine_similarity(emb1, emb2)
                if sim >= SEMANTIC_THRESHOLD:
                    links.append(
                        LinkRecord(uid1, uid2, "semantic", sim, None)
                    )
                    links.append(
                        LinkRecord(uid2, uid1, "semantic", sim, None)
                    )

    return links


# ---------- Entity リンク ----------


async def _build_entity_links(
    conn: asyncpg.Connection,
    bank_id: str,
    unit_ids: list[str],
) -> list[LinkRecord]:
    """共有エンティティに基づく entity リンクを構築する"""
    ue_rows = await conn.fetch(
        """
        SELECT unit_id, entity_id
        FROM unit_entities
        WHERE unit_id = ANY($1::uuid[])
        """,
        unit_ids,
    )

    if not ue_rows:
        return []

    entity_to_new_units: dict[str, list[str]] = {}
    all_entity_ids: set[str] = set()
    for row in ue_rows:
        eid = str(row["entity_id"])
        uid = str(row["unit_id"])
        entity_to_new_units.setdefault(eid, []).append(uid)
        all_entity_ids.add(eid)

    # 既存ユニットを created_at DESC で取得（最新を優先）
    existing_rows = await conn.fetch(
        """
        SELECT ue.entity_id, ue.unit_id
        FROM unit_entities ue
        JOIN memory_units mu ON ue.unit_id = mu.id
        WHERE ue.entity_id = ANY($1::uuid[])
          AND ue.unit_id != ALL($2::uuid[])
        ORDER BY mu.created_at DESC
        """,
        list(all_entity_ids),
        unit_ids,
    )

    entity_to_existing: dict[str, list[str]] = {}
    for row in existing_rows:
        eid = str(row["entity_id"])
        entity_to_existing.setdefault(eid, []).append(str(row["unit_id"]))

    links: list[LinkRecord] = []

    for eid, new_units in entity_to_new_units.items():
        existing_units = entity_to_existing.get(eid, [])

        # 新規ユニット ↔ 既存ユニット（最新 MAX_LINKS_PER_ENTITY 件）
        for nuid in new_units:
            for euid in existing_units[:MAX_LINKS_PER_ENTITY]:
                links.append(LinkRecord(nuid, euid, "entity", 1.0, eid))
                links.append(LinkRecord(euid, nuid, "entity", 1.0, eid))

        # 新規ユニット同士
        for i, uid1 in enumerate(new_units):
            for uid2 in new_units[i + 1 :]:
                links.append(LinkRecord(uid1, uid2, "entity", 1.0, eid))
                links.append(LinkRecord(uid2, uid1, "entity", 1.0, eid))

    return links


# ---------- Entity Cooccurrences ----------


async def _update_entity_cooccurrences(
    conn: asyncpg.Connection,
    bank_id: str,
    unit_ids: list[str],
) -> int:
    """同一ユニット内で共起するエンティティペアを更新する"""
    rows = await conn.fetch(
        """
        SELECT unit_id, array_agg(entity_id ORDER BY entity_id) AS entity_ids
        FROM unit_entities
        WHERE unit_id = ANY($1::uuid[])
        GROUP BY unit_id
        HAVING COUNT(*) >= 2
        """,
        unit_ids,
    )

    if not rows:
        return 0

    pairs: set[tuple[str, str]] = set()
    for row in rows:
        eids = [str(eid) for eid in row["entity_ids"]]
        for i, eid1 in enumerate(eids):
            for eid2 in eids[i + 1 :]:
                pairs.add((eid1, eid2))

    if not pairs:
        return 0

    await conn.executemany(
        """
        INSERT INTO entity_cooccurrences (entity_id_1, entity_id_2, bank_id, cooccurrence_count, last_cooccurred)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 1, NOW())
        ON CONFLICT (entity_id_1, entity_id_2) DO UPDATE SET
            cooccurrence_count = entity_cooccurrences.cooccurrence_count + 1,
            last_cooccurred = NOW()
        """,
        [(p[0], p[1], bank_id) for p in pairs],
    )

    return len(pairs)


# ---------- バッチ挿入 ----------


async def _insert_links_batch(
    conn: asyncpg.Connection,
    bank_id: str,
    links: list[LinkRecord],
) -> None:
    """リンクをバッチ挿入する（ON CONFLICT DO NOTHING）"""
    if not links:
        return

    for start in range(0, len(links), _INSERT_BATCH_SIZE):
        batch = links[start : start + _INSERT_BATCH_SIZE]
        await conn.executemany(
            _INSERT_LINK_SQL,
            [
                (bank_id, lk.from_unit_id, lk.to_unit_id, lk.link_type, lk.weight, lk.entity_id)
                for lk in batch
            ],
        )
