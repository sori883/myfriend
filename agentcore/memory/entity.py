"""エンティティ抽出・解決モジュール

ファクトの who フィールドからエンティティを抽出し、3要素スコアリングで
既存エンティティとの照合・統合を行う。

スコアリング要素:
  - 名前類似度 (SequenceMatcher): 重み 0.5
  - 共起エンティティ: 重み 0.3
  - 時間的近接性 (7日ウィンドウ): 重み 0.2
閾値: 0.6

バッチ処理によりエンティティ数に依存しない定数回の DB クエリで動作する。
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher

import asyncpg

logger = logging.getLogger(__name__)

# ---------- 定数 ----------

SCORE_THRESHOLD = 0.6
WEIGHT_NAME = 0.5
WEIGHT_COOCCURRENCE = 0.3
WEIGHT_TEMPORAL = 0.2
TEMPORAL_WINDOW_DAYS = 7
_SECONDS_PER_DAY = 86400


# ---------- データクラス ----------


@dataclass(frozen=True)
class ResolvedEntity:
    """解決済みエンティティ"""

    entity_id: str
    canonical_name: str
    entity_type: str
    is_new: bool


# ---------- バッチ取得 ----------


async def _fetch_all_candidates(
    conn: asyncpg.Connection,
    bank_id: str,
) -> list[asyncpg.Record]:
    """bank 内の全エンティティを取得する（バッチ用）"""
    return await conn.fetch(
        """SELECT id, canonical_name, last_seen, mention_count
           FROM entities
           WHERE bank_id = $1::uuid""",
        bank_id,
    )


async def _fetch_cooccurrence_map(
    conn: asyncpg.Connection,
    bank_id: str,
) -> dict[str, set[str]]:
    """エンティティID → 共起エンティティ名(lowercase)の集合を構築する"""
    rows = await conn.fetch(
        """SELECT ec.entity_id_1, ec.entity_id_2,
                  e1.canonical_name AS name1, e2.canonical_name AS name2
           FROM entity_cooccurrences ec
           JOIN entities e1 ON ec.entity_id_1 = e1.id
           JOIN entities e2 ON ec.entity_id_2 = e2.id
           WHERE ec.bank_id = $1::uuid""",
        bank_id,
    )
    cooc: dict[str, set[str]] = {}
    for row in rows:
        id1, id2 = str(row["entity_id_1"]), str(row["entity_id_2"])
        n1, n2 = row["name1"].lower(), row["name2"].lower()
        cooc.setdefault(id1, set()).add(n2)
        cooc.setdefault(id2, set()).add(n1)
    return cooc


# ---------- スコアリング ----------


def _score_candidate(
    entity_text: str,
    canonical_name: str,
    candidate_id: str,
    nearby_names_lower: set[str],
    cooc_map: dict[str, set[str]],
    event_date: datetime | None,
    last_seen: datetime | None,
) -> float:
    """3要素スコアを計算する

    1. 名前類似度 (0-0.5): SequenceMatcher ratio
    2. 共起スコア (0-0.3): nearby entities との重なり率
    3. 時間的近接性 (0-0.2): 7日ウィンドウ内での線形減衰
    """
    entity_lower = entity_text.lower()
    canonical_lower = canonical_name.lower()

    # 大文字小文字無視の完全一致は常にマッチ
    if entity_lower == canonical_lower:
        return 1.0

    name_sim = SequenceMatcher(None, entity_lower, canonical_lower).ratio()
    score = name_sim * WEIGHT_NAME

    if nearby_names_lower:
        co_entities = cooc_map.get(candidate_id, set())
        overlap = len(nearby_names_lower & co_entities)
        score += (overlap / len(nearby_names_lower)) * WEIGHT_COOCCURRENCE

    if event_date is not None and last_seen is not None:
        ed = event_date if event_date.tzinfo else event_date.replace(tzinfo=UTC)
        ls = last_seen if last_seen.tzinfo else last_seen.replace(tzinfo=UTC)
        days_diff = abs((ed - ls).total_seconds()) / _SECONDS_PER_DAY
        if days_diff < TEMPORAL_WINDOW_DAYS:
            temporal = max(0.0, 1.0 - days_diff / TEMPORAL_WINDOW_DAYS)
            score += temporal * WEIGHT_TEMPORAL

    return score


# ---------- バッチ書き込み ----------


async def _batch_create_entities(
    conn: asyncpg.Connection,
    bank_id: str,
    names: list[str],
) -> list[tuple[str, str]]:
    """新規エンティティをバッチ作成し (entity_id, canonical_name) を返す"""
    if not names:
        return []
    rows = await conn.fetch(
        """INSERT INTO entities (bank_id, canonical_name, entity_type)
           SELECT $1::uuid, unnest($2::text[]), 'person'
           ON CONFLICT (bank_id, LOWER(canonical_name)) DO UPDATE
               SET mention_count = entities.mention_count + 1,
                   last_seen = NOW()
           RETURNING id, canonical_name""",
        bank_id,
        names,
    )
    return [(str(r["id"]), r["canonical_name"]) for r in rows]


async def _batch_update_stats(
    conn: asyncpg.Connection,
    entity_ids: list[str],
) -> None:
    """既存マッチしたエンティティの統計をバッチ更新する"""
    if not entity_ids:
        return
    await conn.execute(
        """UPDATE entities
           SET mention_count = mention_count + 1, last_seen = NOW()
           WHERE id = ANY($1::text[]::uuid[])""",
        entity_ids,
    )


# ---------- 公開 API ----------


async def resolve_entities(
    conn: asyncpg.Connection,
    bank_id: str,
    names: list[str],
    *,
    event_date: datetime | None = None,
) -> list[ResolvedEntity]:
    """名前リストからエンティティを解決する

    3要素スコアリング（名前類似度 + 共起 + 時間的近接性）でマッチングする。
    バッチ処理により定数回の DB クエリで動作する。

    Args:
        conn: DB コネクション（トランザクション内で使用）
        bank_id: メモリバンクID
        names: エンティティ名のリスト
        event_date: ファクトのイベント日時（時間的近接性スコア用）

    Returns:
        解決済みエンティティのリスト
    """
    if not names:
        return []

    # 1. バッチ取得（定数 2 クエリ）
    candidates = await _fetch_all_candidates(conn, bank_id)
    cooc_map = await _fetch_cooccurrence_map(conn, bank_id)

    all_names_lower = {n.strip().lower() for n in names if n.strip()}

    resolved: list[ResolvedEntity] = []
    seen_names: dict[str, ResolvedEntity] = {}
    matched_ids: list[str] = []
    new_names: list[str] = []

    # 2. インメモリスコアリング
    for name in names:
        name = name.strip()
        if not name:
            continue

        lower_name = name.lower()
        if lower_name in seen_names:
            resolved.append(seen_names[lower_name])
            continue

        nearby = all_names_lower - {lower_name}

        best_score, best_candidate = 0.0, None
        for cand in candidates:
            s = _score_candidate(
                name, cand["canonical_name"], str(cand["id"]),
                nearby, cooc_map, event_date, cand["last_seen"],
            )
            if s > best_score:
                best_score, best_candidate = s, cand

        if best_candidate is not None and best_score >= SCORE_THRESHOLD:
            entity = ResolvedEntity(
                entity_id=str(best_candidate["id"]),
                canonical_name=best_candidate["canonical_name"],
                entity_type="person",
                is_new=False,
            )
            matched_ids.append(entity.entity_id)
        else:
            entity = ResolvedEntity(
                entity_id="",
                canonical_name=name,
                entity_type="person",
                is_new=True,
            )
            new_names.append(name)

        seen_names[lower_name] = entity
        resolved.append(entity)

    # 3. バッチ INSERT（新規エンティティ）
    if new_names:
        created = await _batch_create_entities(conn, bank_id, new_names)
        created_map = {cn.lower(): eid for eid, cn in created}
        resolved = [
            ResolvedEntity(
                entity_id=created_map.get(e.canonical_name.lower(), e.entity_id),
                canonical_name=e.canonical_name,
                entity_type=e.entity_type,
                is_new=e.is_new,
            ) if e.is_new else e
            for e in resolved
        ]

    # 4. バッチ UPDATE（既存エンティティの統計）
    await _batch_update_stats(conn, matched_ids)

    new_count = sum(1 for e in resolved if e.is_new)
    logger.info(
        "Resolved %d entities (%d new, %d existing)",
        len(resolved),
        new_count,
        len(resolved) - new_count,
    )
    return resolved
