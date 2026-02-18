"""時間検索モジュール

時間範囲に基づく記憶検索。2フェーズで動作する:
- Phase 1: occurred_start/occurred_end/mentioned_at による直接時間範囲マッチ
- Phase 2: temporal/causes/caused_by リンクを通じた拡散伝播
"""

import logging
from datetime import UTC, datetime

import asyncpg

logger = logging.getLogger(__name__)

# ---------- 定数 ----------

DEFAULT_SIMILARITY_THRESHOLD = 0.1
ENTRY_POINT_LIMIT = 20
EXPANSION_LIMIT_PER_HOP = 50
MIN_COMBINED_SCORE = 0.05
PROPAGATION_DECAY = 0.7

# causal リンクのブースト倍率
_CAUSAL_BOOST = {
    "causes": 2.0,
    "caused_by": 2.0,
    "temporal": 1.0,
}


# ---------- 公開 API ----------


async def temporal_search(
    pool: asyncpg.Pool,
    bank_id: str,
    query_embedding: list[float],
    start_date: datetime,
    end_date: datetime,
    budget: int = 50,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> list[tuple[str, float]]:
    """時間範囲検索 + リンク拡散を実行する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        query_embedding: クエリの Embedding ベクトル（関連性フィルタ用）
        start_date: 検索開始日時（UTC, tz-aware）
        end_date: 検索終了日時（UTC, tz-aware）
        budget: 返却する最大結果数
        similarity_threshold: 最小 Embedding 類似度

    Returns:
        (unit_id, score) のリスト（スコア降順）
    """
    start_date = _ensure_utc(start_date)
    end_date = _ensure_utc(end_date)
    mid_date = start_date + (end_date - start_date) / 2
    total_days = max((end_date - start_date).total_seconds() / 86400, 0.01)

    async with pool.acquire() as conn:
        # Phase 1: 直接時間範囲マッチ
        entry_rows = await _find_temporal_entry_points(
            conn, bank_id, query_embedding,
            start_date, end_date, similarity_threshold,
        )

        scored: dict[str, float] = {}
        entry_ids: list[str] = []

        for row in entry_rows:
            uid = str(row["id"])
            best = _resolve_best_date(
                row["occurred_start"], row["occurred_end"], row["mentioned_at"]
            )
            proximity = _compute_temporal_proximity(best, mid_date, total_days) if best else 0.5
            scored[uid] = proximity
            entry_ids.append(uid)

        # Phase 2: リンク拡散（エントリポイントが存在する場合のみ）
        budget_remaining = budget - len(scored)
        if entry_ids and budget_remaining > 0:
            expanded = await _expand_through_links(
                conn, bank_id, entry_ids, scored,
                query_embedding, similarity_threshold,
                mid_date, total_days, budget_remaining,
            )
            for uid, score in expanded.items():
                if uid not in scored:
                    scored[uid] = score

    # スコア降順でソート、budget 件に制限
    sorted_results = sorted(scored.items(), key=lambda x: x[1], reverse=True)
    return sorted_results[:budget]


# ---------- ユーティリティ ----------


def _ensure_utc(dt: datetime) -> datetime:
    """タイムゾーンなし datetime を UTC に正規化する"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _resolve_best_date(
    occurred_start: datetime | None,
    occurred_end: datetime | None,
    mentioned_at: datetime | None,
) -> datetime | None:
    """スコアリング用の最適日時を解決する

    優先順:
    1. occurred_start/occurred_end の中間点
    2. occurred_start
    3. occurred_end
    4. mentioned_at
    """
    if occurred_start is not None and occurred_end is not None:
        start = _ensure_utc(occurred_start)
        end = _ensure_utc(occurred_end)
        return start + (end - start) / 2
    if occurred_start is not None:
        return _ensure_utc(occurred_start)
    if occurred_end is not None:
        return _ensure_utc(occurred_end)
    if mentioned_at is not None:
        return _ensure_utc(mentioned_at)
    return None


def _compute_temporal_proximity(
    best_date: datetime,
    mid_date: datetime,
    total_days: float,
) -> float:
    """時間的近接度スコアを計算する

    時間範囲の中央点で 1.0、端で 0.0 に減衰。
    範囲外は 0.0。
    """
    days_from_mid = abs((best_date - mid_date).total_seconds()) / 86400
    half_range = total_days / 2
    if half_range <= 0:
        return 1.0 if days_from_mid < 1 else 0.0
    return max(0.0, 1.0 - (days_from_mid / half_range))


# ---------- Phase 1: 直接時間範囲マッチ ----------


async def _find_temporal_entry_points(
    conn: asyncpg.Connection,
    bank_id: str,
    query_embedding: list[float],
    start_date: datetime,
    end_date: datetime,
    similarity_threshold: float,
) -> list[asyncpg.Record]:
    """時間範囲内のユニットを Embedding 関連性フィルタ付きで取得する"""
    return await conn.fetch(
        """
        SELECT id, occurred_start, occurred_end, mentioned_at,
               1 - (embedding <=> $1::vector) AS similarity
        FROM memory_units
        WHERE bank_id = $2::uuid
          AND fact_type IN ('world', 'experience', 'observation')
          AND embedding IS NOT NULL
          AND (
              (occurred_start IS NOT NULL AND occurred_end IS NOT NULL
               AND occurred_start <= $4 AND occurred_end >= $3)
              OR (mentioned_at IS NOT NULL AND mentioned_at BETWEEN $3 AND $4)
              OR (occurred_start IS NOT NULL AND occurred_start BETWEEN $3 AND $4)
              OR (occurred_end IS NOT NULL AND occurred_end BETWEEN $3 AND $4)
          )
          AND (1 - (embedding <=> $1::vector)) >= $5
        ORDER BY COALESCE(occurred_start, mentioned_at, occurred_end) DESC
        LIMIT $6
        """,
        query_embedding,
        bank_id,
        start_date,
        end_date,
        similarity_threshold,
        ENTRY_POINT_LIMIT,
    )


# ---------- Phase 2: リンク拡散 ----------


async def _expand_through_links(
    conn: asyncpg.Connection,
    bank_id: str,
    entry_ids: list[str],
    parent_scores: dict[str, float],
    query_embedding: list[float],
    similarity_threshold: float,
    mid_date: datetime,
    total_days: float,
    budget: int,
) -> dict[str, float]:
    """temporal/causes/caused_by リンクを通じて拡散伝播する

    BFS 風に1ホップ拡張し、各隣接ノードのスコアを計算:
    - neighbor_proximity: 隣接ノード自身の時間的近接度
    - propagated: 親スコア × weight × causal_boost × decay
    - combined: max(neighbor_proximity, propagated)
    """
    rows = await conn.fetch(
        """
        SELECT mu.id, mu.occurred_start, mu.occurred_end, mu.mentioned_at,
               ml.weight, ml.link_type, ml.from_unit_id,
               1 - (mu.embedding <=> $1::vector) AS similarity
        FROM memory_links ml
        JOIN memory_units mu ON ml.to_unit_id = mu.id
        WHERE ml.from_unit_id = ANY($2::uuid[])
          AND ml.link_type IN ('temporal', 'causes', 'caused_by')
          AND ml.weight >= 0.1
          AND mu.bank_id = $5::uuid
          AND mu.embedding IS NOT NULL
          AND (1 - (mu.embedding <=> $1::vector)) >= $3
        ORDER BY ml.weight DESC
        LIMIT $4
        """,
        query_embedding,
        entry_ids,
        similarity_threshold,
        EXPANSION_LIMIT_PER_HOP,
        bank_id,
    )

    expanded: dict[str, float] = {}

    for row in rows:
        uid = str(row["id"])
        if uid in parent_scores:
            continue

        parent_id = str(row["from_unit_id"])
        parent_score = parent_scores.get(parent_id, 0.5)
        link_weight = row["weight"]
        link_type = row["link_type"]
        boost = _CAUSAL_BOOST.get(link_type, 1.0)

        # 隣接ノード自身の時間的近接度
        best = _resolve_best_date(
            row["occurred_start"], row["occurred_end"], row["mentioned_at"]
        )
        neighbor_proximity = (
            _compute_temporal_proximity(best, mid_date, total_days) if best else 0.0
        )

        # 親からの伝搬スコア
        propagated = parent_score * link_weight * boost * PROPAGATION_DECAY

        combined = max(neighbor_proximity, propagated)

        if combined >= MIN_COMBINED_SCORE:
            # 複数の親からの拡散は最大値を採用
            expanded[uid] = max(expanded.get(uid, 0.0), combined)

        if len(expanded) >= budget:
            break

    return expanded
