"""レコメンデーションエンジン

preference_profiles のスコアリング・フィルタリング・多様性制御を行い、
パーソナライズされた推薦候補を返す。
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)

MAX_RECOMMENDATIONS = 3
DIVERSITY_WINDOW_DAYS = 7
MAX_AVOID_ITEMS = 10


# ---------------------------------------------------------------------------
# Data classes (immutable)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoredItem:
    """スコア計算済みの推薦候補"""

    item: str
    intensity: float
    evidence_count: int
    last_mentioned_at: datetime
    score: float
    reason: str
    last_recommended_at: datetime | None


@dataclass(frozen=True)
class AvoidItem:
    """回避リストのアイテム"""

    item: str
    intensity: float


# ---------------------------------------------------------------------------
# Score calculation
# ---------------------------------------------------------------------------


def _recency_bonus(last_mentioned_at: datetime) -> float:
    """最終言及からの経過日数に応じた新鮮度ボーナスを計算する"""
    days_ago = (datetime.now(timezone.utc) - last_mentioned_at).days
    if days_ago <= 7:
        return 1.0
    if days_ago <= 30:
        return 0.7
    if days_ago <= 90:
        return 0.4
    return 0.2


def _calculate_score(
    intensity: float,
    evidence_count: int,
    last_mentioned_at: datetime,
) -> float:
    """推薦スコアを計算する

    score = intensity * 0.5 + evidence_bonus * 0.3 + recency_bonus * 0.2
    """
    evidence_bonus = min(evidence_count / 10.0, 1.0)
    recency = _recency_bonus(last_mentioned_at)
    return intensity * 0.5 + evidence_bonus * 0.3 + recency * 0.2


def _assign_reason(
    intensity: float,
    evidence_count: int,
    recency_bonus_val: float,
) -> str:
    """推薦理由を判定する"""
    if recency_bonus_val >= 0.7 and intensity >= 0.6:
        return "recent_high_intensity"
    if evidence_count >= 5:
        return "high_evidence"
    if intensity >= 0.7:
        return "high_intensity"
    return "general_preference"


# ---------------------------------------------------------------------------
# Diversity control
# ---------------------------------------------------------------------------


def _diversity_decay(last_recommended_at: datetime | None) -> float:
    """直近の推薦時期に応じたスコア減衰率を返す"""
    if last_recommended_at is None:
        return 1.0
    hours_ago = (
        datetime.now(timezone.utc) - last_recommended_at
    ).total_seconds() / 3600
    if hours_ago <= 24:
        return 0.1
    if hours_ago <= 72:
        return 0.3
    if hours_ago <= 168:
        return 0.6
    return 1.0


async def _get_recent_recommendations(
    conn: asyncpg.Connection,
    bank_id: str,
    entity_id: str,
    category: str,
) -> dict[str, datetime]:
    """直近7日間の推薦履歴から、各アイテムの最新推薦日時を取得する

    Returns:
        {item_name: last_recommended_at} のマッピング
    """
    rows = await conn.fetch(
        """SELECT recommended_item, MAX(created_at) AS last_recommended_at
           FROM (
               SELECT unnest(recommended_items) AS recommended_item, created_at
               FROM recommendation_history
               WHERE bank_id = $1::uuid
                 AND entity_id = $2::uuid
                 AND category = $3
                 AND created_at > NOW() - INTERVAL '7 days'
           ) sub
           GROUP BY recommended_item""",
        bank_id,
        entity_id,
        category,
    )
    return {row["recommended_item"]: row["last_recommended_at"] for row in rows}


# ---------------------------------------------------------------------------
# Data fetching & filtering
# ---------------------------------------------------------------------------


async def _fetch_preferences(
    conn: asyncpg.Connection,
    bank_id: str,
    entity_id: str,
    category: str,
) -> list[asyncpg.Record]:
    """指定カテゴリの嗜好データを取得する"""
    return await conn.fetch(
        """SELECT item, sentiment, intensity, evidence_count,
                  first_mentioned_at, last_mentioned_at
           FROM preference_profiles
           WHERE bank_id = $1::uuid
             AND entity_id = $2::uuid
             AND category = $3
           ORDER BY intensity DESC""",
        bank_id,
        entity_id,
        category,
    )


def _filter_preferences(
    rows: list[asyncpg.Record],
) -> tuple[list[asyncpg.Record], list[AvoidItem]]:
    """negative を回避リストに分離し、positive/neutral を候補として返す"""
    candidates: list[asyncpg.Record] = []
    avoid: list[AvoidItem] = []
    for row in rows:
        if row["sentiment"] == "negative":
            avoid.append(
                AvoidItem(item=row["item"], intensity=round(row["intensity"], 2))
            )
        else:
            candidates.append(row)
    return (candidates, avoid[:MAX_AVOID_ITEMS])


# ---------------------------------------------------------------------------
# Scoring & diversification
# ---------------------------------------------------------------------------


def _score_and_diversify(
    candidates: list[asyncpg.Record],
    recent_recs: dict[str, datetime],
) -> list[ScoredItem]:
    """候補にスコアを付け、多様性制御を適用し、スコア降順でソートする"""
    scored: list[ScoredItem] = []
    for row in candidates:
        base_score = _calculate_score(
            row["intensity"],
            row["evidence_count"],
            row["last_mentioned_at"],
        )
        last_rec = recent_recs.get(row["item"])
        decay = _diversity_decay(last_rec)
        final_score = base_score * decay

        recency_val = _recency_bonus(row["last_mentioned_at"])
        reason = _assign_reason(row["intensity"], row["evidence_count"], recency_val)

        scored.append(
            ScoredItem(
                item=row["item"],
                intensity=round(row["intensity"], 2),
                evidence_count=row["evidence_count"],
                last_mentioned_at=row["last_mentioned_at"],
                score=round(final_score, 4),
                reason=reason,
                last_recommended_at=last_rec,
            )
        )

    return sorted(scored, key=lambda x: x.score, reverse=True)


# ---------------------------------------------------------------------------
# History recording
# ---------------------------------------------------------------------------


async def _record_recommendation(
    conn: asyncpg.Connection,
    bank_id: str,
    entity_id: str,
    category: str,
    items: list[str],
    context: str,
) -> str:
    """推薦結果を recommendation_history に記録し、ID を返す"""
    row = await conn.fetchrow(
        """INSERT INTO recommendation_history
               (bank_id, entity_id, category, recommended_items, context)
           VALUES ($1::uuid, $2::uuid, $3, $4, $5)
           RETURNING id""",
        bank_id,
        entity_id,
        category,
        items,
        context or None,
    )
    return str(row["id"])


# ---------------------------------------------------------------------------
# Response building
# ---------------------------------------------------------------------------


def _build_response(
    top_items: list[ScoredItem],
    avoid_items: list[AvoidItem],
    category: str,
    rec_id: str | None,
) -> dict:
    """推薦結果をレスポンス辞書に変換する"""
    return {
        "recommendations": [
            {
                "item": item.item,
                "intensity": item.intensity,
                "evidence_count": item.evidence_count,
                "reason": item.reason,
                "last_recommended_at": (
                    item.last_recommended_at.isoformat()
                    if item.last_recommended_at
                    else None
                ),
            }
            for item in top_items
        ],
        "avoid": [
            {"item": ai.item, "intensity": ai.intensity} for ai in avoid_items
        ],
        "category": category,
        "recommendation_id": rec_id,
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


async def get_recommendations(
    pool: asyncpg.Pool,
    bank_id: str,
    entity_id: str,
    category: str,
    context: str = "",
) -> dict:
    """推薦候補を生成する

    処理フロー:
    1. 嗜好データ取得
    2. フィルタリング (negative → avoid, positive/neutral → candidate)
    3. スコア計算
    4. 多様性制御
    5. 上位N件選出 + 回避リスト生成
    6. 履歴記録
    """
    async with pool.acquire() as conn:
        preferences = await _fetch_preferences(conn, bank_id, entity_id, category)
        if not preferences:
            return {
                "recommendations": [],
                "avoid": [],
                "category": category,
                "message": "まだ好みがわかっていないカテゴリです",
            }

        candidates, avoid_items = _filter_preferences(preferences)

        recent_recs = await _get_recent_recommendations(
            conn, bank_id, entity_id, category
        )
        scored = _score_and_diversify(candidates, recent_recs)

        top_items = scored[:MAX_RECOMMENDATIONS]

        rec_id = None
        if top_items:
            try:
                rec_id = await _record_recommendation(
                    conn,
                    bank_id,
                    entity_id,
                    category,
                    [item.item for item in top_items],
                    context,
                )
            except Exception:
                logger.warning("Failed to record recommendation history", exc_info=True)

    return _build_response(top_items, avoid_items, category, rec_id)


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------


async def record_feedback(
    pool: asyncpg.Pool,
    bank_id: str,
    recommendation_id: str,
    accepted: bool,
    accepted_item: str | None = None,
) -> dict:
    """推薦へのフィードバックを記録する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID（認可チェック用）
        recommendation_id: recommendation_history の ID
        accepted: ユーザーが受け入れたかどうか
        accepted_item: 受け入れた具体的なアイテム名 (accepted=True の場合)
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            """UPDATE recommendation_history
               SET accepted = $1, accepted_item = $2
               WHERE id = $3::uuid
                 AND bank_id = $4::uuid
                 AND accepted IS NULL""",
            accepted,
            accepted_item,
            recommendation_id,
            bank_id,
        )
    updated = int(result.split()[-1])
    if updated == 0:
        return {
            "updated": 0,
            "recommendation_id": recommendation_id,
            "error": "recommendation not found or already recorded",
        }
    return {"updated": updated, "recommendation_id": recommendation_id}
