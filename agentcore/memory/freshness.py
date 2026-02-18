"""鮮度追跡モジュール

Observation の鮮度（Freshness Status）を証拠のタイムスタンプ分布から計算する。
Consolidation 完了後に自動実行される。

計算式:
    recent_density = 直近30日の証拠数 / 30
    older_density  = それ以前の証拠数 / (総期間 - 30)
    ratio = recent_density / older_density

ステータス判定:
    - NEW:            全証拠が30日以内
    - STRENGTHENING:  ratio > 1.5
    - STABLE:         0.5 <= ratio <= 1.5
    - WEAKENING:      ratio < 0.5
    - STALE:          直近30日に証拠なし
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from enum import Enum

import asyncpg

logger = logging.getLogger(__name__)

RECENT_DAYS = 30


class FreshnessStatus(str, Enum):
    """Observation の鮮度ステータス"""

    NEW = "new"
    STRENGTHENING = "strengthening"
    STABLE = "stable"
    WEAKENING = "weakening"
    STALE = "stale"


def compute_freshness(
    evidence_timestamps: list[datetime],
    now: datetime | None = None,
    recent_days: int = RECENT_DAYS,
) -> FreshnessStatus:
    """証拠のタイムスタンプ分布から鮮度ステータスを計算する

    Args:
        evidence_timestamps: 各証拠（source memory）のタイムスタンプ（UTC）
        now: 基準時刻（デフォルト: 現在 UTC）
        recent_days: 「最近」と判定する日数（デフォルト: 30）

    Returns:
        FreshnessStatus
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if not evidence_timestamps:
        return FreshnessStatus.STALE

    def _normalize(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts

    timestamps = [_normalize(ts) for ts in evidence_timestamps]
    recent_cutoff = now - timedelta(days=recent_days)

    recent = [ts for ts in timestamps if ts > recent_cutoff]
    older = [ts for ts in timestamps if ts <= recent_cutoff]

    # 直近30日に証拠なし → STALE
    if not recent:
        return FreshnessStatus.STALE

    # 全証拠が30日以内 → NEW
    if not older:
        return FreshnessStatus.NEW

    # 密度計算
    recent_density = len(recent) / recent_days

    earliest = min(timestamps)
    total_span_days = max((now - earliest).total_seconds() / 86400, 1)
    older_period = max(total_span_days - recent_days, 1)
    older_density = len(older) / older_period

    # 防御的チェック: older が空なら78行目で NEW を返すため
    # 通常到達しないが、浮動小数点演算の安全弁として残す
    if older_density == 0:
        return FreshnessStatus.NEW

    ratio = recent_density / older_density

    if ratio > 1.5:
        return FreshnessStatus.STRENGTHENING
    elif ratio < 0.5:
        return FreshnessStatus.WEAKENING
    else:
        return FreshnessStatus.STABLE


async def update_freshness_for_bank(
    pool: asyncpg.Pool,
    bank_id: str,
) -> dict:
    """指定 bank の全 Observation の鮮度ステータスを一括更新する

    効率的なバッチ処理:
    1. bank 内の全 Observation + source_memory_ids を1クエリで取得
    2. 必要な全 source memory の created_at を1クエリで取得
    3. Python 側で各 Observation の鮮度を計算
    4. バッチ UPDATE で DB 更新

    Args:
        pool: DB 接続プール
        bank_id: メモリバンク ID

    Returns:
        更新統計 {"total": int, "updated": int, "statuses": dict}
    """
    async with pool.acquire() as conn:
        observations = await conn.fetch(
            """
            SELECT id, source_memory_ids
            FROM memory_units
            WHERE bank_id = $1::uuid
              AND fact_type = 'observation'
            """,
            bank_id,
        )

    if not observations:
        return {"total": 0, "updated": 0, "statuses": {}}

    # 全 source_memory_ids を収集して重複排除
    all_source_ids = set()
    for obs in observations:
        if obs["source_memory_ids"]:
            all_source_ids.update(obs["source_memory_ids"])

    # source memory の created_at を一括取得
    source_timestamps: dict[str, datetime] = {}
    if all_source_ids:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, created_at
                FROM memory_units
                WHERE id = ANY($1::uuid[])
                  AND bank_id = $2::uuid
                """,
                list(all_source_ids),
                bank_id,
            )
        source_timestamps = {str(r["id"]): r["created_at"] for r in rows}

    # 各 Observation の鮮度を計算
    updates: list[tuple[str, str]] = []  # (freshness_status, observation_id)
    status_counts: dict[str, int] = defaultdict(int)

    for obs in observations:
        source_ids = obs["source_memory_ids"] or []
        evidence_ts = [
            source_timestamps[str(sid)]
            for sid in source_ids
            if str(sid) in source_timestamps
        ]

        status = compute_freshness(evidence_ts)
        updates.append((status.value, str(obs["id"])))
        status_counts[status.value] += 1

    # バッチ UPDATE
    if updates:
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                UPDATE memory_units
                SET freshness_status = $1
                WHERE id = $2::uuid
                """,
                updates,
            )

    logger.info(
        "Freshness updated for bank %s: %d observations (%s)",
        bank_id,
        len(updates),
        dict(status_counts),
    )

    return {
        "total": len(observations),
        "updated": len(updates),
        "statuses": dict(status_counts),
    }
