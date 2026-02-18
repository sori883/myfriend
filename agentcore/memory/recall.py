"""Recall パイプライン

4方向並列検索（セマンティック + BM25 + グラフ + 時間）
→ RRF 融合 → クロスエンコーダリランキング → 最終スコアリング → トリミング。
"""

import asyncio
import logging
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

import asyncpg

from memory.embedding import generate_embedding
from memory.graph_search import SeedNode, graph_search
from memory.reranker import (
    RERANK_CANDIDATE_LIMIT,
    build_rerank_document,
    rerank,
)
from memory.temporal_search import temporal_search

logger = logging.getLogger(__name__)

# ---------- 定数 ----------

BUDGETS = {
    "low": {"max_tokens": 2048, "max_results": 20},
    "mid": {"max_tokens": 4096, "max_results": 50},
    "high": {"max_tokens": 8192, "max_results": 100},
}

RRF_K = 60

SEMANTIC_SIMILARITY_THRESHOLD = 0.1

# バッチ最適化: fact_type 毎の取得上限（3タイプ × 34 ≈ 100）
PER_TYPE_LIMIT = 34

# グラフ検索シード: セマンティック結果から cosine >= 0.5 の上位5件
GRAPH_SEED_SIMILARITY_THRESHOLD = 0.5
GRAPH_SEED_MAX = 5

# トークン推定: 英語 ~4文字/トークン、日本語混在 ~1.5文字/トークン、中間値 3
CHARS_PER_TOKEN = 3

# 最終スコアリング重み
WEIGHT_CE = 0.5
WEIGHT_RRF = 0.3
WEIGHT_RECENCY = 0.1
WEIGHT_TEMPORAL = 0.1
RECENCY_DECAY_DAYS = 365
SECONDS_PER_DAY = 86400

# 相対時間表現の最大日数（10年）
MAX_RELATIVE_DAYS = 3650

_FACT_TYPES = ["world", "experience", "observation"]


# ---------- データ構造 ----------


@dataclass(frozen=True)
class MemoryResult:
    """検索結果の1件"""

    id: str
    text: str
    context: str | None
    fact_type: str
    fact_kind: str | None
    event_date: str | None
    created_at: datetime | None
    occurred_start: datetime | None
    mentioned_at: datetime | None
    score: float
    ce_score: float = 0.0


# ---------- セマンティック検索 ----------


async def _semantic_search(
    pool: asyncpg.Pool,
    bank_id: str,
    query_embedding: list[float],
) -> list[asyncpg.Record]:
    """PARTITION BY fact_type でバランスされたセマンティック検索"""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            WITH ranked AS (
                SELECT id, text, context, fact_type, fact_kind, event_date,
                       created_at, occurred_start, mentioned_at,
                       1 - (embedding <=> $1::vector) AS similarity,
                       ROW_NUMBER() OVER (
                           PARTITION BY fact_type
                           ORDER BY embedding <=> $1::vector
                       ) AS rn
                FROM memory_units
                WHERE bank_id = $2::uuid
                  AND fact_type = ANY($3::text[])
                  AND embedding IS NOT NULL
                  AND (1 - (embedding <=> $1::vector)) >= $4
            )
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   created_at, occurred_start, mentioned_at, similarity
            FROM ranked
            WHERE rn <= $5
            ORDER BY similarity DESC
            """,
            query_embedding,
            bank_id,
            _FACT_TYPES,
            SEMANTIC_SIMILARITY_THRESHOLD,
            PER_TYPE_LIMIT,
        )


# ---------- BM25 検索 ----------


async def _bm25_search(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
) -> list[asyncpg.Record]:
    """PARTITION BY fact_type でバランスされた全文検索（pg_bigm）"""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            WITH ranked AS (
                SELECT id, text, context, fact_type, fact_kind, event_date,
                       created_at, occurred_start, mentioned_at,
                       bigm_similarity(text, $1) AS score,
                       ROW_NUMBER() OVER (
                           PARTITION BY fact_type
                           ORDER BY bigm_similarity(text, $1) DESC
                       ) AS rn
                FROM memory_units
                WHERE bank_id = $2::uuid
                  AND fact_type = ANY($3::text[])
                  AND (text LIKE likequery($1) OR context LIKE likequery($1))
            )
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   created_at, occurred_start, mentioned_at, score
            FROM ranked
            WHERE rn <= $4
            ORDER BY score DESC
            """,
            query,
            bank_id,
            _FACT_TYPES,
            PER_TYPE_LIMIT,
        )


# ---------- グラフ検索シード ----------


def _extract_graph_seeds(
    semantic_results: list[asyncpg.Record],
) -> list[SeedNode]:
    """セマンティック結果 top-5 (cosine >= 0.5) を SeedNode に変換する"""
    seeds: list[SeedNode] = []
    for row in semantic_results:
        if row["similarity"] >= GRAPH_SEED_SIMILARITY_THRESHOLD:
            seeds.append(
                SeedNode(node_id=str(row["id"]), score=float(row["similarity"]))
            )
            if len(seeds) >= GRAPH_SEED_MAX:
                break
    return seeds


# ---------- 時間範囲抽出 ----------

# 日本語の相対的時間表現パターン
_RELATIVE_PATTERNS: list[tuple[re.Pattern, callable]] = []


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _build_relative_patterns() -> list[tuple[re.Pattern, callable]]:
    """相対的時間表現の正規表現パターンを構築する"""
    patterns = []

    def _yesterday(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=1), today)

    def _today(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today, today + timedelta(days=1))

    def _last_week(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=7), today)

    def _last_month(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=30), today)

    def _last_year(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=365), today)

    def _n_days_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS)
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=n), today)

    def _n_weeks_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 7)
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(weeks=n), today)

    def _n_months_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 30)
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=30 * n), today)

    def _year_month(m):
        year = int(m.group(1))
        month = int(m.group(2))
        if not (1 <= month <= 12):
            return None
        start = datetime(year, month, 1, tzinfo=UTC)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(year, month + 1, 1, tzinfo=UTC)
        return (start, end)

    def _day_before_yesterday(_m):
        today = _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
        return (today - timedelta(days=2), today - timedelta(days=1))

    # 優先度順（より具体的なパターンを先に）
    patterns.append((re.compile(r"(\d+)\s*日前"), _n_days_ago))
    patterns.append((re.compile(r"(\d+)\s*週間前"), _n_weeks_ago))
    patterns.append((re.compile(r"(\d+)\s*[かヶケ]月前"), _n_months_ago))
    patterns.append((re.compile(r"(\d{4})年(\d{1,2})月"), _year_month))
    patterns.append((re.compile(r"おととい|一昨日"), _day_before_yesterday))
    patterns.append((re.compile(r"昨日|きのう"), _yesterday))
    patterns.append((re.compile(r"今日|きょう"), _today))
    patterns.append((re.compile(r"先週"), _last_week))
    patterns.append((re.compile(r"先月"), _last_month))
    patterns.append((re.compile(r"去年|昨年"), _last_year))

    return patterns


_RELATIVE_PATTERNS = _build_relative_patterns()


def _extract_time_range(query: str) -> tuple[datetime, datetime] | None:
    """クエリから時間範囲を正規表現で抽出する

    一致なし → None（時間検索スキップ）
    """
    for pattern, handler in _RELATIVE_PATTERNS:
        m = pattern.search(query)
        if m:
            result = handler(m)
            if result is not None:
                return result
    return None


# ---------- graph/temporal 結果の詳細取得 ----------


async def _fetch_unit_details(
    pool: asyncpg.Pool,
    bank_id: str,
    unit_ids: list[str],
) -> dict[str, asyncpg.Record]:
    """graph/temporal 結果の unit_id からテキスト等を一括取得する"""
    if not unit_ids:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   created_at, occurred_start, mentioned_at
            FROM memory_units
            WHERE id = ANY($1::uuid[])
              AND bank_id = $2::uuid
            """,
            unit_ids,
            bank_id,
        )
    return {str(row["id"]): row for row in rows}


# ---------- RRF 融合 ----------


def _rrf_fuse(
    semantic_results: list[asyncpg.Record],
    bm25_results: list[asyncpg.Record],
    graph_results: list[tuple[str, float]],
    temporal_results: list[tuple[str, float]],
    detail_cache: dict[str, asyncpg.Record],
) -> list[MemoryResult]:
    """4方向 RRF 融合

    score(d) = Σ 1/(K + rank_in_list_i)
    """
    scores: dict[str, float] = {}
    records: dict[str, asyncpg.Record] = {}

    # DB レコード系（semantic, bm25）
    for ranked_list in [semantic_results, bm25_results]:
        for rank, row in enumerate(ranked_list):
            doc_id = str(row["id"])
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (RRF_K + rank)
            if doc_id not in records:
                records[doc_id] = row

    # tuple 系（graph, temporal）
    for ranked_tuples in [graph_results, temporal_results]:
        for rank, (doc_id, _score) in enumerate(ranked_tuples):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (RRF_K + rank)
            if doc_id not in records:
                detail = detail_cache.get(doc_id)
                if detail is not None:
                    records[doc_id] = detail

    sorted_ids = sorted(scores, key=lambda x: scores[x], reverse=True)

    return [
        result
        for doc_id in sorted_ids
        if (result := _record_to_result(doc_id, records.get(doc_id), scores[doc_id]))
        is not None
    ]


def _record_to_result(
    doc_id: str,
    row: asyncpg.Record | None,
    score: float,
) -> MemoryResult | None:
    """asyncpg.Record を MemoryResult に変換する"""
    if row is None:
        return None
    event_date = row["event_date"]
    return MemoryResult(
        id=doc_id,
        text=row["text"],
        context=row["context"],
        fact_type=row["fact_type"],
        fact_kind=row.get("fact_kind"),
        event_date=event_date.isoformat() if event_date else None,
        created_at=row.get("created_at"),
        occurred_start=row.get("occurred_start"),
        mentioned_at=row.get("mentioned_at"),
        score=score,
    )


# ---------- リランキング統合 ----------


async def _rerank_results(
    query: str,
    fused: list[MemoryResult],
) -> list[MemoryResult]:
    """RRF 融合結果をクロスエンコーダでリランキングする"""
    candidates = fused[:RERANK_CANDIDATE_LIMIT]
    if not candidates:
        return []

    documents = [
        _build_rerank_document(c.text, c.context, c.occurred_start)
        for c in candidates
    ]

    try:
        reranked = await rerank(query, documents)
    except Exception:
        logger.warning("Rerank API failed, falling back to RRF order", exc_info=True)
        return candidates

    return [
        replace(candidates[idx], ce_score=score)
        for idx, score in reranked
        if idx < len(candidates)
    ]


# ---------- 最終スコアリング ----------


def _compute_recency(created_at: datetime | None) -> float:
    """recency スコア: 新しいほど 1.0 に近い"""
    if created_at is None:
        return 0.0
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    now = _now_utc()
    days = max(0.0, (now - created_at).total_seconds() / SECONDS_PER_DAY)
    return max(0.0, 1.0 - days / RECENCY_DECAY_DAYS)


def _compute_temporal_proximity(
    result: MemoryResult,
    time_range: tuple[datetime, datetime] | None,
) -> float:
    """時間的近接度: 検索範囲の中央に近いほど 1.0"""
    if time_range is None:
        return 0.0
    start, end = time_range
    mid = start + (end - start) / 2
    total_days = max((end - start).total_seconds() / SECONDS_PER_DAY, 0.01)

    best_date = result.occurred_start or result.mentioned_at
    if best_date is None:
        return 0.0
    if best_date.tzinfo is None:
        best_date = best_date.replace(tzinfo=UTC)

    days_from_mid = abs((best_date - mid).total_seconds()) / SECONDS_PER_DAY
    half_range = total_days / 2
    if half_range <= 0:
        return 1.0 if days_from_mid < 1 else 0.0
    return max(0.0, 1.0 - days_from_mid / half_range)


def _final_scoring(
    reranked: list[MemoryResult],
    time_range: tuple[datetime, datetime] | None,
) -> list[MemoryResult]:
    """最終スコア = 0.5*CE + 0.3*RRF_norm + 0.1*recency + 0.1*temporal"""
    if not reranked:
        return []

    max_rrf = max(r.score for r in reranked)
    if max_rrf <= 0:
        max_rrf = 1.0

    scored: list[MemoryResult] = []
    for r in reranked:
        rrf_norm = r.score / max_rrf
        recency = _compute_recency(r.created_at)
        temporal = _compute_temporal_proximity(r, time_range)

        final = (
            WEIGHT_CE * r.ce_score
            + WEIGHT_RRF * rrf_norm
            + WEIGHT_RECENCY * recency
            + WEIGHT_TEMPORAL * temporal
        )
        scored.append(replace(r, score=final))

    return sorted(scored, key=lambda x: x.score, reverse=True)


# ---------- トリミング ----------


def _trim_to_budget(
    results: list[MemoryResult],
    max_tokens: int,
    max_results: int,
) -> list[MemoryResult]:
    """トークンバジェット内に結果をトリミングする"""
    trimmed: list[MemoryResult] = []
    total_chars = 0
    max_chars = max_tokens * CHARS_PER_TOKEN

    for result in results[:max_results]:
        result_chars = len(result.text) + len(result.context or "")
        if total_chars + result_chars > max_chars and trimmed:
            break
        trimmed.append(result)
        total_chars += result_chars

    return trimmed


# ---------- 公開 API ----------


async def _empty_search() -> list[tuple[str, float]]:
    """空の検索結果を返すダミーコルーチン"""
    return []


def _build_response(
    trimmed: list[MemoryResult],
    total_found: int,
    budget: str,
) -> dict:
    """検索結果をレスポンス dict に変換する"""
    return {
        "memories": [
            {
                "id": r.id,
                "text": r.text,
                "context": r.context,
                "fact_type": r.fact_type,
                "fact_kind": r.fact_kind,
                "event_date": r.event_date,
                "score": round(r.score, 4),
            }
            for r in trimmed
        ],
        "total_found": total_found,
        "returned": len(trimmed),
        "budget": budget,
    }


async def _execute_pipeline(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
    query_embedding: list[float],
    time_range: tuple[datetime, datetime] | None,
    budget_config: dict,
) -> tuple[list[MemoryResult], int]:
    """4方向並列検索 → RRF → リランク → スコアリング → トリミング"""
    # Phase A: セマンティック + BM25 + 時間 並列検索
    temporal_task = (
        temporal_search(pool, bank_id, query_embedding, time_range[0], time_range[1])
        if time_range
        else _empty_search()
    )
    semantic_results, bm25_results, temporal_results = await asyncio.gather(
        _semantic_search(pool, bank_id, query_embedding),
        _bm25_search(pool, bank_id, query),
        temporal_task,
    )

    # Phase B: グラフ検索（セマンティック結果のシードに依存）
    graph_seeds = _extract_graph_seeds(semantic_results)
    graph_results = (
        await graph_search(pool, bank_id, graph_seeds) if graph_seeds else []
    )

    # graph/temporal 固有 ID の詳細取得
    db_ids = {str(row["id"]) for row in [*semantic_results, *bm25_results]}
    extra_ids = [
        uid for uid, _ in [*graph_results, *temporal_results] if uid not in db_ids
    ]
    detail_cache = await _fetch_unit_details(pool, bank_id, extra_ids)

    # RRF 融合 → リランク → スコアリング → トリミング
    fused = _rrf_fuse(
        semantic_results, bm25_results,
        graph_results, temporal_results,
        detail_cache,
    )
    reranked = await _rerank_results(query, fused)
    scored = _final_scoring(reranked, time_range)
    trimmed = _trim_to_budget(
        scored,
        max_tokens=budget_config["max_tokens"],
        max_results=budget_config["max_results"],
    )

    logger.info(
        "Recall complete: semantic=%d, bm25=%d, graph=%d, temporal=%d, "
        "fused=%d, reranked=%d, trimmed=%d",
        len(semantic_results), len(bm25_results),
        len(graph_results), len(temporal_results),
        len(fused), len(reranked), len(trimmed),
    )
    return trimmed, len(fused)


async def recall(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
    budget: str = "mid",
) -> dict:
    """Recall パイプラインを実行する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        query: 検索クエリ
        budget: トークンバジェット ("low" / "mid" / "high")

    Returns:
        検索結果
    """
    budget_config = BUDGETS.get(budget, BUDGETS["mid"])

    try:
        query_embedding = await generate_embedding(query)
    except Exception:
        logger.error("Embedding generation failed for query", exc_info=True)
        return _build_response([], 0, budget)

    time_range = _extract_time_range(query)

    trimmed, total_found = await _execute_pipeline(
        pool, bank_id, query, query_embedding, time_range, budget_config,
    )
    return _build_response(trimmed, total_found, budget)
