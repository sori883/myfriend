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

# BM25 キーワード抽出用定数
# 複合助詞・文末表現（除去対象、長い順にソート）
# NOTE: 短い動詞ステム（ある、いる、する等）は複合語を壊すため除外
_COMPOUND_PARTICLES = sorted([
    "について", "に関して", "に関する", "に対して", "に対する",
    "のこと", "ということ", "といった", "のような", "みたいな",
    "における", "としての", "によると", "によって",
    "ですか", "ですが", "ですね", "ですよ", "でした",
    "ました", "ません", "ている", "ていた", "てください",
    "しました", "しています", "してた", "してる",
    "ってなに", "ってどう", "って何",
    "だった", "だっけ", "かな", "かも",
    "知って", "教えて", "覚えて", "思い出して",
], key=len, reverse=True)
_PARTICLE_SPLIT_PATTERN = re.compile(r"[はがのをにでともやへか\s]+")


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
    """PARTITION BY fact_type でバランスされた全文検索（pg_bigm）

    クエリから日本語助詞ベースでキーワードを抽出し、
    各キーワードの OR で LIKE 検索、MAX(bigm_similarity) でスコアリング。
    """
    keywords = _extract_keywords(query)
    if not keywords:
        if len(query) <= 20:
            keywords = [query]
        else:
            logger.debug("BM25 skipped: no keywords extracted from query: %s", query[:50])
            return []
    logger.debug("BM25 keywords: %s", keywords)

    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            WITH scored AS (
                SELECT id, text, context, fact_type, fact_kind, event_date,
                       created_at, occurred_start, mentioned_at,
                       (SELECT MAX(GREATEST(
                           bigm_similarity(mu.text, kw),
                           bigm_similarity(COALESCE(mu.context, ''), kw)
                       )) FROM unnest($1::text[]) AS kw) AS score
                FROM memory_units mu
                WHERE mu.bank_id = $2::uuid
                  AND mu.fact_type = ANY($3::text[])
                  AND EXISTS (
                      SELECT 1 FROM unnest($1::text[]) AS kw
                      WHERE mu.text LIKE likequery(kw)
                         OR mu.context LIKE likequery(kw)
                  )
            ),
            ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY fact_type ORDER BY score DESC
                ) AS rn
                FROM scored
            )
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   created_at, occurred_start, mentioned_at, score
            FROM ranked
            WHERE rn <= $4
            ORDER BY score DESC
            """,
            keywords,
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
        sim = float(row["similarity"])
        if sim >= GRAPH_SEED_SIMILARITY_THRESHOLD:
            seeds.append(
                SeedNode(node_id=str(row["id"]), score=sim)
            )
            if len(seeds) >= GRAPH_SEED_MAX:
                break
    if semantic_results:
        top_sim = float(semantic_results[0]["similarity"])
        logger.info(
            "Graph seed extraction: top_similarity=%.4f, threshold=%.2f, seeds=%d",
            top_sim, GRAPH_SEED_SIMILARITY_THRESHOLD, len(seeds),
        )
    return seeds


# ---------- 時間範囲抽出 ----------

# 日本語の相対的時間表現パターン
_RELATIVE_PATTERNS: list[tuple[re.Pattern, callable]] = []


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _build_relative_patterns() -> list[tuple[re.Pattern, callable]]:
    """相対的時間表現の正規表現パターンを構築する

    先月/去年/来月/来年はカレンダー境界を使用する。
    先週/来週はローリングウィンドウ（7日）を維持
    （日本語の「先週」は厳密なカレンダー週を意味しないため）。
    """
    patterns = []

    def _start_of_today() -> datetime:
        return _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)

    # --- 過去 ---
    def _yesterday(_m):
        t = _start_of_today()
        return (t - timedelta(days=1), t)

    def _day_before_yesterday(_m):
        t = _start_of_today()
        return (t - timedelta(days=2), t - timedelta(days=1))

    def _last_week(_m):
        t = _start_of_today()
        return (t - timedelta(days=7), t)

    def _last_month(_m):
        t = _start_of_today()
        first_of_this_month = t.replace(day=1)
        first_of_prev_month = (first_of_this_month - timedelta(days=1)).replace(day=1)
        return (first_of_prev_month, first_of_this_month)

    def _last_year(_m):
        t = _start_of_today()
        return (datetime(t.year - 1, 1, 1, tzinfo=UTC), datetime(t.year, 1, 1, tzinfo=UTC))

    def _n_days_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS)
        t = _start_of_today()
        return (t - timedelta(days=n), t)

    def _n_weeks_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 7)
        t = _start_of_today()
        return (t - timedelta(weeks=n), t)

    def _n_months_ago(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 30)
        t = _start_of_today()
        return (t - timedelta(days=30 * n), t)

    # --- 未来 ---
    def _tomorrow(_m):
        t = _start_of_today()
        return (t + timedelta(days=1), t + timedelta(days=2))

    def _day_after_tomorrow(_m):
        t = _start_of_today()
        return (t + timedelta(days=2), t + timedelta(days=3))

    def _next_week(_m):
        t = _start_of_today()
        return (t + timedelta(days=1), t + timedelta(days=8))

    def _next_month(_m):
        t = _start_of_today()
        ny, nm = (t.year + 1, 1) if t.month == 12 else (t.year, t.month + 1)
        first_of_next = datetime(ny, nm, 1, tzinfo=UTC)
        ny2, nm2 = (ny + 1, 1) if nm == 12 else (ny, nm + 1)
        first_after_next = datetime(ny2, nm2, 1, tzinfo=UTC)
        return (first_of_next, first_after_next)

    def _next_year(_m):
        t = _start_of_today()
        return (datetime(t.year + 1, 1, 1, tzinfo=UTC), datetime(t.year + 2, 1, 1, tzinfo=UTC))

    def _n_days_later(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS)
        t = _start_of_today()
        target = t + timedelta(days=n)
        return (target, target + timedelta(days=1))

    def _n_weeks_later(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 7)
        t = _start_of_today()
        target = t + timedelta(weeks=n)
        return (target, target + timedelta(weeks=1))

    def _n_months_later(m):
        n = min(int(m.group(1)), MAX_RELATIVE_DAYS // 30)
        t = _start_of_today()
        target = t + timedelta(days=30 * n)
        return (target, target + timedelta(days=30))

    # --- 今日 ---
    def _today(_m):
        t = _start_of_today()
        return (t, t + timedelta(days=1))

    # --- 絶対日付 ---
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

    def _year_only(m):
        year = int(m.group(1))
        if not (1900 <= year <= 2100):
            return None
        return (datetime(year, 1, 1, tzinfo=UTC), datetime(year + 1, 1, 1, tzinfo=UTC))

    # --- 曜日 ---
    _weekday_map = {
        "月曜日": 0, "月曜": 0,
        "火曜日": 1, "火曜": 1,
        "水曜日": 2, "水曜": 2,
        "木曜日": 3, "木曜": 3,
        "金曜日": 4, "金曜": 4,
        "土曜日": 5, "土曜": 5,
        "日曜日": 6, "日曜": 6,
    }

    def _last_weekday(m):
        target = _weekday_map.get(m.group(1))
        if target is None:
            return None
        t = _start_of_today()
        # 先週の月曜日（weekday=0）を基準に、target 曜日を加算
        days_since_monday = t.weekday()
        last_week_monday = t - timedelta(days=days_since_monday + 7)
        d = last_week_monday + timedelta(days=target)
        return (d, d + timedelta(days=1))

    # 優先度順（より具体的なパターンを先に）
    # 過去: N日/週/月前
    patterns.append((re.compile(r"(\d+)\s*日前"), _n_days_ago))
    patterns.append((re.compile(r"(\d+)\s*週間前"), _n_weeks_ago))
    patterns.append((re.compile(r"(\d+)\s*[かヶケ]月前"), _n_months_ago))
    # 未来: N日/週/月後
    patterns.append((re.compile(r"(\d+)\s*日後"), _n_days_later))
    patterns.append((re.compile(r"(\d+)\s*週間後"), _n_weeks_later))
    patterns.append((re.compile(r"(\d+)\s*[かヶケ]月後"), _n_months_later))
    # 絶対日付（年月 → 年のみ の順）
    patterns.append((re.compile(r"(\d{4})年(\d{1,2})月"), _year_month))
    patterns.append((re.compile(r"(\d{4})年(?!\d{1,2}月)"), _year_only))
    # 過去: 相対表現
    patterns.append((re.compile(r"おととい|一昨日"), _day_before_yesterday))
    patterns.append((re.compile(r"昨日|きのう"), _yesterday))
    # 曜日パターン（先週より前に配置して「先週の月曜日」を正しくマッチ）
    patterns.append((
        re.compile(r"(?:先週|前)の(月曜日?|火曜日?|水曜日?|木曜日?|金曜日?|土曜日?|日曜日?)"),
        _last_weekday,
    ))
    patterns.append((re.compile(r"先週"), _last_week))
    patterns.append((re.compile(r"先月"), _last_month))
    patterns.append((re.compile(r"去年|昨年"), _last_year))
    # 未来: 相対表現
    patterns.append((re.compile(r"明後日|あさって"), _day_after_tomorrow))
    patterns.append((re.compile(r"明日|あした"), _tomorrow))
    patterns.append((re.compile(r"来週"), _next_week))
    patterns.append((re.compile(r"来月"), _next_month))
    patterns.append((re.compile(r"来年"), _next_year))
    # 今日
    patterns.append((re.compile(r"今日|きょう"), _today))

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


# ---------- BM25 キーワード抽出 ----------


def _extract_keywords(query: str) -> list[str]:
    """クエリから BM25 検索用キーワードを抽出する（日本語助詞ベース分割）

    複合助詞の除去 → 単一助詞で分割 → 短トークン除去。外部 NLP 依存なし。
    """
    cleaned = query
    for cp in _COMPOUND_PARTICLES:
        cleaned = cleaned.replace(cp, " ")
    tokens = _PARTICLE_SPLIT_PATTERN.split(cleaned)
    seen: set[str] = set()
    keywords: list[str] = []
    for token in tokens:
        token = token.strip()
        if len(token) < 2 or token in seen:
            continue
        seen.add(token)
        keywords.append(token)
    return keywords


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
        build_rerank_document(c.text, c.context, c.occurred_start)
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
