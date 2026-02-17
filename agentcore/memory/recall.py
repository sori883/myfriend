"""Recall パイプライン（基本版）

2方向検索（セマンティック + BM25）+ RRF 融合 + トークンバジェット管理。
Phase 2 で4方向検索（グラフ + 時間）に拡張予定。
"""

import asyncio
import logging
from dataclasses import dataclass

import asyncpg

from memory.embedding import generate_embedding

logger = logging.getLogger(__name__)

BUDGETS = {
    "low": {"max_tokens": 2048, "max_results": 20},
    "mid": {"max_tokens": 4096, "max_results": 50},
    "high": {"max_tokens": 8192, "max_results": 100},
}

RRF_K = 60

SEMANTIC_SIMILARITY_THRESHOLD = 0.3

# トークン推定: 英語 ~4文字/トークン、日本語混在 ~1.5文字/トークン、中間値 3
CHARS_PER_TOKEN = 3


@dataclass(frozen=True)
class MemoryResult:
    """検索結果の1件"""

    id: str
    text: str
    context: str | None
    fact_type: str
    fact_kind: str | None
    event_date: str | None
    score: float


async def _semantic_search(
    pool: asyncpg.Pool,
    bank_id: str,
    query_embedding: list[float],
) -> list[asyncpg.Record]:
    """セマンティック検索（HNSW コサイン類似度）"""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   1 - (embedding <=> $1::vector) AS similarity
            FROM memory_units
            WHERE bank_id = $2::uuid
              AND fact_type IN ('world', 'experience')
              AND embedding IS NOT NULL
              AND (1 - (embedding <=> $1::vector)) >= $3
            ORDER BY embedding <=> $1::vector
            LIMIT 100
            """,
            query_embedding,
            bank_id,
            SEMANTIC_SIMILARITY_THRESHOLD,
        )


async def _bm25_search(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
) -> list[asyncpg.Record]:
    """BM25 検索（tsvector + GIN）"""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, text, context, fact_type, fact_kind, event_date,
                   ts_rank_cd(search_vector, websearch_to_tsquery('english', $1)) AS score
            FROM memory_units
            WHERE bank_id = $2::uuid
              AND fact_type IN ('world', 'experience')
              AND search_vector @@ websearch_to_tsquery('english', $1)
            ORDER BY score DESC
            LIMIT 100
            """,
            query,
            bank_id,
        )


def _rrf_fuse(
    ranked_lists: list[list[asyncpg.Record]],
) -> list[MemoryResult]:
    """RRF（Reciprocal Rank Fusion）でスコアを統合する

    score(d) = Σ 1/(K + rank_in_list_i)
    """
    scores: dict[str, float] = {}
    records: dict[str, asyncpg.Record] = {}

    for ranked_list in ranked_lists:
        for rank, row in enumerate(ranked_list):
            doc_id = str(row["id"])
            rrf_score = 1.0 / (RRF_K + rank)
            scores[doc_id] = scores.get(doc_id, 0.0) + rrf_score
            if doc_id not in records:
                records[doc_id] = row

    sorted_ids = sorted(scores, key=lambda x: scores[x], reverse=True)

    results = []
    for doc_id in sorted_ids:
        row = records[doc_id]
        event_date = row["event_date"]
        results.append(
            MemoryResult(
                id=doc_id,
                text=row["text"],
                context=row["context"],
                fact_type=row["fact_type"],
                fact_kind=row["fact_kind"],
                event_date=event_date.isoformat() if event_date else None,
                score=scores[doc_id],
            )
        )

    return results


def _trim_to_budget(
    results: list[MemoryResult],
    max_tokens: int,
    max_results: int,
) -> list[MemoryResult]:
    """トークンバジェット内に結果をトリミングする"""
    trimmed = []
    total_chars = 0
    max_chars = max_tokens * CHARS_PER_TOKEN

    for result in results[:max_results]:
        result_chars = len(result.text) + len(result.context or "")
        if total_chars + result_chars > max_chars and trimmed:
            break
        trimmed.append(result)
        total_chars += result_chars

    return trimmed


async def recall(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
    budget: str = "mid",
) -> dict:
    """Recall パイプラインを実行する

    処理フロー:
    1. クエリ Embedding 生成
    2. 2方向並列検索（セマンティック + BM25）
    3. RRF 融合
    4. トークンバジェットに基づくトリミング

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        query: 検索クエリ
        budget: トークンバジェット ("low" / "mid" / "high")

    Returns:
        検索結果
    """
    budget_config = BUDGETS.get(budget, BUDGETS["mid"])

    # 1. クエリ Embedding 生成
    query_embedding = await generate_embedding(query)

    # 2. 2方向並列検索
    semantic_results, bm25_results = await asyncio.gather(
        _semantic_search(pool, bank_id, query_embedding),
        _bm25_search(pool, bank_id, query),
    )

    # 3. RRF 融合
    fused = _rrf_fuse([semantic_results, bm25_results])

    # 4. トリミング
    trimmed = _trim_to_budget(
        fused,
        max_tokens=budget_config["max_tokens"],
        max_results=budget_config["max_results"],
    )

    logger.info(
        "Recall complete: semantic=%d, bm25=%d, fused=%d, trimmed=%d",
        len(semantic_results),
        len(bm25_results),
        len(fused),
        len(trimmed),
    )

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
        "total_found": len(fused),
        "returned": len(trimmed),
        "budget": budget,
    }
