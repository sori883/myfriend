"""クロスエンコーダリランキングモジュール

Bedrock Rerank API (amazon.rerank-v1:0) を使用して
RRF 融合後の候補をリランキングする。
"""

import asyncio
import logging
import os
import threading
from datetime import datetime

import boto3

logger = logging.getLogger(__name__)

_DEFAULT_RERANK_MODEL_ID = "amazon.rerank-v1:0"


def _get_rerank_model_id() -> str:
    return os.environ.get("RERANK_MODEL_ID", _DEFAULT_RERANK_MODEL_ID)


def _get_rerank_model_arn() -> str:
    region = os.environ.get("AWS_REGION", "ap-northeast-1")
    model_id = _get_rerank_model_id()
    return f"arn:aws:bedrock:{region}::foundation-model/{model_id}"

# リランク候補の上限（API は最大 1,000 件対応）
RERANK_CANDIDATE_LIMIT = 300

# ---------- クライアント ----------

_client = None
_lock = threading.Lock()


def _get_bedrock_agent_runtime_client():
    """bedrock-agent-runtime クライアントを取得する（スレッドセーフ）"""
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                _client = boto3.client(
                    "bedrock-agent-runtime",
                    region_name=os.environ.get("AWS_REGION", "ap-northeast-1"),
                )
    return _client


# ---------- ドキュメント構築 ----------


def build_rerank_document(
    text: str,
    context: str | None,
    occurred_start: datetime | None = None,
) -> str:
    """リランク用ドキュメントテキストを構築する

    Hindsight 参考: 日付情報があれば先頭に付与して
    クロスエンコーダの時間認識を向上させる。
    """
    doc = text
    if context:
        doc = f"{context}: {doc}"
    if occurred_start is not None:
        date_iso = occurred_start.strftime("%Y-%m-%d")
        doc = f"[Date: {date_iso}] {doc}"
    return doc


# ---------- 公開 API ----------


def _invoke_rerank(
    query: str,
    documents: list[str],
    top_n: int,
) -> list[dict]:
    """Bedrock Rerank API を同期呼び出しする"""
    client = _get_bedrock_agent_runtime_client()

    sources = [
        {
            "type": "INLINE",
            "inlineDocumentSource": {
                "type": "TEXT",
                "textDocument": {"text": doc},
            },
        }
        for doc in documents
    ]

    response = client.rerank(
        queries=[
            {
                "textQuery": {"text": query},
                "type": "TEXT",
            }
        ],
        sources=sources,
        rerankingConfiguration={
            "type": "BEDROCK_RERANKING_MODEL",
            "bedrockRerankingConfiguration": {
                "modelConfiguration": {
                    "modelArn": _get_rerank_model_arn(),
                },
                "numberOfResults": top_n,
            },
        },
    )

    return [
        {
            "index": r["index"],
            "relevance_score": r["relevanceScore"],
        }
        for r in response["results"]
    ]


def reset_client() -> None:
    """キャッシュ済みクライアントをリセットする（テスト・認証情報ローテーション用）"""
    global _client
    with _lock:
        _client = None


async def rerank(
    query: str,
    candidates: list[str],
    top_n: int | None = None,
) -> list[tuple[int, float]]:
    """候補をリランキングし (元index, relevanceScore) のリストを返す

    Args:
        query: 検索クエリ
        candidates: リランク対象のドキュメントテキストリスト
        top_n: 返却する上位件数（None で全件）

    Returns:
        (元の candidates index, relevance_score) のリスト（スコア降順）
    """
    if not candidates:
        return []

    effective_top_n = top_n if top_n is not None else len(candidates)

    results = await asyncio.to_thread(
        _invoke_rerank, query, candidates, effective_top_n
    )

    return [(r["index"], r["relevance_score"]) for r in results]
