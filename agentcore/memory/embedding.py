"""Embedding 生成モジュール

Bedrock Titan Embed V2 を使用して 1024 次元ベクトルを生成する。
"""

import asyncio
import json
import logging
import os

from memory.bedrock_client import get_bedrock_runtime_client

logger = logging.getLogger(__name__)

EMBEDDING_MODEL_ID = os.environ.get(
    "EMBEDDING_MODEL_ID",
    "amazon.titan-embed-text-v2:0",
)

EMBEDDING_DIMENSIONS = 1024

# Titan Embed V2 の入力上限 8192 トークン（安全マージンを含む文字数）
MAX_INPUT_CHARS = 24000

# API スロットリング防止のための並列数上限
_EMBEDDING_CONCURRENCY = 5


def _invoke_embedding(text: str) -> list[float]:
    """Titan Embed V2 を同期呼び出し"""
    if len(text) > MAX_INPUT_CHARS:
        logger.warning(
            "Text truncated for embedding: %d -> %d chars", len(text), MAX_INPUT_CHARS
        )
        text = text[:MAX_INPUT_CHARS]

    client = get_bedrock_runtime_client()
    response = client.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        body=json.dumps({
            "inputText": text,
            "dimensions": EMBEDDING_DIMENSIONS,
            "normalize": True,
        }),
    )
    result = json.loads(response["body"].read())
    return result["embedding"]


async def generate_embedding(text: str) -> list[float]:
    """単一テキストの Embedding を生成する

    Args:
        text: 埋め込み対象のテキスト

    Returns:
        1024 次元の浮動小数点ベクトル
    """
    return await asyncio.to_thread(_invoke_embedding, text)


async def generate_embeddings(texts: list[str]) -> list[list[float]]:
    """複数テキストの Embedding を並列生成する

    並列数は _EMBEDDING_CONCURRENCY で制限される。

    Args:
        texts: 埋め込み対象のテキストリスト

    Returns:
        各テキストに対応する 1024 次元ベクトルのリスト
    """
    semaphore = asyncio.Semaphore(_EMBEDDING_CONCURRENCY)

    async def _with_limit(text: str) -> list[float]:
        async with semaphore:
            return await asyncio.to_thread(_invoke_embedding, text)

    tasks = [_with_limit(text) for text in texts]
    results = await asyncio.gather(*tasks)
    logger.info("Generated %d embeddings", len(results))
    return list(results)
