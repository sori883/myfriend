import logging

import asyncpg

from memory.db import close_pool, get_pool
from memory.retain import retain as _retain
from memory.recall import recall as _recall

logger = logging.getLogger(__name__)


class MemoryEngine:
    """記憶システムのコアエンジン

    DB接続プールのライフサイクル管理と、retain/recall の公開インターフェースを提供する。
    """

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        """DB接続プールを初期化する"""
        self._pool = await get_pool()
        logger.info("MemoryEngine initialized")

    async def close(self) -> None:
        """リソースをクリーンアップする"""
        await close_pool()
        self._pool = None
        logger.info("MemoryEngine closed")

    async def _ensure_pool(self) -> asyncpg.Pool:
        """接続プールが利用可能であることを保証する"""
        if self._pool is None or self._pool._closed:
            await self.initialize()
        return self._pool

    async def retain(
        self,
        bank_id: str,
        content: str,
        context: str = "",
    ) -> dict:
        """会話テキストからファクトを抽出し、記憶として永続化する

        Args:
            bank_id: メモリバンクID
            content: 記憶する会話内容
            context: 追加コンテキスト情報

        Returns:
            保存結果（保存件数、ファクトID等）
        """
        pool = await self._ensure_pool()
        return await _retain(pool, bank_id, content, context)

    async def recall(
        self,
        bank_id: str,
        query: str,
        budget: str = "mid",
    ) -> dict:
        """記憶から関連情報を検索する

        Args:
            bank_id: メモリバンクID
            query: 検索クエリ
            budget: トークンバジェット ("low" / "mid" / "high")

        Returns:
            検索結果（記憶テキスト、スコア等）
        """
        pool = await self._ensure_pool()
        return await _recall(pool, bank_id, query, budget)
