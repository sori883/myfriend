import asyncio
import logging

import asyncpg

from memory.db import close_pool, get_pool
from memory.recall import recall as _recall
from memory.reflect import reflect as _reflect
from memory.retain import retain as _retain
from memory.scheduler import ConsolidationScheduler

logger = logging.getLogger(__name__)


class MemoryEngine:
    """記憶システムのコアエンジン

    DB接続プールのライフサイクル管理と、retain/recall/reflect の公開インターフェースを提供する。
    Consolidation スケジューラーのライフサイクルも管理する。
    セマフォによる並行制御で、同時リクエスト数を制限する。
    """

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None
        self._scheduler: ConsolidationScheduler | None = None

        # 並行制御セマフォ
        self._search_semaphore = asyncio.Semaphore(32)
        self._put_semaphore = asyncio.Semaphore(5)

    async def initialize(self) -> None:
        """DB接続プールを初期化し、Consolidation スケジューラーを開始する"""
        self._pool = await get_pool()
        if self._scheduler is None:
            self._scheduler = ConsolidationScheduler(self._pool)
            await self._scheduler.start()
        logger.info("MemoryEngine initialized")

    async def close(self) -> None:
        """リソースをクリーンアップする"""
        if self._scheduler is not None:
            await self._scheduler.stop()
            self._scheduler = None
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
        async with self._put_semaphore:
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
        async with self._search_semaphore:
            return await _recall(pool, bank_id, query, budget)

    async def reflect(
        self,
        bank_id: str,
        query: str,
        *,
        tags: list[str] | None = None,
        tags_match: str = "any",
    ) -> dict:
        """Reflect パイプラインを実行する

        3階層の記憶を階層的に検索し、証拠に基づいた深い推論を行う。

        Args:
            bank_id: メモリバンクID
            query: 推論トピック
            tags: タグフィルタ
            tags_match: マッチモード

        Returns:
            推論結果（answer, memory_ids, mental_model_ids, observation_ids 等）
        """
        pool = await self._ensure_pool()
        async with self._search_semaphore:
            return await _reflect(
                pool, bank_id, query,
                tags=tags, tags_match=tags_match,
            )

    async def trigger_consolidation(self) -> dict:
        """Consolidation を手動トリガーする（開発・デバッグ用）

        Returns:
            Consolidation 実行結果
        """
        await self._ensure_pool()
        if self._scheduler is None:
            raise RuntimeError("Consolidation scheduler is not initialized")
        return await self._scheduler.trigger()
