"""嗜好抽出エンジン

PreferenceEngine は嗜好パイプラインの公開インターフェースを提供する。
memory.db の接続プールを共有し、セマフォによる並行制御を行う。
"""

import asyncio
import logging

import asyncpg

from memory.db import get_pool
from recommendation.preference_extractor import (
    extract_preferences,
    persist_preferences,
)
from recommendation.preference_query import (
    get_owner_entity,
    query_preferences,
)
from recommendation.recommendation import (
    get_recommendations,
    record_feedback,
)
from recommendation.web_search import SearchRateLimiter, search_web

logger = logging.getLogger(__name__)


class PreferenceEngine:
    """嗜好抽出パイプラインのファサード"""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None
        self._semaphore = asyncio.Semaphore(5)
        self._rate_limiter = SearchRateLimiter()

    async def _ensure_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await get_pool()
        return self._pool

    async def extract(
        self,
        bank_id: str,
        content: str,
        context: str = "",
    ) -> dict:
        """会話テキストから嗜好を抽出し永続化する

        処理フロー:
        1. LLM で嗜好シグナル抽出
        2. entity_id 解決 (owner_entity_id フォールバック)
        3. item 名寄せ + UPSERT
        """
        pool = await self._ensure_pool()
        async with self._semaphore:
            return await self._extract_impl(pool, bank_id, content, context)

    async def query_profile(
        self,
        bank_id: str,
        category: str = "",
    ) -> dict:
        """嗜好プロファイルを取得する"""
        pool = await self._ensure_pool()
        owner = await get_owner_entity(pool, bank_id)
        if owner is None:
            return {
                "preferences": {},
                "total_count": 0,
                "message": "まだ好みの情報がありません",
            }
        entity_id, entity_name = owner
        result = await query_preferences(pool, bank_id, entity_id, category)
        return {**result, "entity": entity_name}

    async def recommend(
        self,
        bank_id: str,
        category: str,
        context: str = "",
    ) -> dict:
        """嗜好ベースのレコメンデーションを生成する"""
        pool = await self._ensure_pool()
        owner = await get_owner_entity(pool, bank_id)
        if owner is None:
            return {
                "recommendations": [],
                "avoid": [],
                "category": category,
                "message": "まだ好みの情報がありません",
            }
        entity_id = owner[0]
        async with self._semaphore:
            return await get_recommendations(
                pool, bank_id, entity_id, category, context,
            )

    async def record_recommendation_feedback(
        self,
        bank_id: str,
        recommendation_id: str,
        accepted: bool,
        accepted_item: str | None = None,
    ) -> dict:
        """推薦フィードバックを記録する"""
        pool = await self._ensure_pool()
        return await record_feedback(
            pool, bank_id, recommendation_id, accepted, accepted_item,
        )

    async def search(self, bank_id: str, query: str) -> dict:
        """Web 検索を実行する（レート制限・キャッシュ付き）"""
        return await search_web(self._rate_limiter, bank_id, query)

    async def _extract_impl(
        self,
        pool: asyncpg.Pool,
        bank_id: str,
        content: str,
        context: str,
    ) -> dict:
        preferences = await extract_preferences(content, context)
        if not preferences:
            return {"stored": 0, "skipped_reason": "no_preferences_detected"}

        owner = await get_owner_entity(pool, bank_id)
        if owner is None:
            return {"stored": 0, "skipped_reason": "no_entity_resolved"}

        entity_id = owner[0]
        return await persist_preferences(
            pool, bank_id, preferences, entity_id,
        )
