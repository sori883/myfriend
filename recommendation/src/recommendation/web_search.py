"""Web 検索モジュール

Tavily API を使った検索クライアント、レート制限、インメモリキャッシュを提供する。
"""

import asyncio
import logging
import os
import time
from datetime import date

from tavily import TavilyClient

logger = logging.getLogger(__name__)

MAX_SEARCH_RESULTS = 5
CACHE_TTL_SECONDS = 900  # 15分
MAX_CACHE_SIZE = 100


# ---------------------------------------------------------------------------
# Tavily client singleton
# ---------------------------------------------------------------------------

_client: TavilyClient | None = None


def _get_client() -> TavilyClient:
    """TavilyClient のシングルトンを取得する"""
    global _client
    if _client is None:
        api_key = os.environ.get("TAVILY_API_KEY")
        if not api_key:
            raise RuntimeError("TAVILY_API_KEY environment variable is not set")
        _client = TavilyClient(api_key=api_key)
    return _client


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class SearchRateLimiter:
    """検索のレート制限を管理する

    - 1会話（セッション）あたり最大 3 回
    - 1日あたり最大 20 回（bank 単位）
    - 最小検索間隔 10 秒
    """

    MAX_PER_SESSION = 3
    MAX_PER_DAY = 20
    MIN_INTERVAL_SECONDS = 10

    def __init__(self) -> None:
        self._session_count = 0
        self._daily_counts: dict[str, tuple[int, date]] = {}
        self._last_search_at: float = 0.0

    def check(self, bank_id: str) -> str | None:
        """レート制限をチェックする

        Returns:
            制限に達している場合はエラーメッセージ、OK なら None
        """
        if self._session_count >= self.MAX_PER_SESSION:
            return f"この会話での検索回数上限（{self.MAX_PER_SESSION}回）に達しました"

        daily_count, count_date = self._daily_counts.get(bank_id, (0, date.min))
        if count_date == date.today() and daily_count >= self.MAX_PER_DAY:
            return f"本日の検索回数上限（{self.MAX_PER_DAY}回）に達しました"

        elapsed = time.monotonic() - self._last_search_at
        if self._last_search_at > 0 and elapsed < self.MIN_INTERVAL_SECONDS:
            remaining = int(self.MIN_INTERVAL_SECONDS - elapsed)
            return f"検索間隔が短すぎます。{remaining}秒後にお試しください"

        return None

    def record(self, bank_id: str) -> None:
        """検索実行を記録する"""
        self._session_count += 1
        self._last_search_at = time.monotonic()

        today = date.today()
        daily_count, count_date = self._daily_counts.get(bank_id, (0, date.min))
        if count_date == today:
            self._daily_counts[bank_id] = (daily_count + 1, today)
        else:
            self._daily_counts[bank_id] = (1, today)

    @property
    def session_remaining(self) -> int:
        """セッション内の残り検索回数"""
        return max(0, self.MAX_PER_SESSION - self._session_count)

    def daily_remaining(self, bank_id: str) -> int:
        """bank_id の本日の残り検索回数"""
        daily_count, count_date = self._daily_counts.get(bank_id, (0, date.min))
        if count_date != date.today():
            return self.MAX_PER_DAY
        return max(0, self.MAX_PER_DAY - daily_count)


# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_search_cache: dict[str, tuple[dict, float]] = {}
_search_lock = asyncio.Lock()


def _normalize_query(query: str) -> str:
    """キャッシュキー用にクエリを正規化する"""
    return query.strip().lower()


def _get_cached(query: str) -> dict | None:
    """キャッシュから検索結果を取得する（TTL チェック付き）"""
    key = _normalize_query(query)
    entry = _search_cache.get(key)
    if entry is None:
        return None
    result, cached_at = entry
    if time.monotonic() - cached_at > CACHE_TTL_SECONDS:
        del _search_cache[key]
        return None
    return result


def _set_cache(query: str, result: dict) -> None:
    """検索結果をキャッシュに保存する（サイズ上限付き）"""
    key = _normalize_query(query)
    if len(_search_cache) >= MAX_CACHE_SIZE and key not in _search_cache:
        oldest_key = min(_search_cache, key=lambda k: _search_cache[k][1])
        del _search_cache[oldest_key]
    _search_cache[key] = (result, time.monotonic())


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def _call_tavily(query: str) -> dict:
    """Tavily API を同期呼び出しし、検索結果を取得する"""
    client = _get_client()
    response = client.search(
        query=query,
        max_results=MAX_SEARCH_RESULTS,
        search_depth="basic",
        topic="general",
    )
    return response


def _format_response(raw: dict, query: str) -> dict:
    """Tavily レスポンスを整形する"""
    raw_results = raw.get("results")
    if not isinstance(raw_results, list):
        return {"results": [], "query": query}
    results = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        results.append({
            "title": item.get("title", ""),
            "snippet": item.get("content", ""),
            "url": item.get("url", ""),
        })
    return {"results": results, "query": query}


async def search_web(
    rate_limiter: SearchRateLimiter,
    bank_id: str,
    query: str,
) -> dict:
    """Web 検索を実行する（レート制限・キャッシュ付き）

    Args:
        rate_limiter: レート制限インスタンス
        bank_id: メモリバンクID
        query: 検索クエリ

    Returns:
        検索結果辞書 {"results": [...], "query": "..."}
    """
    async with _search_lock:
        cached = _get_cached(query)
        if cached is not None:
            logger.info("Cache hit for query: %s", query)
            return cached

        error = rate_limiter.check(bank_id)
        if error is not None:
            return {
                "error": error,
                "remaining_today": rate_limiter.daily_remaining(bank_id),
                "remaining_session": rate_limiter.session_remaining,
            }

    try:
        raw = await asyncio.to_thread(_call_tavily, query)
    except RuntimeError:
        raise
    except Exception:
        logger.error("Tavily API call failed", exc_info=True)
        return {"error": "検索中にエラーが発生しました。しばらくしてからお試しください", "query": query}

    result = _format_response(raw, query)

    async with _search_lock:
        _set_cache(query, result)
        rate_limiter.record(bank_id)

    return result
