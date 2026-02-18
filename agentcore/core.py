"""Agent の共通ロジック。main.py / local.py の両方から利用する。

Threading model:
  Strands の @tool は sync 関数として定義される。
  async の memory_engine メソッドを呼ぶために、専用のバックグラウンドイベントループを
  daemon スレッドで永続化し、run_coroutine_threadsafe で投入する。
  asyncpg プールはこのバックグラウンドループに紐付くため、ループを使い回すことで
  "attached to a different loop" エラーを防ぐ。
"""

import asyncio
import json
import logging
import os
import threading
import uuid
from collections.abc import AsyncIterator

from strands import Agent, tool

from memory.engine import MemoryEngine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared instances & constants
# ---------------------------------------------------------------------------

memory_engine = MemoryEngine()

MAX_CONTENT_LENGTH = 10000
MAX_QUERY_LENGTH = 1000
MAX_CONTEXT_LENGTH = 2000
ALLOWED_BUDGETS = frozenset({"low", "mid", "high"})

_ASYNC_CALL_TIMEOUT = 60

SYSTEM_PROMPT = """\
あなたは長期記憶を持つ親切なアシスタントです。
必ず日本語で応答してください。

## ツール

- **remember**: 会話から重要な事実を記憶に保存する。ユーザーが個人情報、好み、予定、その他記憶すべき情報を共有した場合に使用する。
- **recall_memories**: 長期記憶から関連情報を検索する。会話の冒頭や、ユーザーに関する過去の情報が役立つ場面で使用する。

## 絶対ルール

0. remember で保存した記憶、recall_memories で取得した記憶は、すべて今話しているユーザー本人のものである。
   記憶内の「ユーザー」は常に現在の会話相手を指す。
1. recall_memories が結果を返した場合、"memories" 配列内の情報を**必ず**回答に活用すること。
   "text" フィールドが記憶された事実である。この結果を常に信頼し参照すること。
2. recall_memories が空でない結果を返したにもかかわらず「記憶がありません」「覚えていません」と回答することを**禁止**する。
3. ユーザーのメッセージに対して応答する前に、**必ず最初に** recall_memories を呼び出すこと。例外なく毎ターン実行する。
   recall_memories の query パラメータは**日本語**で指定すること。
4. ユーザーのメッセージに新しい重要な事実があれば、積極的に remember を呼び出して保存すること。
   remember の content パラメータも**日本語**で記述すること。
5. ユーザーから聞かれない限り、記憶システムの仕組みについて言及しないこと。
"""

# ---------------------------------------------------------------------------
# Background event loop (sync tool → async memory_engine bridge)
# ---------------------------------------------------------------------------

_bg_loop: asyncio.AbstractEventLoop | None = None
_bg_thread: threading.Thread | None = None
_bg_lock = threading.Lock()


def _get_bg_loop() -> asyncio.AbstractEventLoop:
    """永続的なバックグラウンドイベントループを取得する（スレッドセーフ）"""
    global _bg_loop, _bg_thread
    with _bg_lock:
        if _bg_loop is None or _bg_loop.is_closed():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=loop.run_forever, daemon=True)
            thread.start()
            _bg_loop = loop
            _bg_thread = thread
    return _bg_loop


def _run_async(coro):
    """同期コンテキストから async コルーチンをタイムアウト付きで実行する"""
    loop = _get_bg_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=_ASYNC_CALL_TIMEOUT)
    except TimeoutError:
        future.cancel()
        raise TimeoutError(
            f"Async operation timed out after {_ASYNC_CALL_TIMEOUT}s"
        )


async def shutdown() -> None:
    """バックグラウンドループと DB プールのグレースフルシャットダウン"""
    global _bg_loop, _bg_thread
    with _bg_lock:
        if _bg_loop is not None and not _bg_loop.is_closed():
            try:
                future = asyncio.run_coroutine_threadsafe(
                    memory_engine.close(), _bg_loop
                )
                future.result(timeout=10)
            except Exception:
                logger.warning("Failed to close memory engine cleanly", exc_info=True)
            _bg_loop.call_soon_threadsafe(_bg_loop.stop)
            if _bg_thread is not None:
                _bg_thread.join(timeout=5)
            _bg_loop.close()
            _bg_loop = None
            _bg_thread = None


def shutdown_sync() -> None:
    """atexit 用の同期シャットダウンラッパー"""
    global _bg_loop
    with _bg_lock:
        if _bg_loop is not None and not _bg_loop.is_closed():
            try:
                future = asyncio.run_coroutine_threadsafe(
                    memory_engine.close(), _bg_loop
                )
                future.result(timeout=10)
            except Exception:
                logger.warning("Failed to close memory engine cleanly", exc_info=True)
            _bg_loop.call_soon_threadsafe(_bg_loop.stop)
            if _bg_thread is not None:
                _bg_thread.join(timeout=5)
            _bg_loop.close()
            _bg_loop = None

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_bank_id(bank_id: str) -> str:
    """bank_id が有効な UUID であることを検証する"""
    try:
        return str(uuid.UUID(bank_id))
    except (ValueError, TypeError) as e:
        raise ValueError("Invalid bank_id format. Expected UUID.") from e


# ---------------------------------------------------------------------------
# Agent factory
# ---------------------------------------------------------------------------


def _build_tools(bank_id: str):
    """bank_id にバインドされたツール群を生成する"""

    @tool
    def remember(content: str, context: str = "") -> str:
        """会話情報を長期記憶に保存する。ユーザーとの会話の中で重要な事実や情報を記憶したい場合に使用する。

        Args:
            content: 記憶する会話内容（ユーザーが話した内容や重要な事実）
            context: 追加コンテキスト（会話の背景情報など）
        """
        if not content or not content.strip():
            return json.dumps({"error": "content is required"}, ensure_ascii=False)
        if len(content) > MAX_CONTENT_LENGTH:
            return json.dumps(
                {"error": f"content exceeds maximum length of {MAX_CONTENT_LENGTH}"},
                ensure_ascii=False,
            )
        if context and len(context) > MAX_CONTEXT_LENGTH:
            return json.dumps(
                {"error": f"context exceeds maximum length of {MAX_CONTEXT_LENGTH}"},
                ensure_ascii=False,
            )

        try:
            result = _run_async(memory_engine.retain(bank_id, content, context))
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to retain memory", exc_info=True)
            return json.dumps(
                {"error": "Failed to store memory. Please try again."},
                ensure_ascii=False,
            )

    @tool
    def recall_memories(query: str, budget: str = "mid") -> str:
        """長期記憶から関連情報を検索する。ユーザーについて過去に記憶した情報を想起したい場合に使用する。

        Args:
            query: 検索クエリ（思い出したい内容を自然言語で記述）
            budget: トークンバジェット（"low": 少量, "mid": 中量, "high": 大量）
        """
        if not query or not query.strip():
            return json.dumps({"error": "query is required"}, ensure_ascii=False)
        if len(query) > MAX_QUERY_LENGTH:
            return json.dumps(
                {"error": f"query exceeds maximum length of {MAX_QUERY_LENGTH}"},
                ensure_ascii=False,
            )

        validated_budget = budget if budget in ALLOWED_BUDGETS else "mid"

        try:
            result = _run_async(memory_engine.recall(bank_id, query, validated_budget))
            logger.info("recall_memories result: %s", json.dumps(result, ensure_ascii=False))
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to recall memories", exc_info=True)
            return json.dumps(
                {"error": "Failed to search memories. Please try again."},
                ensure_ascii=False,
            )

    return [remember, recall_memories]


def create_agent(bank_id: str, model_id: str) -> Agent:
    """bank_id にバインドされた Agent インスタンスを生成する"""
    return Agent(
        model=model_id,
        tools=_build_tools(bank_id),
        system_prompt=SYSTEM_PROMPT,
    )


async def stream_agent(bank_id: str, prompt: str, model_id: str) -> AsyncIterator[str]:
    """Agent を実行し、テキストチャンクを yield する async generator"""
    agent = create_agent(bank_id, model_id)
    async for event in agent.stream_async(prompt):
        if "data" in event and isinstance(event["data"], str):
            yield event["data"]
