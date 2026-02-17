from dotenv import load_dotenv
load_dotenv()

# Threading model:
# invoke() は BedrockAgentCore から async で呼ばれる。
# ツール (remember, recall_memories) は sync 関数として定義される。
# async の memory_engine メソッドを呼ぶために、専用のバックグラウンドイベントループを
# daemon スレッドで永続化し、run_coroutine_threadsafe で投入する。
# asyncpg プールはこのバックグラウンドループに紐付くため、ループを使い回すことで
# "attached to a different loop" エラーを防ぐ。

import asyncio
import atexit
import json
import logging
import os
import threading
import uuid

from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent
from strands import tool

from memory.engine import MemoryEngine

logger = logging.getLogger(__name__)

app = BedrockAgentCoreApp()

memory_engine = MemoryEngine()

MAX_CONTENT_LENGTH = 10000
MAX_QUERY_LENGTH = 1000
MAX_CONTEXT_LENGTH = 2000
ALLOWED_BUDGETS = frozenset({"low", "mid", "high"})

AGENT_MODEL_ID = os.environ.get("AGENT_MODEL_ID", "openai.gpt-oss-120b-1:0")

# async 操作のタイムアウト（秒）: LLM 呼び出し + DB 操作を含む
_ASYNC_CALL_TIMEOUT = 60

# 専用バックグラウンドイベントループ（asyncpg プールとの互換性を保つ）
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


def _shutdown():
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
            _bg_loop = None
            _bg_thread = None


atexit.register(_shutdown)


def _validate_bank_id(bank_id: str) -> str:
    """bank_id が有効な UUID であることを検証する"""
    try:
        return str(uuid.UUID(bank_id))
    except (ValueError, TypeError) as e:
        raise ValueError("Invalid bank_id format. Expected UUID.") from e


SYSTEM_PROMPT = """\
You are a helpful assistant with long-term memory capabilities.

You have access to memory tools that allow you to remember and recall information across conversations:

- **remember**: Store important facts from conversations. Use this when users share personal information,
  preferences, plans, or any noteworthy details worth remembering for future interactions.
- **recall_memories**: Search your long-term memory for relevant information. Use this at the start of
  conversations or when context about the user would be helpful.

Guidelines:
- Proactively remember important facts without being asked.
- Recall relevant memories when they would enrich the conversation.
- Do not mention the memory system mechanics to the user unless asked.
"""


@app.entrypoint
async def invoke(payload):
    try:
        bank_id = _validate_bank_id(payload.get("bank_id", ""))
    except ValueError:
        yield json.dumps({"error": "Invalid or missing bank_id. Expected a valid UUID."})
        return

    prompt = payload.get("prompt")
    if not prompt or not str(prompt).strip():
        yield json.dumps({"error": "prompt is required."})
        return

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
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to recall memories", exc_info=True)
            return json.dumps(
                {"error": "Failed to search memories. Please try again."},
                ensure_ascii=False,
            )

    agent = Agent(
        model=AGENT_MODEL_ID,
        tools=[remember, recall_memories],
        system_prompt=SYSTEM_PROMPT,
    )

    stream = agent.stream_async(prompt)

    async for event in stream:
        if "data" in event and isinstance(event["data"], str):
            yield event["data"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run()
