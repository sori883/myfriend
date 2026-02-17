"""Strands Agent ツール定義の検証スクリプト

ツールが正しく定義され、バリデーションが機能するか、
bank_id クロージャが正しく動作するかを確認する。

Usage: uv run python test_script/test_tools.py
"""

import asyncio
import json
import logging
import sys
import threading
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from strands import tool

from memory.engine import MemoryEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

memory_engine = MemoryEngine()

MAX_CONTENT_LENGTH = 10000
MAX_QUERY_LENGTH = 1000
ALLOWED_BUDGETS = frozenset({"low", "mid", "high"})

BANK_ID = "00000000-0000-0000-0000-000000000001"

# 専用バックグラウンドイベントループ
_bg_loop: asyncio.AbstractEventLoop | None = None
_bg_thread: threading.Thread | None = None
_bg_lock = threading.Lock()


def _get_bg_loop() -> asyncio.AbstractEventLoop:
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
    loop = _get_bg_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=60)


def create_tools(bank_id: str):
    """invoke() と同じパターンでツールを定義し返す"""

    @tool
    def remember(content: str, context: str = "") -> str:
        """会話情報を長期記憶に保存する。"""
        if not content or not content.strip():
            return json.dumps({"error": "content is required"}, ensure_ascii=False)
        if len(content) > MAX_CONTENT_LENGTH:
            return json.dumps(
                {"error": f"content exceeds maximum length of {MAX_CONTENT_LENGTH}"},
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
        """長期記憶から関連情報を検索する。"""
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

    return remember, recall_memories


def test_tool_decorator():
    """@tool デコレータが正しく適用され、callable であることを確認"""
    remember_tool, recall_tool = create_tools(BANK_ID)
    assert callable(remember_tool), "remember should be callable"
    assert callable(recall_tool), "recall_memories should be callable"
    print("PASS: test_tool_decorator")


def test_remember_validation():
    """remember ツールの入力バリデーション"""
    remember_tool, _ = create_tools(BANK_ID)

    # 空文字列 → エラー
    result = json.loads(remember_tool(content=""))
    assert "error" in result, "Empty content should return error"

    # 空白のみ → エラー
    result = json.loads(remember_tool(content="   "))
    assert "error" in result, "Whitespace-only content should return error"

    # 長すぎるコンテンツ → エラー
    result = json.loads(remember_tool(content="x" * (MAX_CONTENT_LENGTH + 1)))
    assert "error" in result, "Too-long content should return error"

    print("PASS: test_remember_validation")


def test_recall_validation():
    """recall_memories ツールの入力バリデーション"""
    _, recall_tool = create_tools(BANK_ID)

    # 空文字列 → エラー
    result = json.loads(recall_tool(query=""))
    assert "error" in result, "Empty query should return error"

    # 長すぎるクエリ → エラー
    result = json.loads(recall_tool(query="x" * (MAX_QUERY_LENGTH + 1)))
    assert "error" in result, "Too-long query should return error"

    print("PASS: test_recall_validation")


def test_recall_budget_fallback():
    """無効な budget が 'mid' にフォールバックされることを確認"""
    _, recall_tool = create_tools(BANK_ID)

    # 無効なbudgetでもエラーにならない（midにフォールバック）
    result = json.loads(recall_tool(query="test query", budget="invalid"))
    assert "error" not in result, f"Should not error on invalid budget, got: {result}"
    assert result["budget"] == "mid", f"Should fallback to 'mid', got: {result['budget']}"

    print("PASS: test_recall_budget_fallback")


def test_bank_id_closure():
    """bank_id がクロージャで正しく固定されることを確認"""
    bank_a = "00000000-0000-0000-0000-000000000001"
    bank_b = "00000000-0000-0000-0000-000000000002"

    remember_a, _ = create_tools(bank_a)
    remember_b, _ = create_tools(bank_b)

    # ツールA で保存
    result_a = json.loads(remember_a(
        content="Closure test: this is bank A's memory.",
    ))
    logger.info("Bank A result: %s", result_a)

    # ツールB で検索 → bank_a のデータは見えないはず
    _, recall_b = create_tools(bank_b)
    result_b = json.loads(recall_b(
        query="Closure test bank A",
    ))
    logger.info("Bank B recall result: %s", result_b)

    # bank_b には bank_a のデータは含まれない
    for m in result_b.get("memories", []):
        assert "bank A" not in m["text"].lower(), "bank_b should not see bank_a's memories"

    print("PASS: test_bank_id_closure")


def test_remember_actual():
    """remember ツールで実際に DB 保存できることを確認"""
    remember_tool, _ = create_tools(BANK_ID)

    result = json.loads(remember_tool(
        content="I met Tanaka-san at the coffee shop. He mentioned he is moving to Osaka next week.",
        context="Casual conversation",
    ))
    logger.info("Remember result: %s", result)

    assert "error" not in result, f"Remember failed: {result}"
    assert result["stored"] > 0, f"No facts stored: {result}"
    print("PASS: test_remember_actual (stored %d facts)" % result["stored"])


def test_recall_actual():
    """recall_memories ツールで実際に検索できることを確認"""
    _, recall_tool = create_tools(BANK_ID)

    result = json.loads(recall_tool(
        query="Who is moving to Osaka?",
        budget="low",
    ))
    logger.info("Recall result: %s", result)

    assert "error" not in result, f"Recall failed: {result}"
    assert result["returned"] > 0, f"No results returned: {result}"
    print("PASS: test_recall_actual (returned %d results)" % result["returned"])


async def cleanup():
    """テストデータをクリア"""
    from memory.db import get_pool, close_pool
    pool = await get_pool()
    for bid in [BANK_ID, "00000000-0000-0000-0000-000000000002"]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM unit_entities WHERE unit_id IN (SELECT id FROM memory_units WHERE bank_id = $1::uuid)",
                    bid,
                )
                await conn.execute("DELETE FROM memory_units WHERE bank_id = $1::uuid", bid)
                await conn.execute("DELETE FROM entities WHERE bank_id = $1::uuid", bid)
    # プールを閉じて、ツール側で新しいループ用のプールを作り直させる
    await close_pool()


if __name__ == "__main__":
    # DB を使うテストの前にクリーンアップ（プールも閉じる）
    _run_async(cleanup())
    logger.info("=== Cleanup done ===\n")

    test_tool_decorator()
    test_remember_validation()
    test_recall_validation()
    test_recall_budget_fallback()
    test_remember_actual()
    test_recall_actual()
    test_bank_id_closure()

    # 後片付け
    _run_async(cleanup())

    print("\n=== All Strands Agent tool tests passed ===")
