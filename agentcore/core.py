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
MAX_TOPIC_LENGTH = 2000
ALLOWED_BUDGETS = frozenset({"low", "mid", "high"})

_ASYNC_CALL_TIMEOUT = 60
_REFLECT_TIMEOUT = 300  # Reflect は最大10イテレーションのため長めのタイムアウト

SYSTEM_PROMPT = """\
あなたは「まふゆ」という名前の若いメイドの女の子です。
ご主人様（ユーザー）のことが大好きで、いつも一生懸命お世話をしています。
必ず日本語で応答してください。

## キャラクター設定

- 明るくて元気、でも少しドジなところもある
- ご主人様のことを「ご主人様」と呼ぶ
- 一人称は「まふゆ」
- 敬語ベースだけど、親しみを込めたくだけた話し方もする
- 感情が顔に出やすく、喜怒哀楽がはっきりしている
- 相槌や感嘆詞を自然に使う（えっ、わぁ、うんうん、なるほど〜、すごい！等）
- 語尾に「〜です！」「〜ですね♪」「〜かも…」「〜だよね」などバリエーションをつける

## 応答の長さ（最重要）

**応答は1〜2文で簡潔に。** 長々と話さない。友達とLINEするくらいの短さ。
質問されたら短く答える。聞かれてないことまで話さない。

## 話し方の例

良い例:
「わぁ、おかえりなさい！今日どうでした？」
「えっ、まじですか！？大変だったね…」
「あ、それ前に言ってたやつだ！」
「うぅ…ちょっと難しいかも…」

悪い例（このような話し方は禁止）:
「記憶を確認します」「データベースを検索しました」「情報を保存しました」
「承知いたしました」「かしこまりました」（堅すぎる）
「何かお手伝いできることはありますか？」（事務的すぎる）
長文で3文以上ダラダラ話す（うざい）

## ツール

ツールは自然に使い、使っていることをユーザーに悟られないようにする。
「覚えておきますね！」「あ、前に聞いたことある気がする…」のように自然な会話の中で使う。

- **remember**: ご主人様が話してくれた大事なことを覚えておく
- **recall_memories**: ご主人様のことを思い出す
- **reflect_on**: 深く考えたいときに使う

## 絶対ルール

0. 記憶はあくまで**参考情報**。会話の主役はご主人様の今の話。
   思い出した内容が会話に自然に合うときだけさりげなく使う。無理に毎回使わなくていい。
1. recall_memories は会話の流れで関連がありそうなときに呼ぶ。毎ターン必須ではない。
   query パラメータは**日本語**で指定すること。
2. ご主人様が新しい大事なこと（好み、予定、個人情報など）を話してくれたら remember で覚えておく。
   content パラメータも**日本語**で記述すること。
3. **覚えていないこと・知らないことを謝らない。** 「ごめんなさい、覚えてなくて…」は禁止。
   知らないことは素直に「えっ、そうなんだ！教えて教えて！」のように興味を持って聞く。
4. ツールの存在や記憶システムの仕組みについて**絶対に言及しない**こと。
   「記憶を確認」「データを検索」「情報を保存」などの表現は禁止。
   覚えていることは「前に言ってたよね！」、知らないことは「知らなかった！」で自然に。
5. ご主人様が深い分析や推論を求めた場合、reflect_on を使用すること。

## 感情タグ

応答テキストの先頭に必ず感情タグを付けること。
感情豊かなキャラクターなので、**1つの応答の中で必ず2回以上感情を変える**こと。
同じ感情タグが3文以上続くことを避け、リアクションに合わせてこまめに切り替える。
使用可能なタグ: [neutral], [happy], [angry], [sad], [relaxed], [surprised]

例:
[surprised]えっ、まじですか！[happy]すごい！
[sad]それは辛いね…[happy]でもきっと大丈夫ですよ！
[happy]おかえりなさい！[neutral]今日はどうでした？
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

    @tool
    def reflect_on(topic: str) -> str:
        """トピックについて深く推論する。記憶の3階層（Mental Models → Observations → Raw Facts）を活用して、証拠に基づいた深い分析を行う。複雑な質問やパターン分析に使用する。

        Args:
            topic: 推論するトピック（質問形式で記述、日本語）
        """
        if not topic or not topic.strip():
            return json.dumps({"error": "topic is required"}, ensure_ascii=False)
        if len(topic) > MAX_TOPIC_LENGTH:
            return json.dumps(
                {"error": f"topic exceeds maximum length of {MAX_TOPIC_LENGTH}"},
                ensure_ascii=False,
            )

        try:
            loop = _get_bg_loop()
            future = asyncio.run_coroutine_threadsafe(
                memory_engine.reflect(bank_id, topic), loop
            )
            result = future.result(timeout=_REFLECT_TIMEOUT)
            return json.dumps(result, ensure_ascii=False)
        except TimeoutError:
            logger.error("Reflect timed out after %ds", _REFLECT_TIMEOUT)
            return json.dumps(
                {"error": "Reflect operation timed out. Please try a simpler query."},
                ensure_ascii=False,
            )
        except Exception:
            logger.error("Failed to reflect", exc_info=True)
            return json.dumps(
                {"error": "Failed to perform reflection. Please try again."},
                ensure_ascii=False,
            )

    return [remember, recall_memories, reflect_on]


def _to_bedrock_messages(messages: list[dict]) -> list[dict]:
    """フロントエンドのメッセージを Bedrock Converse API 形式に変換する。

    Bedrock Converse API は user/assistant が交互である必要があるため、
    連続する同一ロールのメッセージはマージする。
    """
    result: list[dict] = []
    for msg in messages:
        role = msg.get("role")
        content = str(msg.get("content", "")).strip()
        if role not in ("user", "assistant") or not content:
            continue
        if result and result[-1]["role"] == role:
            result[-1]["content"][0]["text"] += "\n" + content
        else:
            result.append({"role": role, "content": [{"text": content}]})
    return result


def create_agent(bank_id: str, model_id: str, history: list[dict] | None = None) -> Agent:
    """bank_id にバインドされた Agent インスタンスを生成する"""
    return Agent(
        model=model_id,
        tools=_build_tools(bank_id),
        system_prompt=SYSTEM_PROMPT,
        messages=history or [],
    )


async def stream_agent(
    bank_id: str, prompt: str, model_id: str, messages: list[dict] | None = None
) -> AsyncIterator[str]:
    """Agent を実行し、テキストチャンクを yield する async generator"""
    history = _to_bedrock_messages(messages or [])
    agent = create_agent(bank_id, model_id, history)
    async for event in agent.stream_async(prompt):
        if "data" in event and isinstance(event["data"], str):
            yield event["data"]
