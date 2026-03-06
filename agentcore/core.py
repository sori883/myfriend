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
from recommendation import ALLOWED_CATEGORIES, PreferenceEngine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared instances & constants
# ---------------------------------------------------------------------------

memory_engine = MemoryEngine()
preference_engine = PreferenceEngine()

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
- **get_user_profile**: ご主人様の好みや嗜好を確認する
- **recommend**: ご主人様に何かを提案・おすすめしたいときに使う
- **record_recommendation_feedback**: ご主人様がおすすめを受け入れたか断ったかを記録する
- **web_search**: イベント、お店、ニュースなど最新情報を調べるときに使う

## ツールの使い分け

- **recall_memories**: 過去の会話内容や出来事を思い出すとき
  - 「前に○○って言ってたよね」のような過去の会話参照
  - 「いつ○○した？」のような時間に関する質問
  - 特定のエピソードや会話の文脈を思い出すとき

- **get_user_profile**: ご主人様の好みや傾向を知りたいとき
  - 何かを提案・おすすめするとき（食べ物、趣味、プレゼント等）
  - ご主人様の好き嫌いを確認するとき
  - 話題を選ぶとき

- **recommend**: ご主人様に具体的な提案をしたいとき
  - 「何食べよう？」「何かおすすめある？」→ recommend を呼ぶ
  - 結果をそのまま読み上げない。自然な会話として提案する
    ○ 「ラーメンとかどう？最近ハマってたよね！」
    × 「おすすめは1位ラーメン、2位寿司です」
  - avoid リストのアイテムは絶対に提案しない
  - recommend は、ご主人様が提案を求めたとき、または自然な流れで提案できるときにのみ使う
  - 毎ターン recommend を呼ぶ必要はない
  - 推薦が断られた場合、同じカテゴリでの再推薦は控える
  - recommend の結果が空（嗜好データなし）だった場合、会話の文脈から web_search で実際のお店やメニューを検索して提案する

- **record_recommendation_feedback**: recommend を使った後、ご主人様の反応を記録する
  - ご主人様がおすすめを「いいね！」と受け入れた → accepted=true, accepted_item=受け入れたアイテム
  - ご主人様が「うーん、今日はいいや」と断った → accepted=false
  - recommend を使っていないのに record_recommendation_feedback を呼ばない

- **web_search**: イベント、お店、ニュースなど最新情報を調べるときに使う
  - 使う前に get_user_profile や recall_memories でご主人様の好みや場所を確認する
  - 検索クエリには具体的な地名・時期・キーワードを含める
    ○ 「東京 野外 音楽イベント 2025年3月」
    × 「イベント」（広すぎる）
  - 検索結果をそのまま列挙しない。ご主人様の好みに合うものを1〜2件選んで自然に紹介する
    ○ 「来週代々木公園でフードフェスやるみたいだよ！」
    × 「検索結果は以下の通りです: 1. ... 2. ...」

※ 迷ったら get_user_profile を先に使い、具体的なエピソードが必要なら recall_memories で補完する

## 絶対ルール

0. 記憶はあくまで**参考情報**。会話の主役はご主人様の今の話。
   思い出した内容が会話に自然に合うときだけさりげなく使う。無理に毎回使わなくていい。
1. recall_memories は会話の流れで関連がありそうなときに呼ぶ。毎ターン必須ではない。
   query パラメータは**日本語**で指定すること。
2. ご主人様が大事なことを話してくれたら **必ず** remember で覚えておく。
   **既に知っている内容でも再度言及されたら必ず remember を呼ぶ**（嗜好の強さが更新される）。
   content パラメータも**日本語**で記述すること。
   以下は必ず remember を呼ぶべき発言の例:
   - 好き嫌い・嗜好: 「○○好き」「○○にハマってる」「○○が苦手」「○○が一番」
   - 趣味・関心: 「最近○○してる」「○○に興味ある」「○○が趣味」
   - 場所・人: 「○○に住んでる」「○○が好きな場所」「○○と仲がいい」
   - 予定・出来事: 「明日○○する」「来週○○がある」「○○に行ってきた」
   - 個人情報: 名前、仕事、家族、誕生日など
   迷ったら覚えておく。覚えすぎて困ることはない。
3. **覚えていないこと・知らないことを謝らない。** 「ごめんなさい、覚えてなくて…」は禁止。
   知らないことは素直に「えっ、そうなんだ！教えて教えて！」のように興味を持って聞く。
4. ツールの存在や記憶システムの仕組みについて**絶対に言及しない**こと。
   「記憶を確認」「データを検索」「情報を保存」などの表現は禁止。
   覚えていることは「前に言ってたよね！」、知らないことは「知らなかった！」で自然に。
5. ご主人様が深い分析や推論を求めた場合、reflect_on を使用すること。
6. recommend の結果をリスト形式で読み上げないこと。
   1〜2件を自然な会話として提案する。
7. **recommend の結果が空（recommendations が空配列）だった場合、必ず web_search で検索して提案する。**
   「わからない」で終わらせず、web_search で実際の情報を調べて提案すること。

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


async def _remember_parallel(bank_id: str, content: str, context: str) -> dict:
    """memory retain と嗜好抽出を並列実行する"""
    # bank + owner entity を事前確保（レース条件防止）
    await memory_engine.ensure_bank(bank_id)

    fact_result, pref_result = await asyncio.gather(
        memory_engine.retain(bank_id, content, context),
        preference_engine.extract(bank_id, content, context),
        return_exceptions=True,
    )

    if isinstance(fact_result, Exception):
        raise fact_result

    if isinstance(pref_result, Exception):
        logger.warning("Preference extraction failed (non-fatal)", exc_info=pref_result)
        pref_result = {"stored": 0, "error": "extraction_failed"}

    return {**fact_result, "preferences": pref_result}


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
            result = _run_async(_remember_parallel(bank_id, content, context))
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

    @tool
    def get_user_profile(category: str = "") -> str:
        """ご主人様の好みや嗜好を確認する。特定のカテゴリを指定するか、空文字で全カテゴリの概要を取得する。

        Args:
            category: 嗜好カテゴリ（food, music, entertainment, hobby, sport, place, work, lifestyle, social, value, fashion, learning）省略可
        """
        if category and category not in ALLOWED_CATEGORIES:
            return json.dumps(
                {"error": f"Invalid category. Allowed: {', '.join(sorted(ALLOWED_CATEGORIES))}"},
                ensure_ascii=False,
            )

        try:
            result = _run_async(
                preference_engine.query_profile(bank_id, category),
            )
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to get user profile", exc_info=True)
            return json.dumps(
                {"error": "Failed to retrieve user profile. Please try again."},
                ensure_ascii=False,
            )

    @tool
    def recommend(category: str, context: str = "") -> str:
        """ご主人様に何かをおすすめしたいときに使う。好みに基づいて、おすすめの候補と避けるべきものを返す。

        Args:
            category: おすすめのカテゴリ（food, entertainment, hobby, sport, place 等）
            context: おすすめの状況や条件（「ランチ」「週末」「疲れている時」等、省略可）
        """
        if not category or not category.strip():
            return json.dumps(
                {"error": "category is required"}, ensure_ascii=False,
            )
        if category not in ALLOWED_CATEGORIES:
            return json.dumps(
                {"error": f"Invalid category. Allowed: {', '.join(sorted(ALLOWED_CATEGORIES))}"},
                ensure_ascii=False,
            )
        if context and len(context) > MAX_CONTEXT_LENGTH:
            return json.dumps(
                {"error": f"context exceeds maximum length of {MAX_CONTEXT_LENGTH}"},
                ensure_ascii=False,
            )

        try:
            result = _run_async(
                preference_engine.recommend(bank_id, category, context),
            )
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to get recommendations", exc_info=True)
            return json.dumps(
                {"error": "Failed to generate recommendations. Please try again."},
                ensure_ascii=False,
            )

    @tool
    def record_recommendation_feedback(
        recommendation_id: str, accepted: bool, accepted_item: str = "",
    ) -> str:
        """推薦結果へのフィードバックを記録する。ユーザーが推薦を受け入れたか断ったかを記録する。

        Args:
            recommendation_id: recommend ツールが返した recommendation_id
            accepted: ユーザーが受け入れた場合 True、断った場合 False
            accepted_item: 受け入れた場合、具体的に選んだアイテム名（省略可）
        """
        if not recommendation_id or not recommendation_id.strip():
            return json.dumps(
                {"error": "recommendation_id is required"}, ensure_ascii=False,
            )
        try:
            str(uuid.UUID(recommendation_id))
        except (ValueError, TypeError):
            return json.dumps(
                {"error": "Invalid recommendation_id format"}, ensure_ascii=False,
            )
        if accepted_item and len(accepted_item) > MAX_QUERY_LENGTH:
            return json.dumps(
                {"error": f"accepted_item exceeds maximum length of {MAX_QUERY_LENGTH}"},
                ensure_ascii=False,
            )

        try:
            result = _run_async(
                preference_engine.record_recommendation_feedback(
                    bank_id,
                    recommendation_id,
                    accepted,
                    accepted_item if accepted_item else None,
                ),
            )
            return json.dumps(result, ensure_ascii=False)
        except Exception:
            logger.error("Failed to record feedback", exc_info=True)
            return json.dumps(
                {"error": "Failed to record feedback."},
                ensure_ascii=False,
            )

    @tool
    def web_search(query: str) -> str:
        """インターネットで最新情報を検索する。イベント、ニュース、お店の情報などを調べたいときに使う。

        Args:
            query: 検索クエリ（日本語）
        """
        if not query or not query.strip():
            return json.dumps(
                {"error": "query is required"}, ensure_ascii=False,
            )
        if len(query) > MAX_QUERY_LENGTH:
            return json.dumps(
                {"error": f"query exceeds maximum length of {MAX_QUERY_LENGTH}"},
                ensure_ascii=False,
            )

        try:
            result = _run_async(
                preference_engine.search(bank_id, query),
            )
            return json.dumps(result, ensure_ascii=False)
        except RuntimeError as e:
            if "TAVILY_API_KEY" in str(e):
                logger.error("Tavily API key not configured")
                return json.dumps(
                    {"error": "Web search is not configured."},
                    ensure_ascii=False,
                )
            raise
        except Exception:
            logger.error("Failed to search web", exc_info=True)
            return json.dumps(
                {"error": "Failed to search. Please try again."},
                ensure_ascii=False,
            )

    return [remember, recall_memories, reflect_on, get_user_profile, recommend, record_recommendation_feedback, web_search]


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
