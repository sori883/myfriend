"""Consolidation Worker コアロジック

未統合の Raw Facts を自動的に Observations に統合する。
各 Fact に対して関連 Observation を検索し、LLM 判定で
create / update / skip を決定する。

処理フロー:
1. 未統合 Fact をバッチ取得（LIMIT 10）
2. 各 Fact の関連 Observation をセマンティック検索
3. LLM 判定（Bedrock Converse, Claude Haiku）
4. create → Observation 新規作成 / update → 既存 Observation 更新
5. consolidated_at を個別 UPDATE（クラッシュリカバリ対応）
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import asyncpg

from memory.bedrock_client import get_bedrock_runtime_client
from memory.embedding import generate_embedding
from memory.extraction import extract_json_array
from memory.freshness import update_freshness_for_bank

logger = logging.getLogger(__name__)

_DEFAULT_CONSOLIDATION_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"


def _get_consolidation_model_id() -> str:
    return os.environ.get("CONSOLIDATION_MODEL_ID", _DEFAULT_CONSOLIDATION_MODEL_ID)

CONSOLIDATION_BATCH_SIZE = 10

OBSERVATION_SIMILARITY_THRESHOLD = 0.3

MAX_RELATED_OBSERVATIONS = 10

MAX_SOURCE_MEMORIES_PER_OBS = 5

# ---------- LLM プロンプト ----------

SYSTEM_PROMPT = """\
あなたは記憶統合システムです。事実から永続的な知識（Observation）を抽出し、\
既存の知識と適切にマージする役割を担います。

有効な JSON のみを出力してください。マークダウンのコードブロックや追加テキストは不要です。

## 永続的知識の抽出（一時的状態ではなく）

事実はイベントやアクションを記述することが多いです。\
一時的な状態ではなく、事実が示す永続的な知識を抽出してください。

永続的知識の抽出例:
- 「ユーザーが203号室に移動した」→「203号室が存在する」（現在位置ではなく）
- 「Acme社を105号室で訪問した」→「Acme社は105号室にある」
- 「サラとロビーで会った」→「サラはロビーにいることがある」

ユーザーの現在位置・状態を知識として追跡しないでください（常に変化するため）。
ユーザーの行動から学んだ永続的な事実を追跡してください。

## 具体的な詳細を保持

名前、場所、数値、その他の具体的情報を保持してください。以下はしないでください:
- 一般的な原則に抽象化する
- ビジネスインサイトを生成する
- 知識を汎用的にする

良い例:
- 事実: 「太郎はピザが好き」→「太郎はピザが好き」
- 事実: 「花子はGoogleで働いている」→「花子はGoogleで働いている」

悪い例:
- 「太郎はピザが好き」→「食の好みを理解することは...」（抽象化しすぎ）
- 「ユーザーは203号室にいる」→「ユーザーは現在203号室にいる」（一時的状態）

## マージルール（既存 Observation との比較時）:
1. REDUNDANT: 同じ情報の言い換え → 既存を更新（proof_count 増加）
2. CONTRADICTION: 同じトピックについて矛盾する情報 → 時間マーカー付きで更新
   例: 「太郎は以前ピザが好きだったが、今は嫌い」
3. UPDATE: 古い状態を新しい状態に更新 → 「以前はX、現在はY」で変遷を示す

## 重要な制約:
- 異なる人物の事実を絶対にマージしない
- 無関係なトピック（食の好み vs 仕事 vs 趣味）をマージしない
- 矛盾をマージする場合、text には必ず両方の状態を時間マーカーで記録する:
  * 「以前はX、現在はY」「XからYに変わった」
  * 新しい事実だけを述べない — 必ず変化を示す
- 1つの Observation は1人の特定トピックに焦点を当てる
- text には永続的知識を記述し、一時的状態は含めない"""

USER_PROMPT_TEMPLATE = """\
この新しい事実を分析し、知識に統合してください。
{mission_section}
新しい事実: {fact_text}

既存の Observations（JSON 配列、根拠となる元の記憶付き）:
{observations_text}

各 Observation の構造:
- id: 更新用の一意識別子
- text: Observation の内容
- proof_count: 根拠となる記憶の数
- source_memories: 根拠となる元の記憶（テキストと日付）

手順:
1. 新しい事実から永続的知識を抽出する（一時的状態ではなく）
2. 既存 Observations の source_memories を確認して根拠を理解する
3. 日付を確認して矛盾や更新を検出する
4. Observations と比較する:
   - 同じトピック → learning_id 付きで UPDATE
   - 新しいトピック → 新規 CREATE
   - 純粋に一時的 → 空配列 [] を返す

出力 JSON 配列:
[
  {{"action": "update", "learning_id": "既存ObservationのUUID", "text": "更新された知識", "reason": "更新理由"}},
  {{"action": "create", "text": "新しい永続的知識", "reason": "作成理由"}}
]

永続的知識がない場合は [] を返してください。"""


# ---------- メイン処理 ----------


async def consolidate(pool: asyncpg.Pool, bank_id: str) -> dict:
    """Consolidation を実行する

    未統合 Fact をバッチ取得し、各 Fact を処理して
    Observation を作成・更新する。

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID

    Returns:
        実行結果の統計
    """
    started_at = time.monotonic()

    # bank の mission を取得
    async with pool.acquire() as conn:
        bank_row = await conn.fetchrow(
            "SELECT mission FROM banks WHERE id = $1::uuid",
            bank_id,
        )

    mission = (bank_row["mission"] if bank_row else None) or ""

    stats = {
        "processed": 0,
        "observations_created": 0,
        "observations_updated": 0,
        "skipped": 0,
        "affected_observation_ids": [],
    }

    while True:
        async with pool.acquire() as conn:
            facts = await _fetch_unconsolidated_facts(conn, bank_id)

        if not facts:
            break

        for fact in facts:
            try:
                result = await _process_fact(pool, bank_id, fact, mission)
            except Exception:
                logger.exception("Failed to process fact %s", fact["id"])
                continue  # consolidated_at を更新しない → 次回リトライ

            stats["processed"] += 1
            action = result.get("action")
            if action == "created":
                stats["observations_created"] += 1
                if result.get("observation_id"):
                    stats["affected_observation_ids"].append(result["observation_id"])
            elif action == "updated":
                stats["observations_updated"] += 1
                if result.get("observation_id"):
                    stats["affected_observation_ids"].append(result["observation_id"])
            elif action == "multiple":
                stats["observations_created"] += result.get("created", 0)
                stats["observations_updated"] += result.get("updated", 0)
                stats["affected_observation_ids"].extend(result.get("observation_ids", []))
            elif action == "skipped":
                stats["skipped"] += 1

            # 個別に consolidated_at を更新（クラッシュリカバリ）
            # スキップの場合も更新する（LLM が永続知識なしと判断 → 再処理不要）
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE memory_units
                    SET consolidated_at = NOW()
                    WHERE id = $1::uuid
                    """,
                    fact["id"],
                )

    # Consolidation で処理があった場合、鮮度ステータスを更新
    if stats["processed"] > 0:
        try:
            freshness_result = await update_freshness_for_bank(pool, bank_id)
            stats["freshness_updated"] = freshness_result.get("updated", 0)
        except Exception:
            logger.exception("Failed to update freshness for bank %s", bank_id)
            stats["freshness_updated"] = 0

        # Mental Model 自動リフレッシュ・自動生成
        from memory.mental_model_trigger import (
            trigger_mental_model_generation,
            trigger_mental_model_refresh,
        )

        try:
            refreshed = await trigger_mental_model_refresh(pool, bank_id)
            stats["mental_models_refreshed"] = refreshed
        except Exception:
            logger.exception("Failed to refresh mental models for bank %s", bank_id)
            stats["mental_models_refreshed"] = 0

        try:
            generated = await trigger_mental_model_generation(
                pool, bank_id, stats["affected_observation_ids"], mission,
            )
            stats["mental_models_generated"] = generated
        except Exception:
            logger.exception("Failed to generate mental models for bank %s", bank_id)
            stats["mental_models_generated"] = 0

    elapsed_ms = (time.monotonic() - started_at) * 1000

    logger.info(
        "Consolidation complete for bank %s: "
        "processed=%d, created=%d, updated=%d, skipped=%d, "
        "mm_refreshed=%d, mm_generated=%d (%.0fms)",
        bank_id,
        stats["processed"],
        stats["observations_created"],
        stats["observations_updated"],
        stats["skipped"],
        stats.get("mental_models_refreshed", 0),
        stats.get("mental_models_generated", 0),
        elapsed_ms,
    )

    return {**stats, "elapsed_ms": round(elapsed_ms)}


# ---------- 内部関数 ----------


async def _fetch_unconsolidated_facts(
    conn: asyncpg.Connection,
    bank_id: str,
    limit: int = CONSOLIDATION_BATCH_SIZE,
) -> list[asyncpg.Record]:
    """未統合 Fact をバッチ取得する

    パーシャルインデックス idx_memory_units_unconsolidated を活用。
    """
    return await conn.fetch(
        """
        SELECT id, text, fact_type, event_date,
               occurred_start, occurred_end, mentioned_at
        FROM memory_units
        WHERE bank_id = $1::uuid
          AND consolidated_at IS NULL
          AND fact_type IN ('world', 'experience')
        ORDER BY created_at ASC
        LIMIT $2
        """,
        bank_id,
        limit,
    )


async def _find_related_observations(
    pool: asyncpg.Pool,
    bank_id: str,
    fact_text: str,
) -> list[dict]:
    """Fact に関連する既存 Observation をセマンティック検索で取得する

    Returns:
        Observation の dict リスト。各 dict は id, text, proof_count,
        history, source_memories を含む。
    """
    fact_embedding = await generate_embedding(fact_text)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, text, proof_count, history, source_memory_ids,
                   1 - (embedding <=> $1::vector) AS similarity
            FROM memory_units
            WHERE bank_id = $2::uuid
              AND fact_type = 'observation'
              AND embedding IS NOT NULL
              AND (1 - (embedding <=> $1::vector)) >= $3
            ORDER BY embedding <=> $1::vector
            LIMIT $4
            """,
            fact_embedding,
            bank_id,
            OBSERVATION_SIMILARITY_THRESHOLD,
            MAX_RELATED_OBSERVATIONS,
        )

    if not rows:
        return []

    observations = []
    async with pool.acquire() as conn:
        for row in rows:
            source_memories = await _fetch_source_memories(
                conn, bank_id, row["source_memory_ids"]
            )

            history = row["history"]
            if isinstance(history, str):
                history = json.loads(history)
            elif history is None:
                history = []

            observations.append({
                "id": row["id"],
                "text": row["text"],
                "proof_count": row["proof_count"] or 1,
                "history": history,
                "source_memories": source_memories,
            })

    return observations


async def _fetch_source_memories(
    conn: asyncpg.Connection,
    bank_id: str,
    source_memory_ids: list | None,
) -> list[dict]:
    """Observation の根拠となる元の記憶を取得する（最大5件）"""
    if not source_memory_ids:
        return []

    rows = await conn.fetch(
        """
        SELECT text, event_date, occurred_start
        FROM memory_units
        WHERE id = ANY($1::uuid[])
          AND bank_id = $2::uuid
        ORDER BY created_at ASC
        LIMIT $3
        """,
        source_memory_ids[:MAX_SOURCE_MEMORIES_PER_OBS],
        bank_id,
        MAX_SOURCE_MEMORIES_PER_OBS,
    )

    return [
        {
            "text": r["text"],
            "event_date": r["event_date"].isoformat() if r["event_date"] else None,
            "occurred_start": r["occurred_start"].isoformat() if r["occurred_start"] else None,
        }
        for r in rows
    ]


async def _process_fact(
    pool: asyncpg.Pool,
    bank_id: str,
    fact: asyncpg.Record,
    mission: str,
) -> dict:
    """1件の Fact を処理する

    1. 関連 Observation 検索
    2. LLM 判定
    3. アクション実行（create / update）
    """
    fact_text = fact["text"]
    fact_id = fact["id"]

    # 1. 関連 Observation 検索
    observations = await _find_related_observations(pool, bank_id, fact_text)

    # 2. LLM 判定
    actions = await _consolidate_with_llm(fact_text, observations, mission)

    if not actions:
        return {"action": "skipped", "reason": "no_durable_knowledge"}

    # 3. アクション実行（トランザクション内で複数アクションの原子性を保証）
    results = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            for action in actions:
                action_type = action.get("action")
                if action_type == "create":
                    result = await _execute_create_action(
                        conn, bank_id, fact_id, action,
                        event_date=fact["event_date"],
                        occurred_start=fact["occurred_start"],
                        occurred_end=fact["occurred_end"],
                        mentioned_at=fact["mentioned_at"],
                    )
                    results.append(result)
                elif action_type == "update":
                    result = await _execute_update_action(
                        conn, bank_id, fact_id, action, observations,
                        source_occurred_start=fact["occurred_start"],
                        source_occurred_end=fact["occurred_end"],
                        source_mentioned_at=fact["mentioned_at"],
                    )
                    results.append(result)

    if not results:
        return {"action": "skipped", "reason": "no_valid_actions"}

    if len(results) == 1:
        return results[0]

    created = sum(1 for r in results if r.get("action") == "created")
    updated = sum(1 for r in results if r.get("action") == "updated")
    return {
        "action": "multiple",
        "created": created,
        "updated": updated,
        "total_actions": len(results),
        "observation_ids": [r["observation_id"] for r in results if "observation_id" in r],
    }


# ---------- LLM ----------


def _call_consolidation_llm(fact_text: str, observations_text: str, mission: str) -> list[dict]:
    """Bedrock Converse API を同期呼び出しして統合判定を行う"""
    client = get_bedrock_runtime_client()

    mission_section = ""
    if mission:
        mission_section = f"\nミッション: {mission}\nこのミッションに役立つ永続的知識に焦点を当ててください。\n"

    user_message = USER_PROMPT_TEMPLATE.format(
        mission_section=mission_section,
        fact_text=fact_text,
        observations_text=observations_text,
    )

    response = client.converse(
        modelId=_get_consolidation_model_id(),
        messages=[
            {
                "role": "user",
                "content": [{"text": user_message}],
            }
        ],
        system=[{"text": SYSTEM_PROMPT}],
        inferenceConfig={
            "maxTokens": 2048,
            "temperature": 0.0,
        },
    )

    output_text = response["output"]["message"]["content"][0]["text"]
    return extract_json_array(output_text)


async def _consolidate_with_llm(
    fact_text: str,
    observations: list[dict],
    mission: str,
) -> list[dict]:
    """LLM を呼び出して統合アクションを決定する

    Returns:
        アクションのリスト。各アクションは action, text, (learning_id), reason を持つ。
        永続的知識がない場合は空リスト。
    """
    if observations:
        obs_list = []
        for obs in observations:
            obs_data = {
                "id": str(obs["id"]),
                "text": obs["text"],
                "proof_count": obs["proof_count"],
            }
            if obs.get("source_memories"):
                obs_data["source_memories"] = obs["source_memories"][:3]
            obs_list.append(obs_data)
        observations_text = json.dumps(obs_list, ensure_ascii=False, indent=2)
    else:
        observations_text = "[]"

    try:
        actions = await asyncio.to_thread(
            _call_consolidation_llm, fact_text, observations_text, mission,
        )
    except Exception:
        logger.error("Failed to call LLM for consolidation", exc_info=True)
        return []

    # バリデーション
    valid_actions = []
    for action in actions:
        action_type = action.get("action")
        if action_type == "create" and action.get("text"):
            valid_actions.append(action)
        elif action_type == "update" and action.get("learning_id") and action.get("text"):
            valid_actions.append(action)
        else:
            logger.warning("Invalid consolidation action: %s", action)

    return valid_actions


# ---------- アクション実行 ----------


async def _execute_create_action(
    conn: asyncpg.Connection,
    bank_id: str,
    source_memory_id,
    action: dict,
    event_date=None,
    occurred_start=None,
    occurred_end=None,
    mentioned_at=None,
) -> dict:
    """新しい Observation を作成する"""
    text = action["text"]

    embedding = await generate_embedding(text)

    now = datetime.now(timezone.utc)

    row = await conn.fetchrow(
        """
        INSERT INTO memory_units (
            bank_id, text, embedding, fact_type,
            proof_count, source_memory_ids, history,
            event_date, occurred_start, occurred_end, mentioned_at
        ) VALUES (
            $1::uuid, $2, $3::vector, 'observation',
            1, ARRAY[$4::uuid], '[]'::jsonb,
            $5, $6, $7, $8
        )
        RETURNING id
        """,
        bank_id,
        text,
        embedding,
        source_memory_id,
        event_date,              # NULL 許容（継続的知識には日付不要）
        occurred_start or now,
        occurred_end or now,
        mentioned_at or now,
    )

    obs_id = str(row["id"])

    # 元 Fact のエンティティリンクを Observation に引き継ぐ
    await conn.execute(
        """
        INSERT INTO unit_entities (unit_id, entity_id)
        SELECT $1::uuid, entity_id
        FROM unit_entities
        WHERE unit_id = $2::uuid
        ON CONFLICT DO NOTHING
        """,
        obs_id,
        source_memory_id,
    )

    logger.debug("Created observation %s from fact %s", obs_id, source_memory_id)
    return {"action": "created", "observation_id": obs_id}


async def _execute_update_action(
    conn: asyncpg.Connection,
    bank_id: str,
    source_memory_id,
    action: dict,
    observations: list[dict],
    source_occurred_start=None,
    source_occurred_end=None,
    source_mentioned_at=None,
) -> dict:
    """既存の Observation を更新する"""
    learning_id = action["learning_id"]
    new_text = action["text"]
    reason = action.get("reason", "Updated with new fact")

    # 対象 Observation を特定
    target = next(
        (obs for obs in observations if str(obs["id"]) == learning_id),
        None,
    )
    if not target:
        logger.warning("Observation %s not found for update", learning_id)
        return {"action": "skipped", "reason": "observation_not_found"}

    # 変更履歴を追加
    history = list(target.get("history") or [])
    history.append({
        "previous_text": target["text"],
        "changed_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "source_memory_id": str(source_memory_id),
    })

    # 新しい Embedding を生成
    embedding = await generate_embedding(new_text)

    # source_memory_ids を更新（重複追加を防止）
    await conn.execute(
        """
        UPDATE memory_units
        SET text = $1,
            embedding = $2::vector,
            history = $3::jsonb,
            source_memory_ids = CASE
                WHEN $4::uuid = ANY(source_memory_ids)
                THEN source_memory_ids
                ELSE array_append(source_memory_ids, $4::uuid)
            END,
            proof_count = CASE
                WHEN $4::uuid = ANY(source_memory_ids)
                THEN COALESCE(array_length(source_memory_ids, 1), 0)
                ELSE COALESCE(array_length(source_memory_ids, 1), 0) + 1
            END,
            occurred_start = LEAST(occurred_start, COALESCE($6, occurred_start)),
            occurred_end = GREATEST(occurred_end, COALESCE($7, occurred_end)),
            mentioned_at = GREATEST(mentioned_at, COALESCE($8, mentioned_at))
        WHERE id = $5::uuid
        """,
        new_text,
        embedding,
        json.dumps(history),
        source_memory_id,
        target["id"],
        source_occurred_start,
        source_occurred_end,
        source_mentioned_at,
    )

    # 新しい source Fact のエンティティリンクを Observation に追加
    await conn.execute(
        """
        INSERT INTO unit_entities (unit_id, entity_id)
        SELECT $1::uuid, entity_id
        FROM unit_entities
        WHERE unit_id = $2::uuid
        ON CONFLICT DO NOTHING
        """,
        target["id"],
        source_memory_id,
    )

    logger.debug("Updated observation %s from fact %s", learning_id, source_memory_id)
    return {"action": "updated", "observation_id": learning_id}
