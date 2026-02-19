"""Reflect パイプライン

Bedrock Converse API の tool_use を利用した独自エージェントループ。
3 階層の記憶（Mental Models → Observations → Raw Facts）を
階層的に検索し、証拠に基づいた深い推論を行う。

アーキテクチャ:
  reflect() → system_prompt 構築 → agent_loop（最大10回）
    → Bedrock Converse（tool_config 付き）
    → tool_call 検出 → 実行 → メッセージ履歴追加
    → done() で終了 or 最大イテレーション到達
  → ID 検証 → 結果返却
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import asyncpg

from memory.bedrock_client import get_bedrock_runtime_client
from memory.directive import build_directives_reminder, build_directives_section, load_directives
from memory.disposition import build_disposition_prompt, load_disposition
from memory.embedding import generate_embedding
from memory.mental_model import search_mental_models as _search_mental_models
from memory.visibility import build_tags_where_clause

logger = logging.getLogger(__name__)

_DEFAULT_REFLECT_MODEL_ID = "anthropic.claude-sonnet-4-20250514-v1:0"


def _get_reflect_model_id() -> str:
    return os.environ.get("REFLECT_MODEL_ID", _DEFAULT_REFLECT_MODEL_ID)

MAX_ITERATIONS = 10
MAX_TOKENS_RESPONSE = 4096

# Observation 検索のデフォルト
OBSERVATION_SEARCH_LIMIT = 20
OBSERVATION_SIMILARITY_THRESHOLD = 0.1

# Raw Fact 検索のデフォルト
RECALL_SEARCH_LIMIT = 30
RECALL_SIMILARITY_THRESHOLD = 0.1


# ==========================================================================
# 公開 API
# ==========================================================================


async def reflect(
    pool: asyncpg.Pool,
    bank_id: str,
    query: str,
    *,
    tags: list[str] | None = None,
    tags_match: str = "any",
    exclude_mental_model_ids: list[str] | None = None,
    max_iterations: int = MAX_ITERATIONS,
) -> dict:
    """Reflect パイプラインを実行する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        query: 推論トピック
        tags: タグフィルタ
        tags_match: マッチモード
        exclude_mental_model_ids: 除外する Mental Model ID（自己参照防止）
        max_iterations: 最大イテレーション数

    Returns:
        {
            "answer": str,
            "memory_ids": list[str],
            "mental_model_ids": list[str],
            "observation_ids": list[str],
            "iterations": int,
            "tool_calls": list[dict],
            "elapsed_ms": float,
        }
    """
    started_at = time.monotonic()

    # Disposition と Directives を並列で読み込み
    disposition, directives = await asyncio.gather(
        load_disposition(pool, bank_id),
        load_directives(pool, bank_id),
    )

    # システムプロンプト構築
    system_prompt = _build_system_prompt(disposition, directives)

    # ツール定義
    tool_config = _build_tool_config(bool(directives))

    # ツール実行コンテキスト
    ctx = _ReflectContext(
        pool=pool,
        bank_id=bank_id,
        tags=tags,
        tags_match=tags_match,
        exclude_mental_model_ids=exclude_mental_model_ids or [],
        directives=directives,
    )

    # エージェントループ
    result = await _agent_loop(
        ctx=ctx,
        query=query,
        system_prompt=system_prompt,
        tool_config=tool_config,
        max_iterations=max_iterations,
    )

    elapsed_ms = (time.monotonic() - started_at) * 1000
    result["elapsed_ms"] = round(elapsed_ms)

    logger.info(
        "Reflect complete: iterations=%d, answer_len=%d, cited_ids=%d (%.0fms)",
        result["iterations"],
        len(result.get("answer", "")),
        len(result.get("memory_ids", []))
        + len(result.get("mental_model_ids", []))
        + len(result.get("observation_ids", [])),
        elapsed_ms,
    )

    return result


# ==========================================================================
# エージェントループ
# ==========================================================================


class _ReflectContext:
    """エージェントループの状態を保持する"""

    def __init__(
        self,
        pool: asyncpg.Pool,
        bank_id: str,
        tags: list[str] | None,
        tags_match: str,
        exclude_mental_model_ids: list[str],
        directives: list[str],
    ):
        self.pool = pool
        self.bank_id = bank_id
        self.tags = tags
        self.tags_match = tags_match
        self.exclude_mental_model_ids = exclude_mental_model_ids
        self.directives = directives

        # 取得済み ID の追跡（ハルシネーション防止）
        self.available_memory_ids: set[str] = set()
        self.available_mental_model_ids: set[str] = set()
        self.available_observation_ids: set[str] = set()

        # ツール呼び出しログ
        self.tool_calls: list[dict] = []


async def _agent_loop(
    ctx: _ReflectContext,
    query: str,
    system_prompt: str,
    tool_config: dict,
    max_iterations: int,
) -> dict:
    """Bedrock Converse のツール使用ループ"""

    messages = [
        {
            "role": "user",
            "content": [{"text": query}],
        }
    ]

    for iteration in range(max_iterations):
        # LLM 呼び出し
        response = await _call_converse(system_prompt, messages, tool_config)
        stop_reason = response.get("stopReason", "")
        output_message = response["output"]["message"]

        # アシスタントメッセージを履歴に追加
        messages.append(output_message)

        # ツール使用なし → テキスト回答として処理
        if stop_reason != "tool_use":
            answer = _extract_text_from_message(output_message)
            return {
                "answer": answer,
                "memory_ids": [],
                "mental_model_ids": [],
                "observation_ids": [],
                "iterations": iteration + 1,
                "tool_calls": ctx.tool_calls,
            }

        # ツール呼び出しを実行
        tool_results = await _execute_tool_calls(ctx, output_message, iteration)

        # done ツールの結果をチェック
        for tr in tool_results:
            if tr.get("_is_done"):
                return {
                    "answer": tr["answer"],
                    "memory_ids": tr["memory_ids"],
                    "mental_model_ids": tr["mental_model_ids"],
                    "observation_ids": tr["observation_ids"],
                    "iterations": iteration + 1,
                    "tool_calls": ctx.tool_calls,
                }

        # toolResult メッセージを履歴に追加
        tool_result_contents = [
            {
                "toolResult": {
                    "toolUseId": tr["toolUseId"],
                    "content": [{"text": tr["result_text"]}],
                }
            }
            for tr in tool_results
        ]
        messages.append({"role": "user", "content": tool_result_contents})

    # 最大イテレーション到達 → 最後のテキストを返す
    logger.warning(
        "Reflect reached max iterations (%d) for bank %s",
        max_iterations, ctx.bank_id,
    )
    last_text = _extract_text_from_message(messages[-1]) if messages else ""
    return {
        "answer": last_text or "最大イテレーション数に到達しました。十分な証拠が収集できませんでした。",
        "memory_ids": list(ctx.available_memory_ids),
        "mental_model_ids": list(ctx.available_mental_model_ids),
        "observation_ids": list(ctx.available_observation_ids),
        "iterations": max_iterations,
        "tool_calls": ctx.tool_calls,
    }


# ==========================================================================
# ツール実行
# ==========================================================================


async def _execute_tool_calls(
    ctx: _ReflectContext,
    message: dict,
    iteration: int,
) -> list[dict]:
    """メッセージ内の全 toolUse を実行する"""
    results = []

    for content_block in message.get("content", []):
        if "toolUse" not in content_block:
            continue

        tool_use = content_block["toolUse"]
        tool_name = tool_use["name"]
        tool_input = tool_use.get("input", {})
        tool_use_id = tool_use["toolUseId"]

        started = time.monotonic()
        try:
            result = await _dispatch_tool(ctx, tool_name, tool_input, iteration)
        except Exception:
            logger.exception("Tool %s failed", tool_name)
            result = {"error": f"ツール {tool_name} の実行に失敗しました"}

        elapsed = (time.monotonic() - started) * 1000

        ctx.tool_calls.append({
            "tool": tool_name,
            "input": tool_input,
            "iteration": iteration,
            "elapsed_ms": round(elapsed),
        })

        result_text = json.dumps(result, ensure_ascii=False)

        entry = {
            "toolUseId": tool_use_id,
            "result_text": result_text,
        }

        # done ツールの特別処理
        if tool_name == "done" and "_is_done" in result:
            entry.update(result)

        results.append(entry)

    return results


async def _dispatch_tool(
    ctx: _ReflectContext,
    tool_name: str,
    tool_input: dict,
    iteration: int,
) -> dict:
    """ツール名に基づいてディスパッチする"""
    if tool_name == "search_mental_models":
        return await _tool_search_mental_models(ctx, tool_input)
    elif tool_name == "search_observations":
        return await _tool_search_observations(ctx, tool_input)
    elif tool_name == "recall":
        return await _tool_recall(ctx, tool_input)
    elif tool_name == "expand":
        return await _tool_expand(ctx, tool_input)
    elif tool_name == "done":
        return _tool_done(ctx, tool_input, iteration)
    else:
        return {"error": f"未知のツール: {tool_name}"}


# ---------- search_mental_models ----------


async def _tool_search_mental_models(ctx: _ReflectContext, input: dict) -> dict:
    """Mental Model をセマンティック検索する"""
    query = input.get("query", "")
    max_results = min(input.get("max_results", 5), 20)

    if not query:
        return {"error": "query は必須です", "results": []}

    results = await _search_mental_models(
        ctx.pool,
        ctx.bank_id,
        query,
        tags=ctx.tags,
        tags_match=ctx.tags_match,
        max_results=max_results,
        exclude_ids=ctx.exclude_mental_model_ids,
    )

    for r in results:
        ctx.available_mental_model_ids.add(r["id"])

    return {
        "results": [
            {
                "id": r["id"],
                "name": r["name"],
                "content": r.get("content") or r.get("description", ""),
                "tags": r.get("tags", []),
                "is_stale": r.get("is_stale", False),
            }
            for r in results
        ],
        "total": len(results),
    }


# ---------- search_observations ----------


async def _tool_search_observations(ctx: _ReflectContext, input: dict) -> dict:
    """Observation をセマンティック検索する"""
    query = input.get("query", "")
    max_results = min(input.get("max_results", OBSERVATION_SEARCH_LIMIT), 50)

    if not query:
        return {"error": "query は必須です", "results": []}

    query_embedding = await generate_embedding(query)

    base_query = """
        SELECT id, text, proof_count, source_memory_ids,
               freshness_status,
               1 - (embedding <=> $1::vector) AS similarity
        FROM memory_units
        WHERE bank_id = $2::uuid
          AND fact_type = 'observation'
          AND embedding IS NOT NULL
          AND (1 - (embedding <=> $1::vector)) >= $3
    """
    params: list = [query_embedding, ctx.bank_id, OBSERVATION_SIMILARITY_THRESHOLD]
    next_param = 4

    if ctx.tags:
        tag_clause, tag_params, next_param = build_tags_where_clause(
            ctx.tags, next_param, ctx.tags_match
        )
        base_query += f" AND {tag_clause}"
        params.extend(tag_params)

    base_query += f" ORDER BY embedding <=> $1::vector LIMIT ${next_param}"
    params.append(max_results)

    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(base_query, *params)

    results = []
    for row in rows:
        obs_id = str(row["id"])
        ctx.available_observation_ids.add(obs_id)

        source_ids = row["source_memory_ids"] or []
        results.append({
            "id": obs_id,
            "text": row["text"],
            "proof_count": row["proof_count"] or 0,
            "source_memory_ids": [str(s) for s in source_ids[:5]],
            "freshness_status": row["freshness_status"] or "unknown",
            "similarity": float(row["similarity"]),
        })

    return {"results": results, "total": len(results)}


# ---------- recall ----------


async def _tool_recall(ctx: _ReflectContext, input: dict) -> dict:
    """Raw Facts をセマンティック検索する（observation を除外）"""
    query = input.get("query", "")
    max_results = min(input.get("max_results", RECALL_SEARCH_LIMIT), 100)

    if not query:
        return {"error": "query は必須です", "results": []}

    query_embedding = await generate_embedding(query)

    base_query = """
        SELECT id, text, context, fact_type, fact_kind,
               event_date, created_at,
               1 - (embedding <=> $1::vector) AS similarity
        FROM memory_units
        WHERE bank_id = $2::uuid
          AND fact_type IN ('world', 'experience')
          AND embedding IS NOT NULL
          AND (1 - (embedding <=> $1::vector)) >= $3
    """
    params: list = [query_embedding, ctx.bank_id, RECALL_SIMILARITY_THRESHOLD]
    next_param = 4

    if ctx.tags:
        tag_clause, tag_params, next_param = build_tags_where_clause(
            ctx.tags, next_param, ctx.tags_match
        )
        base_query += f" AND {tag_clause}"
        params.extend(tag_params)

    base_query += f" ORDER BY embedding <=> $1::vector LIMIT ${next_param}"
    params.append(max_results)

    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(base_query, *params)

    results = []
    for row in rows:
        mem_id = str(row["id"])
        ctx.available_memory_ids.add(mem_id)
        results.append({
            "id": mem_id,
            "text": row["text"],
            "fact_type": row["fact_type"],
            "fact_kind": row["fact_kind"],
            "event_date": row["event_date"].isoformat() if row["event_date"] else None,
            "similarity": float(row["similarity"]),
        })

    return {"results": results, "total": len(results)}


# ---------- expand ----------


async def _tool_expand(ctx: _ReflectContext, input: dict) -> dict:
    """memory_unit の完全テキスト + コンテキストを取得する"""
    memory_ids = input.get("memory_ids", [])
    if not memory_ids:
        return {"error": "memory_ids は必須です", "results": []}

    # UUID バリデーション
    valid_ids = []
    for mid in memory_ids[:10]:  # 最大 10 件
        try:
            uuid.UUID(mid)
            valid_ids.append(mid)
        except (ValueError, TypeError):
            continue

    if not valid_ids:
        return {"error": "有効な memory_ids がありません", "results": []}

    async with ctx.pool.acquire() as conn:
        # memory_units を取得
        rows = await conn.fetch(
            """
            SELECT id, text, context, fact_type, fact_kind,
                   event_date, who, what, when_description,
                   where_description, why_description
            FROM memory_units
            WHERE id = ANY($1::uuid[])
              AND bank_id = $2::uuid
            """,
            valid_ids,
            ctx.bank_id,
        )

        # 関連 chunks を取得
        chunks_rows = await conn.fetch(
            """
            SELECT memory_unit_id, chunk_index, text
            FROM chunks
            WHERE memory_unit_id = ANY($1::uuid[])
              AND bank_id = $2::uuid
            ORDER BY memory_unit_id, chunk_index
            LIMIT 100
            """,
            valid_ids,
            ctx.bank_id,
        )

    # chunks をメモリ ID でグループ化
    chunks_by_unit: dict[str, list[dict]] = {}
    for cr in chunks_rows:
        uid = str(cr["memory_unit_id"])
        chunks_by_unit.setdefault(uid, []).append({
            "index": cr["chunk_index"],
            "text": cr["text"],
        })

    results = []
    for row in rows:
        mid = str(row["id"])
        ctx.available_memory_ids.add(mid)

        result = {
            "id": mid,
            "text": row["text"],
            "context": row["context"],
            "fact_type": row["fact_type"],
            "who": list(row["who"]) if row["who"] else [],
            "what": row["what"],
            "when": row["when_description"],
            "where": row["where_description"],
            "why": row["why_description"],
        }

        unit_chunks = chunks_by_unit.get(mid, [])
        if unit_chunks:
            result["chunks"] = unit_chunks

        results.append(result)

    return {"results": results, "total": len(results)}


# ---------- done ----------


def _tool_done(
    ctx: _ReflectContext,
    input: dict,
    iteration: int,
) -> dict:
    """回答を確定する（証拠ガードレール + ID 検証）"""
    answer = input.get("answer", "")
    cited_memory_ids = input.get("memory_ids", [])
    cited_mental_model_ids = input.get("mental_model_ids", [])
    cited_observation_ids = input.get("observation_ids", [])

    # 証拠ガードレール: 全 ID が空かつ最大イテレーション未達なら拒否
    all_cited_empty = (
        not cited_memory_ids
        and not cited_mental_model_ids
        and not cited_observation_ids
    )
    has_available_evidence = (
        ctx.available_memory_ids
        or ctx.available_mental_model_ids
        or ctx.available_observation_ids
    )

    if all_cited_empty and iteration < MAX_ITERATIONS - 1:
        if not has_available_evidence:
            return {
                "error": "証拠が収集されていません。search_mental_models、"
                         "search_observations、recall ツールを使用して"
                         "証拠を収集してから回答してください。"
            }

    # ID 検証: 実際に取得した ID のみ許可
    validated_memory_ids = [
        mid for mid in cited_memory_ids
        if mid in ctx.available_memory_ids
    ]
    validated_mental_model_ids = [
        mid for mid in cited_mental_model_ids
        if mid in ctx.available_mental_model_ids
    ]
    validated_observation_ids = [
        mid for mid in cited_observation_ids
        if mid in ctx.available_observation_ids
    ]

    # 除去された ID をログ
    removed = (
        len(cited_memory_ids) - len(validated_memory_ids)
        + len(cited_mental_model_ids) - len(validated_mental_model_ids)
        + len(cited_observation_ids) - len(validated_observation_ids)
    )
    if removed > 0:
        logger.warning("Removed %d hallucinated IDs from reflect answer", removed)

    return {
        "_is_done": True,
        "answer": answer,
        "memory_ids": validated_memory_ids,
        "mental_model_ids": validated_mental_model_ids,
        "observation_ids": validated_observation_ids,
    }


# ==========================================================================
# Bedrock Converse API
# ==========================================================================


async def _call_converse(
    system_prompt: str,
    messages: list[dict],
    tool_config: dict,
) -> dict:
    """Bedrock Converse API を呼び出す"""

    def _sync_call():
        client = get_bedrock_runtime_client()
        return client.converse(
            modelId=_get_reflect_model_id(),
            messages=messages,
            system=[{"text": system_prompt}],
            toolConfig=tool_config,
            inferenceConfig={
                "maxTokens": MAX_TOKENS_RESPONSE,
                "temperature": 0.0,
            },
        )

    return await asyncio.to_thread(_sync_call)


# ==========================================================================
# プロンプト構築
# ==========================================================================


def _build_system_prompt(disposition, directives: list[str]) -> str:
    """Reflect 用システムプロンプトを構築する"""
    parts = []

    # ディレクティブ（冒頭 — 必須ルール）
    directives_section = build_directives_section(directives)
    if directives_section:
        parts.append(directives_section)

    # Disposition ガイダンス
    disposition_prompt = build_disposition_prompt(disposition)
    if disposition_prompt:
        parts.append(disposition_prompt)

    # 本体
    parts.append(_REFLECT_SYSTEM_PROMPT)

    # ディレクティブリマインダー（末尾 — リーセンシー効果）
    directives_reminder = build_directives_reminder(directives)
    if directives_reminder:
        parts.append(directives_reminder)

    return "\n".join(parts)


_REFLECT_SYSTEM_PROMPT = """\
あなたは記憶に基づいて深く推論するエージェントです。
提供されたツールを使用して、3階層の記憶から証拠を収集し、根拠のある回答を生成してください。

## 検索階層（優先度順）

1. **search_mental_models** — キュレーション済みサマリ。最高品質の知識源。まずこれを検索してください。
2. **search_observations** — 自動統合された知識。事実から抽出されたパターンや永続的知識。
3. **recall** — 生の事実（グラウンドトゥルース）。元の記憶テキスト。検証や詳細確認に使用。
4. **expand** — 特定の記憶の完全なコンテキストを取得。5W1H情報を含む。

## 推論ルール

- **ツール結果からの情報のみ使用すること**。自分の知識で補完しないでください。
- 回答には必ず根拠となる証拠を付与してください（done ツールの ID フィールド）。
- 複雑なクエリは分解して複数回検索してください。
- 十分な証拠を収集してから done ツールを呼び出してください。
- done ツールの answer フィールドに回答を記述してください。ID はテキストに含めないでください。
- 回答は日本語で記述してください。"""


# ==========================================================================
# ツール定義（Bedrock Converse tool_config 形式）
# ==========================================================================


def _build_tool_config(has_directives: bool) -> dict:
    """ツール定義を構築する"""
    tools = [
        {
            "toolSpec": {
                "name": "search_mental_models",
                "description": "キュレーション済みサマリ（Mental Model）を検索する。"
                               "最高品質の知識源。まずこれを使用してください。",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "検索クエリ（日本語）",
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "最大結果数（デフォルト: 5）",
                            },
                            "reason": {
                                "type": "string",
                                "description": "このツールを呼び出す理由",
                            },
                        },
                        "required": ["query", "reason"],
                    }
                },
            }
        },
        {
            "toolSpec": {
                "name": "search_observations",
                "description": "自動統合された知識（Observation）を検索する。"
                               "事実から抽出されたパターンや永続的知識。",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "検索クエリ（日本語）",
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "最大結果数（デフォルト: 20）",
                            },
                            "reason": {
                                "type": "string",
                                "description": "このツールを呼び出す理由",
                            },
                        },
                        "required": ["query", "reason"],
                    }
                },
            }
        },
        {
            "toolSpec": {
                "name": "recall",
                "description": "生の事実（Raw Fact）を検索する。"
                               "元の記憶テキスト。検証や詳細確認に使用。",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "検索クエリ（日本語）",
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "最大結果数（デフォルト: 30）",
                            },
                            "reason": {
                                "type": "string",
                                "description": "このツールを呼び出す理由",
                            },
                        },
                        "required": ["query", "reason"],
                    }
                },
            }
        },
        {
            "toolSpec": {
                "name": "expand",
                "description": "特定の記憶の完全なコンテキストを取得する。"
                               "5W1H情報やチャンクテキストを含む。",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "memory_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "取得する memory_unit の ID リスト（最大10件）",
                            },
                            "reason": {
                                "type": "string",
                                "description": "このツールを呼び出す理由",
                            },
                        },
                        "required": ["memory_ids", "reason"],
                    }
                },
            }
        },
        _build_done_tool(has_directives),
    ]

    return {"tools": tools}


def _build_done_tool(has_directives: bool) -> dict:
    """done ツール定義を構築する"""
    properties = {
        "answer": {
            "type": "string",
            "description": "最終回答（日本語、マークダウン形式可）。"
                           "ID はこのフィールドに含めないでください。",
        },
        "memory_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "回答の根拠となる Raw Fact の ID リスト",
        },
        "mental_model_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "回答の根拠となる Mental Model の ID リスト",
        },
        "observation_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "回答の根拠となる Observation の ID リスト",
        },
    }
    required = ["answer"]

    if has_directives:
        properties["directive_compliance"] = {
            "type": "array",
            "items": {"type": "string"},
            "description": "各ディレクティブにどう準拠したかの説明リスト",
        }
        required.append("directive_compliance")

    return {
        "toolSpec": {
            "name": "done",
            "description": "推論が完了し、回答を返す。十分な証拠を収集してから呼び出すこと。",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                }
            },
        }
    }


# ==========================================================================
# ユーティリティ
# ==========================================================================


def _extract_text_from_message(message: dict) -> str:
    """メッセージからテキストを抽出する"""
    parts = []
    for block in message.get("content", []):
        if "text" in block:
            parts.append(block["text"])
    return "\n".join(parts)
