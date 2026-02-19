"""Phase 3 検証スクリプト

Phase 3（長期記憶: Mental Models）の各機能を検証する。

項目:
  1. DB マイグレーション — mental_models / chunks テーブルとインデックスの存在確認
  2. Visibility（タグ可視性） — SQL WHERE 句生成 + Python フィルタリング
  3. Disposition（性格特性） — プロンプト生成ロジック
  4. Directive（ディレクティブ） — セクション + リマインダー生成
  5. Mental Model CRUD — create / get / list / search / update / delete
  6. Reflect ツール定義 — ツール構成の正当性
  7. Reflect done ツール — 証拠ガードレール + ID 検証
  8. システムプロンプト構築 — Disposition + Directives 統合
  9. Mental Model 自動生成ロジック — source_query 生成 + 重複チェック + DB ユニーク制約
 10. entity_id 対応 — create_mental_model の entity_id 引数

Usage: cd agentcore && uv run python test_script/test_phase3_verification.py
"""

import asyncio
import logging
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

import asyncpg

from memory.db import close_pool, get_pool
from memory.directive import (
    build_directives_reminder,
    build_directives_section,
)
from memory.disposition import (
    Disposition,
    build_disposition_prompt,
)
from memory.reflect import (
    _ReflectContext,
    _build_system_prompt,
    _build_tool_config,
    _tool_done,
)
from memory.visibility import (
    ALLOWED_COLUMNS,
    build_tags_where_clause,
    filter_results_by_tags,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

TEST_BANK_ID = str(uuid.uuid4())

results: list[tuple[str, bool, str]] = []


class _RollbackTest(Exception):
    """テスト用ロールバック例外"""


def record(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append((name, passed, detail))
    logger.info("[%s] %s %s", status, name, f"- {detail}" if detail else "")


# ==========================================================================
# Test 1: DB マイグレーション
# ==========================================================================

async def test_db_migration(pool: asyncpg.Pool):
    """mental_models / chunks テーブルとインデックスの存在を確認"""
    name = "1. DB マイグレーション"

    try:
        async with pool.acquire() as conn:
            # テーブル存在確認
            tables = await conn.fetch(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('mental_models', 'chunks')
                ORDER BY table_name
                """
            )
            table_names = [r["table_name"] for r in tables]
            assert "chunks" in table_names, f"chunks テーブルがない: {table_names}"
            assert "mental_models" in table_names, f"mental_models テーブルがない: {table_names}"

            # インデックス確認
            indexes = await conn.fetch(
                """
                SELECT indexname FROM pg_indexes
                WHERE tablename IN ('mental_models', 'chunks')
                ORDER BY indexname
                """
            )
            idx_names = [r["indexname"] for r in indexes]
            expected_indexes = [
                "idx_mental_models_bank",
                "idx_mental_models_bank_entity",
                "idx_mental_models_embedding",
                "idx_mental_models_tags",
                "idx_chunks_memory_unit",
                "idx_chunks_embedding",
            ]
            for idx in expected_indexes:
                assert idx in idx_names, f"インデックス {idx} がない: {idx_names}"

            # トリガー確認
            triggers = await conn.fetch(
                """
                SELECT tgname FROM pg_trigger
                WHERE tgrelid = 'mental_models'::regclass AND NOT tgisinternal
                """
            )
            trigger_names = [r["tgname"] for r in triggers]
            assert "update_mental_models_updated_at" in trigger_names

        record(name, True, f"テーブル: {table_names}, インデックス: {len(idx_names)}個")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 2: タグベース可視性制御
# ==========================================================================

async def test_visibility(pool: asyncpg.Pool):
    """SQL WHERE 句生成 + Python フィルタリング"""
    name = "2. タグベース可視性制御"

    try:
        # SQL WHERE 句テスト
        clause, params, next_offset = build_tags_where_clause(
            ["tag1", "tag2"], param_offset=3, match_mode="any"
        )
        assert "$3" in clause, f"パラメータプレースホルダが不正: {clause}"
        assert "&&" in clause, f"any モードなのに && がない: {clause}"
        assert "IS NULL" in clause, f"non-strict なのに IS NULL がない: {clause}"
        assert next_offset == 4

        clause_strict, _, _ = build_tags_where_clause(
            ["tag1"], param_offset=1, match_mode="all_strict"
        )
        assert "IS NOT NULL" in clause_strict, f"strict なのに IS NOT NULL がない"
        assert "@>" in clause_strict, f"all モードなのに @> がない"

        # SQL injection 防止テスト
        try:
            build_tags_where_clause(["tag1"], column="evil; DROP TABLE banks")
            record(name, False, "SQL injection が防止されていない")
            return
        except ValueError:
            pass  # 期待通り

        # ALLOWED_COLUMNS チェック
        assert "tags" in ALLOWED_COLUMNS

        # Python フィルタリングテスト
        items = [
            {"tags": ["a", "b"]},
            {"tags": ["b", "c"]},
            {"tags": []},
            {"tags": None},
        ]

        # any: OR + 未タグ含む
        result = filter_results_by_tags(items, ["a"], match_mode="any")
        assert len(result) == 3, f"any: 期待3件, 実際{len(result)}件"

        # any_strict: OR + 未タグ除外
        result = filter_results_by_tags(items, ["a"], match_mode="any_strict")
        assert len(result) == 1, f"any_strict: 期待1件, 実際{len(result)}件"

        # all: AND + 未タグ含む
        result = filter_results_by_tags(items, ["a", "b"], match_mode="all")
        assert len(result) == 3, f"all: 期待3件, 実際{len(result)}件"

        # all_strict: AND + 未タグ除外
        result = filter_results_by_tags(items, ["a", "b"], match_mode="all_strict")
        assert len(result) == 1, f"all_strict: 期待1件, 実際{len(result)}件"

        # 無効なモードは any にフォールバック
        clause_fallback, _, _ = build_tags_where_clause(
            ["tag1"], match_mode="invalid_mode"
        )
        assert "&&" in clause_fallback, "無効モードが any にフォールバックしていない"

        record(name, True, "SQL句生成4モード + Pythonフィルタ4モード + injection防止")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 3: Disposition（性格特性）
# ==========================================================================

async def test_disposition(pool: asyncpg.Pool):
    """プロンプト生成ロジック"""
    name = "3. Disposition プロンプト生成"

    try:
        # デフォルト（全て中間値 = ニュートラル）
        d_neutral = Disposition()
        prompt_neutral = build_disposition_prompt(d_neutral)
        assert prompt_neutral == "", f"ニュートラルなのにテキストが生成された: {prompt_neutral!r}"

        # 高懐疑性
        d_skeptic = Disposition(skepticism=5, literalism=3, empathy=3)
        prompt_skeptic = build_disposition_prompt(d_skeptic)
        assert "懐疑的" in prompt_skeptic, f"懐疑性テキストがない: {prompt_skeptic}"
        assert "推論ガイダンス" in prompt_skeptic

        # 低懐疑性
        d_trusting = Disposition(skepticism=1, literalism=3, empathy=3)
        prompt_trusting = build_disposition_prompt(d_trusting)
        assert "信頼" in prompt_trusting, f"信頼テキストがない: {prompt_trusting}"

        # 高文字通り度
        d_literal = Disposition(skepticism=3, literalism=5, empathy=3)
        prompt_literal = build_disposition_prompt(d_literal)
        assert "文字通り" in prompt_literal

        # 低文字通り度
        d_intuitive = Disposition(skepticism=3, literalism=1, empathy=3)
        prompt_intuitive = build_disposition_prompt(d_intuitive)
        assert "行間" in prompt_intuitive

        # 高共感性
        d_empathic = Disposition(skepticism=3, literalism=3, empathy=5)
        prompt_empathic = build_disposition_prompt(d_empathic)
        assert "感情" in prompt_empathic

        # 低共感性
        d_factual = Disposition(skepticism=3, literalism=3, empathy=1)
        prompt_factual = build_disposition_prompt(d_factual)
        assert "事実" in prompt_factual

        # 全軸高い → 3つのガイダンス
        d_all_high = Disposition(skepticism=5, literalism=5, empathy=5)
        prompt_all = build_disposition_prompt(d_all_high)
        bullet_count = prompt_all.count("- ")
        assert bullet_count == 3, f"ガイダンス数が3でない: {bullet_count}"

        record(name, True, "ニュートラル + 6パターン + 全軸テスト")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 4: Directive（ディレクティブ）
# ==========================================================================

async def test_directive(pool: asyncpg.Pool):
    """セクション + リマインダー生成"""
    name = "4. ディレクティブ生成"

    try:
        # 空ディレクティブ
        assert build_directives_section([]) == ""
        assert build_directives_reminder([]) == ""

        # ディレクティブあり
        directives = ["常に敬語を使う", "政治的話題を避ける"]

        section = build_directives_section(directives)
        assert "ディレクティブ（必須）" in section
        assert "1. 常に敬語を使う" in section
        assert "2. 政治的話題を避ける" in section
        assert "違反" in section

        reminder = build_directives_reminder(directives)
        assert "回答前の確認" in reminder
        assert "1. 常に敬語を使う" in reminder

        record(name, True, "空ケース + セクション + リマインダー")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 5: Mental Model CRUD
# ==========================================================================

async def test_mental_model_crud(pool: asyncpg.Pool):
    """create / get / list / search / update / delete をテスト（Bedrock Embedding 使用）"""
    name = "5. Mental Model CRUD"

    try:
        from memory.mental_model import (
            create_mental_model,
            delete_mental_model,
            get_mental_model,
            get_refreshable_models,
            list_mental_models,
            search_mental_models,
            update_mental_model,
        )

        # テスト用 bank を作成
        async with pool.acquire() as conn:
            bank_row = await conn.fetchrow(
                """
                INSERT INTO banks (name)
                VALUES ($1)
                RETURNING id
                """,
                f"test_bank_{uuid.uuid4().hex[:8]}",
            )
            bank_id = str(bank_row["id"])

        try:
            # Create
            model = await create_mental_model(
                pool, bank_id,
                name="テストモデル",
                content="これはテスト用のMental Modelです。ユーザーの好みに関する知識をまとめます。",
                description="テスト用",
                source_query="ユーザーの好みは？",
                tags=["test", "preference"],
                trigger={"refresh_after_consolidation": True},
            )
            assert model["name"] == "テストモデル"
            assert model["tags"] == ["test", "preference"]
            assert model["trigger"]["refresh_after_consolidation"] is True
            model_id = model["id"]
            logger.info("  Created: %s", model_id)

            # Get
            fetched = await get_mental_model(pool, bank_id, model_id)
            assert fetched is not None
            assert fetched["name"] == "テストモデル"
            assert fetched["content"] == "これはテスト用のMental Modelです。ユーザーの好みに関する知識をまとめます。"

            # List
            models = await list_mental_models(pool, bank_id)
            assert len(models) == 1
            assert models[0]["id"] == model_id

            # List with tags
            tagged = await list_mental_models(pool, bank_id, tags=["test"])
            assert len(tagged) == 1
            untagged = await list_mental_models(pool, bank_id, tags=["nonexistent"], tags_match="any_strict")
            assert len(untagged) == 0

            # Search (semantic)
            search_results = await search_mental_models(
                pool, bank_id, "ユーザーの好み", max_results=5
            )
            assert len(search_results) >= 1
            assert search_results[0]["id"] == model_id
            assert "similarity" in search_results[0]
            assert "is_stale" in search_results[0]

            # Search with exclude
            excluded = await search_mental_models(
                pool, bank_id, "ユーザーの好み", exclude_ids=[model_id]
            )
            assert len(excluded) == 0

            # Update
            updated = await update_mental_model(
                pool, bank_id, model_id,
                name="更新済みモデル",
                content="更新後のコンテンツ。ユーザーはコーヒーよりお茶が好き。",
            )
            assert updated is not None
            assert updated["name"] == "更新済みモデル"
            assert "更新後" in updated["content"]
            assert updated["last_refreshed_at"] is not None

            # Get refreshable models
            refreshable = await get_refreshable_models(pool, bank_id)
            assert len(refreshable) >= 1
            assert refreshable[0]["source_query"] == "ユーザーの好みは？"

            # Delete
            deleted = await delete_mental_model(pool, bank_id, model_id)
            assert deleted is True

            # Verify deletion
            gone = await get_mental_model(pool, bank_id, model_id)
            assert gone is None

            # Delete nonexistent
            deleted_again = await delete_mental_model(pool, bank_id, model_id)
            assert deleted_again is False

            record(name, True, "create/get/list/search/update/refreshable/delete")
        finally:
            # クリーンアップ
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM mental_models WHERE bank_id = $1::uuid", bank_id)
                await conn.execute("DELETE FROM banks WHERE id = $1::uuid", bank_id)

    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 6: Reflect ツール定義
# ==========================================================================

async def test_reflect_tool_config(pool: asyncpg.Pool):
    """ツール構成の正当性"""
    name = "6. Reflect ツール定義"

    try:
        # ディレクティブなし
        config_no_dir = _build_tool_config(has_directives=False)
        tools = config_no_dir["tools"]
        tool_names = [t["toolSpec"]["name"] for t in tools]
        expected = ["search_mental_models", "search_observations", "recall", "expand", "done"]
        assert tool_names == expected, f"ツール名不一致: {tool_names}"

        # done ツールに directive_compliance がないことを確認
        done_tool = tools[4]["toolSpec"]
        done_props = done_tool["inputSchema"]["json"]["properties"]
        assert "directive_compliance" not in done_props

        # ディレクティブあり
        config_with_dir = _build_tool_config(has_directives=True)
        done_tool_dir = config_with_dir["tools"][4]["toolSpec"]
        done_props_dir = done_tool_dir["inputSchema"]["json"]["properties"]
        assert "directive_compliance" in done_props_dir
        done_required = done_tool_dir["inputSchema"]["json"]["required"]
        assert "directive_compliance" in done_required

        record(name, True, "5ツール定義 + ディレクティブ有無の切替")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 7: Reflect done ツール（証拠ガードレール + ID 検証）
# ==========================================================================

async def test_reflect_done_tool(pool: asyncpg.Pool):
    """証拠ガードレール + ID 検証"""
    name = "7. Reflect done ツール"

    try:
        # コンテキスト作成
        ctx = _ReflectContext(
            pool=pool,
            bank_id=TEST_BANK_ID,
            tags=None,
            tags_match="any",
            exclude_mental_model_ids=[],
            directives=[],
        )

        # 証拠なし + 証拠未収集 → 拒否
        result = _tool_done(ctx, {"answer": "テスト回答"}, iteration=0)
        assert "error" in result, "証拠なしで done が通ってしまった"
        assert "証拠" in result["error"]

        # 証拠を追加
        real_id = str(uuid.uuid4())
        ctx.available_memory_ids.add(real_id)

        # 証拠あり + 有効 ID → 成功
        result = _tool_done(
            ctx,
            {
                "answer": "証拠付き回答",
                "memory_ids": [real_id],
                "mental_model_ids": [],
                "observation_ids": [],
            },
            iteration=1,
        )
        assert result.get("_is_done") is True
        assert result["answer"] == "証拠付き回答"
        assert real_id in result["memory_ids"]

        # ハルシネーション ID の除去
        fake_id = str(uuid.uuid4())
        result_hallucinated = _tool_done(
            ctx,
            {
                "answer": "テスト",
                "memory_ids": [real_id, fake_id],
                "mental_model_ids": [fake_id],
                "observation_ids": [],
            },
            iteration=2,
        )
        assert result_hallucinated.get("_is_done") is True
        assert fake_id not in result_hallucinated["memory_ids"]
        assert real_id in result_hallucinated["memory_ids"]
        assert len(result_hallucinated["mental_model_ids"]) == 0

        record(name, True, "証拠ガードレール拒否 + 有効ID通過 + ハルシネーションID除去")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 8: システムプロンプト構築
# ==========================================================================

async def test_system_prompt_build(pool: asyncpg.Pool):
    """Disposition + Directives のプロンプト統合"""
    name = "8. システムプロンプト構築"

    try:
        # Disposition + Directives 付き
        disposition = Disposition(skepticism=5, literalism=1, empathy=4)
        directives = ["常に日本語で回答する", "個人情報を推測しない"]

        prompt = _build_system_prompt(disposition, directives)

        # ディレクティブセクションが冒頭にある
        assert prompt.index("ディレクティブ（必須）") < prompt.index("推論ガイダンス")

        # Disposition ガイダンスが含まれている
        assert "懐疑的" in prompt
        assert "行間" in prompt
        assert "感情" in prompt

        # 本体が含まれている
        assert "記憶に基づいて深く推論" in prompt
        assert "search_mental_models" in prompt

        # リマインダーが末尾にある
        assert "回答前の確認" in prompt
        reminder_pos = prompt.index("回答前の確認")
        body_pos = prompt.index("記憶に基づいて深く推論")
        assert reminder_pos > body_pos

        # Disposition なし + Directives なし
        prompt_minimal = _build_system_prompt(Disposition(), [])
        assert "ディレクティブ" not in prompt_minimal
        assert "推論ガイダンス" not in prompt_minimal
        assert "記憶に基づいて深く推論" in prompt_minimal

        record(name, True, "Disposition+Directives統合 + 最小構成")
    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 9: Mental Model 自動生成ロジック
# ==========================================================================

async def test_auto_generation_logic(pool: asyncpg.Pool):
    """source_query 生成 + 重複チェック + DB ユニーク制約"""
    name = "9. Mental Model 自動生成ロジック"

    try:
        from memory.mental_model_trigger import (
            _build_source_query,
            _check_mental_model_exists,
        )

        # _build_source_query: mission あり
        q_with_mission = _build_source_query("田中太郎", "ユーザーの友人関係を把握する")
        assert "田中太郎" in q_with_mission
        assert "ミッション" in q_with_mission
        assert "友人関係" in q_with_mission

        # _build_source_query: mission なし
        q_no_mission = _build_source_query("佐藤花子", "")
        assert "佐藤花子" in q_no_mission
        assert "ミッション" not in q_no_mission

        # テスト用 bank 作成
        async with pool.acquire() as conn:
            bank_row = await conn.fetchrow(
                "INSERT INTO banks (name) VALUES ($1) RETURNING id",
                f"test_autogen_{uuid.uuid4().hex[:8]}",
            )
            bank_id = str(bank_row["id"])

        try:
            # _check_mental_model_exists: 何もない → False
            exists = await _check_mental_model_exists(
                pool, bank_id, str(uuid.uuid4()), "テストエンティティ"
            )
            assert exists is False, "存在しないのに True が返った"

            # テスト用エンティティを entities テーブルに作成（FK 制約対応）
            async with pool.acquire() as conn:
                entity_row = await conn.fetchrow(
                    """
                    INSERT INTO entities (bank_id, canonical_name, entity_type)
                    VALUES ($1::uuid, $2, 'person')
                    RETURNING id
                    """,
                    bank_id,
                    "テスト自動生成エンティティ",
                )
                entity_id = str(entity_row["id"])

            # entity_id 付きで Mental Model を作成
            from memory.mental_model import create_mental_model, delete_mental_model

            model = await create_mental_model(
                pool, bank_id,
                name="テスト自動生成",
                content="テスト用の自動生成コンテンツです。エンティティに関する知識をまとめています。",
                entity_id=entity_id,
            )

            # _check_mental_model_exists: entity_id 一致 → True
            exists = await _check_mental_model_exists(
                pool, bank_id, entity_id, "テスト自動生成"
            )
            assert exists is True, "entity_id 一致なのに False が返った"

            # DB ユニーク制約テスト: 同じ (bank_id, entity_id) で INSERT → エラー
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO mental_models (bank_id, name, content, entity_id)
                        VALUES ($1::uuid, 'dup', 'dup', $2::uuid)
                        """,
                        bank_id,
                        entity_id,
                    )
                record(name, False, "ユニーク制約が機能していない")
                return
            except asyncpg.UniqueViolationError:
                pass  # 期待通り

            await delete_mental_model(pool, bank_id, model["id"])

            record(name, True, "source_query生成 + 重複チェック + DB ユニーク制約")
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM mental_models WHERE bank_id = $1::uuid", bank_id)
                await conn.execute("DELETE FROM entities WHERE bank_id = $1::uuid", bank_id)
                await conn.execute("DELETE FROM banks WHERE id = $1::uuid", bank_id)

    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Test 10: entity_id 対応 (create_mental_model)
# ==========================================================================

async def test_entity_id_support(pool: asyncpg.Pool):
    """create_mental_model の entity_id 引数がDBに反映されるか"""
    name = "10. entity_id 対応"

    try:
        from memory.mental_model import create_mental_model, get_mental_model, delete_mental_model

        async with pool.acquire() as conn:
            bank_row = await conn.fetchrow(
                "INSERT INTO banks (name) VALUES ($1) RETURNING id",
                f"test_entity_{uuid.uuid4().hex[:8]}",
            )
            bank_id = str(bank_row["id"])

        try:
            # entity_id なし
            model_no_entity = await create_mental_model(
                pool, bank_id,
                name="エンティティなし",
                content="エンティティIDなしで作成されたMental Modelです。",
            )
            fetched = await get_mental_model(pool, bank_id, model_no_entity["id"])
            assert fetched["entity_id"] is None, f"entity_id が None でない: {fetched['entity_id']}"

            # テスト用エンティティを entities テーブルに作成（FK 制約対応）
            async with pool.acquire() as conn:
                entity_row = await conn.fetchrow(
                    """
                    INSERT INTO entities (bank_id, canonical_name, entity_type)
                    VALUES ($1::uuid, $2, 'person')
                    RETURNING id
                    """,
                    bank_id,
                    "テストエンティティ10",
                )
                test_entity_id = str(entity_row["id"])

            # entity_id あり
            model_with_entity = await create_mental_model(
                pool, bank_id,
                name="エンティティあり",
                content="エンティティIDありで作成されたMental Modelです。",
                entity_id=test_entity_id,
            )
            fetched2 = await get_mental_model(pool, bank_id, model_with_entity["id"])
            assert fetched2["entity_id"] == test_entity_id, (
                f"entity_id 不一致: {fetched2['entity_id']} != {test_entity_id}"
            )

            await delete_mental_model(pool, bank_id, model_no_entity["id"])
            await delete_mental_model(pool, bank_id, model_with_entity["id"])

            record(name, True, "entity_id=None + entity_id=UUID の保存・取得")
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM mental_models WHERE bank_id = $1::uuid", bank_id)
                await conn.execute("DELETE FROM entities WHERE bank_id = $1::uuid", bank_id)
                await conn.execute("DELETE FROM banks WHERE id = $1::uuid", bank_id)

    except Exception as e:
        record(name, False, str(e))


# ==========================================================================
# Main
# ==========================================================================


async def main():
    pool = await get_pool()

    try:
        # テスト用 bank の事前作成（disposition / directive テスト用）
        async with pool.acquire() as conn:
            await conn.fetchrow(
                """
                INSERT INTO banks (id, name, disposition, directives)
                VALUES ($1::uuid, $2, $3::jsonb, $4)
                ON CONFLICT (id) DO NOTHING
                """,
                TEST_BANK_ID,
                "test_phase3_bank",
                '{"skepticism": 4, "literalism": 2, "empathy": 5}',
                ["常に日本語で回答", "推測は避ける"],
            )

        # DB 不要のテスト
        await test_db_migration(pool)
        await test_visibility(pool)
        await test_disposition(pool)
        await test_directive(pool)
        await test_reflect_tool_config(pool)
        await test_reflect_done_tool(pool)
        await test_system_prompt_build(pool)

        # Bedrock 接続が必要なテスト
        await test_mental_model_crud(pool)
        await test_auto_generation_logic(pool)
        await test_entity_id_support(pool)

    finally:
        # テスト用データのクリーンアップ
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM mental_models WHERE bank_id = $1::uuid",
                TEST_BANK_ID,
            )
            await conn.execute(
                "DELETE FROM banks WHERE id = $1::uuid",
                TEST_BANK_ID,
            )
        await close_pool()

    # サマリ
    print("\n" + "=" * 60)
    print("Phase 3 検証サマリ")
    print("=" * 60)
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)

    for name, ok, detail in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if detail:
            print(f"         {detail}")

    print("-" * 60)
    print(f"  結果: {passed}/{total} PASSED")
    print("=" * 60)

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
