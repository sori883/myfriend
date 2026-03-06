"""E2E テスト: Task 08/09/10 の機能検証

Usage:
    cd agentcore
    uv run e2e_test.py
"""
from dotenv import load_dotenv
load_dotenv(".env.local")

import asyncio
import json
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("e2e_test")

BANK_ID = "00000000-0000-0000-0000-000000000001"
ENTITY_ID = "00000000-0000-0000-0000-000000000010"

passed = 0
failed = 0


def ok(name: str, detail: str = ""):
    global passed
    passed += 1
    msg = f"  PASS: {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def ng(name: str, detail: str = ""):
    global failed
    failed += 1
    msg = f"  FAIL: {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def assert_test(name: str, condition: bool, detail: str = ""):
    if condition:
        ok(name, detail)
    else:
        ng(name, detail)


# =========================================================================
# Task 08: 嗜好抽出パイプライン
# =========================================================================


async def test_task08(pool):
    print("\n" + "=" * 60)
    print("Task 08: 嗜好抽出パイプライン")
    print("=" * 60)

    from recommendation.preference_query import get_owner_entity, query_preferences
    from recommendation.preference_extractor import persist_preferences, ExtractedPreference

    # --- 08-1: entity 解決 ---
    print("\n--- 08-1: entity 解決 ---")

    owner = await get_owner_entity(pool, BANK_ID)
    assert_test(
        "owner_entity_id 解決",
        owner is not None and owner[0] == ENTITY_ID,
        f"entity_id={owner[0] if owner else 'None'}",
    )

    owner_none = await get_owner_entity(pool, "00000000-0000-0000-0000-999999999999")
    assert_test(
        "存在しない bank_id → None",
        owner_none is None,
    )

    # --- 08-2: 嗜好の永続化 ---
    print("\n--- 08-2: 嗜好の永続化 ---")

    # テスト前にクリーンアップ
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM preference_profiles WHERE bank_id = $1::uuid", BANK_ID,
        )

    test_preferences = [
        ExtractedPreference(category="food", item="ラーメン", sentiment="positive", intensity=0.8),
        ExtractedPreference(category="food", item="寿司", sentiment="positive", intensity=0.9),
        ExtractedPreference(category="food", item="パクチー", sentiment="negative", intensity=0.7),
        ExtractedPreference(category="music", item="ジャズ", sentiment="positive", intensity=0.6),
        ExtractedPreference(category="place", item="京都", sentiment="positive", intensity=0.85),
    ]

    result = await persist_preferences(pool, BANK_ID, test_preferences, ENTITY_ID)
    assert_test(
        "嗜好の永続化",
        result.get("stored", 0) == 5,
        f"stored={result.get('stored')}, result={result}",
    )

    # --- 08-3: 再言及による intensity 更新 (EMA) ---
    print("\n--- 08-3: 再言及 (EMA 更新) ---")

    re_mention = [
        ExtractedPreference(category="food", item="ラーメン", sentiment="positive", intensity=1.0),
    ]
    result2 = await persist_preferences(pool, BANK_ID, re_mention, ENTITY_ID)
    assert_test(
        "再言及 UPSERT 成功",
        result2.get("stored", 0) == 1,
        f"result={result2}",
    )

    # intensity は EMA: old(0.8) * 0.7 + new(1.0) * 0.3 = 0.86
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT intensity, evidence_count FROM preference_profiles "
            "WHERE bank_id = $1::uuid AND item = 'ラーメン'",
            BANK_ID,
        )
    assert_test(
        "EMA intensity 更新",
        row is not None and abs(row["intensity"] - 0.86) < 0.01,
        f"intensity={row['intensity'] if row else 'N/A'} (expected ~0.86)",
    )
    assert_test(
        "evidence_count インクリメント",
        row is not None and row["evidence_count"] == 2,
        f"evidence_count={row['evidence_count'] if row else 'N/A'}",
    )

    # --- 08-4: item 名寄せ ---
    print("\n--- 08-4: item 名寄せ ---")

    similar_item = [
        ExtractedPreference(category="food", item="味噌ラーメン", sentiment="positive", intensity=0.7),
    ]
    result3 = await persist_preferences(pool, BANK_ID, similar_item, ENTITY_ID)
    # pg_trgm で「ラーメン」と「味噌ラーメン」の類似度次第
    assert_test(
        "名寄せ UPSERT 成功",
        result3.get("stored", 0) >= 1,
        f"result={result3}",
    )

    # --- 08-5: 嗜好クエリ ---
    print("\n--- 08-5: 嗜好クエリ ---")

    profile_food = await query_preferences(pool, BANK_ID, ENTITY_ID, "food")
    assert_test(
        "カテゴリ指定あり (food)",
        profile_food.get("total_count", 0) >= 2,
        f"total_count={profile_food.get('total_count')}, prefs={json.dumps(profile_food.get('preferences', {}), ensure_ascii=False)[:200]}",
    )

    profile_all = await query_preferences(pool, BANK_ID, ENTITY_ID, "")
    assert_test(
        "カテゴリ指定なし (全件)",
        profile_all.get("total_count", 0) >= 4,
        f"total_count={profile_all.get('total_count')}",
    )

    # --- 08-6: PreferenceEngine ファサード ---
    print("\n--- 08-6: PreferenceEngine ファサード ---")

    from recommendation.engine import PreferenceEngine

    engine = PreferenceEngine()
    profile = await engine.query_profile(BANK_ID, "food")
    assert_test(
        "engine.query_profile (food)",
        "preferences" in profile and profile.get("total_count", 0) >= 2,
        f"total_count={profile.get('total_count')}",
    )

    profile_empty = await engine.query_profile("00000000-0000-0000-0000-999999999999")
    assert_test(
        "engine.query_profile (存在しない bank)",
        "message" in profile_empty and profile_empty.get("total_count", 0) == 0,
        f"message={profile_empty.get('message')}",
    )


# =========================================================================
# Task 09: レコメンデーションエンジン
# =========================================================================


async def test_task09(pool):
    print("\n" + "=" * 60)
    print("Task 09: レコメンデーションエンジン")
    print("=" * 60)

    from recommendation.recommendation import get_recommendations, record_feedback
    from recommendation.engine import PreferenceEngine

    # --- 09-1: レコメンデーション生成 ---
    print("\n--- 09-1: レコメンデーション生成 ---")

    rec = await get_recommendations(pool, BANK_ID, ENTITY_ID, "food", "")
    assert_test(
        "food レコメンデーション生成",
        "recommendations" in rec and len(rec["recommendations"]) > 0,
        f"recommendations={len(rec.get('recommendations', []))} items",
    )
    assert_test(
        "recommendation_id 発行",
        "recommendation_id" in rec and rec["recommendation_id"] is not None,
        f"recommendation_id={rec.get('recommendation_id')}",
    )
    assert_test(
        "category が正しい",
        rec.get("category") == "food",
        f"category={rec.get('category')}",
    )

    # --- 09-2: negative アイテムの回避リスト ---
    print("\n--- 09-2: negative アイテムの回避リスト ---")

    avoid = rec.get("avoid", [])
    has_pakuchi = any("パクチー" in item.get("item", "") for item in avoid)
    assert_test(
        "negative 嗜好が回避リストに含まれる",
        has_pakuchi,
        f"avoid={json.dumps(avoid, ensure_ascii=False)}",
    )

    # --- 09-3: 推薦にスコアが含まれる ---
    print("\n--- 09-3: スコア計算 ---")

    if rec["recommendations"]:
        first = rec["recommendations"][0]
        assert_test(
            "intensity が付与されている",
            "intensity" in first and first["intensity"] > 0,
            f"item={first.get('item')}, intensity={first.get('intensity')}",
        )
        assert_test(
            "推薦理由が付与されている",
            "reason" in first and len(first.get("reason", "")) > 0,
            f"reason={first.get('reason')}",
        )

    # --- 09-4: 嗜好データ 0 件のカテゴリ ---
    print("\n--- 09-4: 嗜好データ 0 件 ---")

    rec_empty = await get_recommendations(pool, BANK_ID, ENTITY_ID, "sport", "")
    assert_test(
        "嗜好なしカテゴリで空結果",
        len(rec_empty.get("recommendations", [])) == 0,
        f"message={rec_empty.get('message')}",
    )

    # --- 09-5: 推薦履歴の記録確認 ---
    print("\n--- 09-5: 推薦履歴の記録 ---")

    async with pool.acquire() as conn:
        history = await conn.fetchrow(
            "SELECT * FROM recommendation_history WHERE bank_id = $1::uuid ORDER BY created_at DESC LIMIT 1",
            BANK_ID,
        )
    assert_test(
        "recommendation_history に記録されている",
        history is not None,
        f"id={history['id'] if history else 'N/A'}",
    )

    # --- 09-6: フィードバック記録 ---
    print("\n--- 09-6: フィードバック記録 ---")

    rec_id = rec.get("recommendation_id")
    if rec_id:
        fb = await record_feedback(pool, BANK_ID, rec_id, True, "ラーメン")
        assert_test(
            "フィードバック記録 (accepted)",
            fb.get("updated", 0) == 1,
            f"result={fb}",
        )

        # 二重書き込み防止
        fb_dup = await record_feedback(pool, BANK_ID, rec_id, False)
        assert_test(
            "二重書き込み防止",
            fb_dup.get("updated", 0) == 0 and "error" in fb_dup,
            f"result={fb_dup}",
        )

        # 別の bank_id からのアクセス拒否
        fb_other = await record_feedback(
            pool, "00000000-0000-0000-0000-999999999999", rec_id, True,
        )
        assert_test(
            "bank_id 認可チェック",
            fb_other.get("updated", 0) == 0,
            f"result={fb_other}",
        )
    else:
        ng("フィードバック記録 (スキップ)", "recommendation_id がない")

    # --- 09-7: 多様性制御（2回目の推薦） ---
    print("\n--- 09-7: 多様性制御 ---")

    rec2 = await get_recommendations(pool, BANK_ID, ENTITY_ID, "food", "")
    assert_test(
        "2回目の推薦生成",
        "recommendations" in rec2,
        f"recommendations={len(rec2.get('recommendations', []))} items",
    )
    # 1回目と同じアイテムのスコアが減衰しているか（直接チェックは難しいのでログで確認）

    # --- 09-8: PreferenceEngine ファサード ---
    print("\n--- 09-8: PreferenceEngine ファサード ---")

    engine = PreferenceEngine()
    rec_facade = await engine.recommend(BANK_ID, "food")
    assert_test(
        "engine.recommend (food)",
        "recommendations" in rec_facade,
        f"recommendations={len(rec_facade.get('recommendations', []))} items",
    )

    rec_no_entity = await engine.recommend("00000000-0000-0000-0000-999999999999", "food")
    assert_test(
        "engine.recommend (存在しない bank)",
        "message" in rec_no_entity and len(rec_no_entity.get("recommendations", [])) == 0,
    )


# =========================================================================
# Task 10: 外部検索連携
# =========================================================================


async def test_task10(pool):
    print("\n" + "=" * 60)
    print("Task 10: 外部検索連携")
    print("=" * 60)

    from recommendation.web_search import (
        SearchRateLimiter,
        search_web,
        _get_cached,
        _normalize_query,
        _search_cache,
    )
    from recommendation.engine import PreferenceEngine

    # --- 10-1: クエリ正規化 ---
    print("\n--- 10-1: クエリ正規化 ---")

    assert_test(
        "strip + lower 正規化",
        _normalize_query("  Tokyo Cafe  ") == "tokyo cafe",
        f"result='{_normalize_query('  Tokyo Cafe  ')}'",
    )

    # --- 10-2: Web 検索 ---
    print("\n--- 10-2: Web 検索 (Tavily API) ---")

    rl = SearchRateLimiter()
    result = await search_web(rl, BANK_ID, "東京 カフェ おすすめ 2025")

    has_results = "results" in result and len(result.get("results", [])) > 0
    assert_test(
        "Tavily API 検索成功",
        has_results,
        f"results={len(result.get('results', []))} items" if has_results else f"result={json.dumps(result, ensure_ascii=False)[:200]}",
    )

    if has_results:
        first = result["results"][0]
        assert_test(
            "結果に title/snippet/url が含まれる",
            all(k in first for k in ("title", "snippet", "url")),
            f"keys={list(first.keys())}",
        )
    assert_test(
        "結果に query が含まれる",
        result.get("query") == "東京 カフェ おすすめ 2025",
    )

    # --- 10-3: キャッシュ ---
    print("\n--- 10-3: キャッシュ ---")

    cached = _get_cached("東京 カフェ おすすめ 2025")
    assert_test(
        "検索結果がキャッシュされている",
        cached is not None,
    )

    # キャッシュヒットでレート制限を消費しないことを確認
    session_before = rl.session_remaining
    result2 = await search_web(rl, BANK_ID, "東京 カフェ おすすめ 2025")
    assert_test(
        "キャッシュヒットでレート制限を消費しない",
        rl.session_remaining == session_before,
        f"remaining: before={session_before}, after={rl.session_remaining}",
    )

    # --- 10-4: レート制限 ---
    print("\n--- 10-4: レート制限 ---")

    # 最小検索間隔チェック (10秒以内の連続検索)
    rl2 = SearchRateLimiter()
    _search_cache.clear()  # キャッシュクリアして実際の制限を試す
    await search_web(rl2, BANK_ID, "テスト検索1")
    result_interval = await search_web(rl2, BANK_ID, "テスト検索2")
    assert_test(
        "最小検索間隔チェック",
        "error" in result_interval and "間隔" in result_interval.get("error", ""),
        f"error={result_interval.get('error', 'none')}",
    )

    # セッション上限チェック
    rl3 = SearchRateLimiter()
    rl3._session_count = 3  # 上限に設定
    result_limit = await search_web(rl3, BANK_ID, "テスト上限")
    assert_test(
        "セッション上限チェック",
        "error" in result_limit and "上限" in result_limit.get("error", ""),
        f"error={result_limit.get('error', 'none')}",
    )

    assert_test(
        "残り回数が返される",
        "remaining_session" in result_limit and "remaining_today" in result_limit,
        f"session={result_limit.get('remaining_session')}, today={result_limit.get('remaining_today')}",
    )

    # --- 10-5: PreferenceEngine ファサード ---
    print("\n--- 10-5: PreferenceEngine ファサード ---")

    _search_cache.clear()
    engine = PreferenceEngine()
    engine_result = await engine.search(BANK_ID, "渋谷 ラーメン")
    assert_test(
        "engine.search 成功",
        "results" in engine_result or "error" in engine_result,
        f"keys={list(engine_result.keys())}",
    )


# =========================================================================
# core.py ツール定義のバリデーションテスト
# =========================================================================


async def test_tool_validation():
    print("\n" + "=" * 60)
    print("ツール定義バリデーション (core.py)")
    print("=" * 60)

    from core import _build_tools

    tools = _build_tools(BANK_ID)
    tool_names = [t.__name__ for t in tools]

    # --- ツールの存在確認 ---
    print("\n--- ツール存在確認 ---")

    expected = [
        "remember", "recall_memories", "reflect_on",
        "get_user_profile", "recommend", "record_recommendation_feedback", "web_search",
    ]
    for name in expected:
        assert_test(
            f"ツール '{name}' が存在",
            name in tool_names,
        )

    # --- get_user_profile バリデーション ---
    print("\n--- get_user_profile バリデーション ---")

    get_profile = next(t for t in tools if t.__name__ == "get_user_profile")
    result = json.loads(get_profile(category="invalid_category"))
    assert_test(
        "無効カテゴリでエラー",
        "error" in result,
        f"error={result.get('error', '')[:60]}",
    )

    result_ok = json.loads(get_profile(category="food"))
    assert_test(
        "有効カテゴリで成功",
        "error" not in result_ok,
        f"total_count={result_ok.get('total_count')}",
    )

    # --- recommend バリデーション ---
    print("\n--- recommend バリデーション ---")

    recommend_tool = next(t for t in tools if t.__name__ == "recommend")
    result = json.loads(recommend_tool(category=""))
    assert_test(
        "空カテゴリでエラー",
        "error" in result,
    )

    result = json.loads(recommend_tool(category="invalid"))
    assert_test(
        "無効カテゴリでエラー",
        "error" in result,
    )

    result_ok = json.loads(recommend_tool(category="food"))
    assert_test(
        "有効カテゴリで推薦生成",
        "recommendations" in result_ok,
        f"recommendations={len(result_ok.get('recommendations', []))} items",
    )

    # --- record_recommendation_feedback バリデーション ---
    print("\n--- record_recommendation_feedback バリデーション ---")

    fb_tool = next(t for t in tools if t.__name__ == "record_recommendation_feedback")
    result = json.loads(fb_tool(recommendation_id="", accepted=True))
    assert_test(
        "空 recommendation_id でエラー",
        "error" in result,
    )

    result = json.loads(fb_tool(recommendation_id="not-a-uuid", accepted=True))
    assert_test(
        "無効 UUID でエラー",
        "error" in result,
    )

    # --- web_search バリデーション ---
    print("\n--- web_search バリデーション ---")

    ws_tool = next(t for t in tools if t.__name__ == "web_search")
    result = json.loads(ws_tool(query=""))
    assert_test(
        "空クエリでエラー",
        "error" in result,
    )

    result = json.loads(ws_tool(query="x" * 1001))
    assert_test(
        "長すぎるクエリでエラー",
        "error" in result,
    )


# =========================================================================
# メイン
# =========================================================================


async def setup_test_data(pool):
    """テスト用の bank + entity を作成する"""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO banks (id, name, mission)
               VALUES ($1::uuid, 'テスト用bank', 'E2Eテスト用')
               ON CONFLICT (id) DO NOTHING""",
            BANK_ID,
        )
        await conn.execute(
            """INSERT INTO entities (id, bank_id, canonical_name, entity_type)
               VALUES ($1::uuid, $2::uuid, 'テストユーザー', 'person')
               ON CONFLICT (id) DO NOTHING""",
            ENTITY_ID, BANK_ID,
        )
        await conn.execute(
            "UPDATE banks SET owner_entity_id = $1::uuid WHERE id = $2::uuid",
            ENTITY_ID, BANK_ID,
        )
    print("  テストデータを作成しました")


async def main():
    from memory.db import get_pool

    pool = await get_pool()

    print("セットアップ")
    print("=" * 60)
    await setup_test_data(pool)

    try:
        await test_task08(pool)
        await test_task09(pool)
        await test_task10(pool)
        await test_tool_validation()
    finally:
        # テストデータのクリーンアップ
        print("\n" + "=" * 60)
        print("クリーンアップ")
        print("=" * 60)
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM recommendation_history WHERE bank_id = $1::uuid", BANK_ID,
            )
            await conn.execute(
                "DELETE FROM preference_profiles WHERE bank_id = $1::uuid", BANK_ID,
            )
            # FK 制約: banks.owner_entity_id → entities.id なので先に解除
            await conn.execute(
                "UPDATE banks SET owner_entity_id = NULL WHERE id = $1::uuid", BANK_ID,
            )
            await conn.execute(
                "DELETE FROM entities WHERE bank_id = $1::uuid", BANK_ID,
            )
            await conn.execute(
                "DELETE FROM banks WHERE id = $1::uuid", BANK_ID,
            )
        print("  テストデータを削除しました")
        await pool.close()

    # 結果サマリ
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"結果: {passed}/{total} passed, {failed}/{total} failed")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
