"""Phase 2.8 検証スクリプト

7項目の検証を実行し、全項目の PASS/FAIL サマリを出力する。

Usage: uv run python test_script/test_phase2_verification.py
"""

import asyncio
import logging
import sys
import uuid
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

import asyncpg

import memory.recall as recall_module
from memory.db import close_pool, get_pool
from memory.entity import (
    SCORE_THRESHOLD,
    WEIGHT_COOCCURRENCE,
    WEIGHT_NAME,
    WEIGHT_TEMPORAL,
    _score_candidate,
    resolve_entities,
)
from memory.recall import _extract_time_range

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

TEST_BANK_ID = str(uuid.uuid4())

results: list[tuple[str, bool, str]] = []


class _RollbackTest(Exception):
    """テスト用ロールバック例外"""


def record(name: str, passed: bool, detail: str = "") -> None:
    tag = "PASS" if passed else "FAIL"
    logger.info("[%s] %s %s", tag, name, detail)
    results.append((name, passed, detail))


# ========================================================================
# 項目 5-7: 純粋関数テスト（DB 不要）
# ========================================================================


def test_5_last_month_calendar_boundary():
    """「先月」で正確にカレンダー月の記憶が返る"""
    fixed_now = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    original = recall_module._now_utc

    try:
        recall_module._now_utc = lambda: fixed_now
        recall_module._RELATIVE_PATTERNS = recall_module._build_relative_patterns()

        result = _extract_time_range("先月の出来事")

        expected_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        expected_end = datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)

        ok = result is not None and result[0] == expected_start and result[1] == expected_end
        detail = f"got {result}, expected ({expected_start}, {expected_end})"
        record("5. 「先月」カレンダー月境界", ok, detail)
    finally:
        recall_module._now_utc = original
        recall_module._RELATIVE_PATTERNS = recall_module._build_relative_patterns()


def test_6_absolute_year_month():
    """「2025年6月」で正確に6月の記憶が返る"""
    result = _extract_time_range("2025年6月の思い出")

    expected_start = datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)
    expected_end = datetime(2025, 7, 1, 0, 0, 0, tzinfo=UTC)

    ok = result is not None and result[0] == expected_start and result[1] == expected_end
    detail = f"got {result}, expected ({expected_start}, {expected_end})"
    record("6. 「2025年6月」絶対月検索", ok, detail)


def test_7_last_weekday():
    """「先週の月曜日」で正確な日付の記憶が返る"""
    # 2026-02-19 は木曜日 (weekday=3)
    fixed_now = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    original = recall_module._now_utc

    try:
        recall_module._now_utc = lambda: fixed_now
        recall_module._RELATIVE_PATTERNS = recall_module._build_relative_patterns()

        result = _extract_time_range("先週の月曜日に何があった？")

        # 2026-02-19 (木): 先週の月曜日 = 2026-02-09
        # days_since_monday = 3, last_week_monday = 2/19 - 10 = 2/9
        expected_start = datetime(2026, 2, 9, 0, 0, 0, tzinfo=UTC)
        expected_end = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)

        ok = result is not None and result[0] == expected_start and result[1] == expected_end
        detail = f"got {result}, expected ({expected_start}, {expected_end})"
        record("7. 「先週の月曜日」曜日検索", ok, detail)
    finally:
        recall_module._now_utc = original
        recall_module._RELATIVE_PATTERNS = recall_module._build_relative_patterns()


# ========================================================================
# 項目 1-4: DB 統合テスト
# ========================================================================


async def _setup_test_bank(conn) -> None:
    """テスト用 bank を作成する"""
    await conn.execute(
        "INSERT INTO banks (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        uuid.UUID(TEST_BANK_ID),
        "test-phase2-verification",
    )


async def _cleanup_test_bank(conn) -> None:
    """テスト用 bank を CASCADE 削除する"""
    await conn.execute(
        "DELETE FROM banks WHERE id = $1",
        uuid.UUID(TEST_BANK_ID),
    )


async def test_1_cooccurrence_entity_merging(pool):
    """同一人物の異名が共起エンティティにより統合される

    検証ロジック:
    - "Alice Chen" vs "Alice": name_sim = 0.667, name_score = 0.667 * 0.5 = 0.333 < 0.6
    - Bob との共起: nearby={"bob"}, co_entities(Alice)={"bob"}
      → overlap=1, len=1 → co_score = (1/1) * 0.3 = 0.3
      → total = 0.333 + 0.3 = 0.633 >= 0.6
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _setup_test_bank(conn)

                alice_row = await conn.fetchrow(
                    """INSERT INTO entities (bank_id, canonical_name, entity_type, last_seen)
                       VALUES ($1::uuid, 'Alice', 'person', NOW())
                       RETURNING id""",
                    TEST_BANK_ID,
                )
                alice_id = str(alice_row["id"])

                bob_row = await conn.fetchrow(
                    """INSERT INTO entities (bank_id, canonical_name, entity_type, last_seen)
                       VALUES ($1::uuid, 'Bob', 'person', NOW())
                       RETURNING id""",
                    TEST_BANK_ID,
                )
                bob_id = str(bob_row["id"])

                # Alice-Bob 共起レコード（CHECK: entity_id_1 < entity_id_2）
                id1, id2 = sorted([alice_id, bob_id])
                await conn.execute(
                    """INSERT INTO entity_cooccurrences
                       (entity_id_1, entity_id_2, bank_id, cooccurrence_count)
                       VALUES ($1::uuid, $2::uuid, $3::uuid, 5)""",
                    id1,
                    id2,
                    TEST_BANK_ID,
                )

                # --- 検証 A: 共起なし（Alice Chen 単独）→ 閾値未満 ---
                await conn.execute("SAVEPOINT sp_test1a")
                resolved_no_cooc = await resolve_entities(
                    conn, TEST_BANK_ID, ["Alice Chen"],
                )
                no_cooc_matched = any(
                    e.canonical_name == "Alice" and not e.is_new
                    for e in resolved_no_cooc
                )
                await conn.execute("ROLLBACK TO SAVEPOINT sp_test1a")

                # --- 検証 B: 共起あり（Alice Chen + Bob）→ ブーストで閾値超え ---
                await conn.execute("SAVEPOINT sp_test1b")
                resolved_with_cooc = await resolve_entities(
                    conn, TEST_BANK_ID, ["Alice Chen", "Bob"],
                )
                with_cooc_matched = any(
                    e.canonical_name == "Alice" and not e.is_new
                    for e in resolved_with_cooc
                )
                await conn.execute("ROLLBACK TO SAVEPOINT sp_test1b")

                name_sim = SequenceMatcher(None, "alice chen", "alice").ratio()
                name_only = name_sim * WEIGHT_NAME

                ok = (not no_cooc_matched) and with_cooc_matched
                detail = (
                    f"without_cooc={no_cooc_matched} (expected False), "
                    f"with_cooc={with_cooc_matched} (expected True), "
                    f"name_only={name_only:.4f} < {SCORE_THRESHOLD}"
                )
                record("1. 共起エンティティによる異名統合", ok, detail)

                raise _RollbackTest()
    except _RollbackTest:
        pass


async def test_2_temporal_proximity_boost(pool):
    """7日以内に出現したエンティティが優先的にマッチする

    _score_candidate を直接呼び出し、時間的近接性によるスコア差を検証する。
    """
    now = datetime.now(UTC)
    entity_text = "Takeshi"
    canonical_name = "Takeshi K"
    candidate_id = "dummy-id"
    empty_cooc: dict[str, set[str]] = {}

    name_sim = SequenceMatcher(None, "takeshi", "takeshi k").ratio()
    logger.info("Name similarity('%s', '%s') = %.4f", entity_text, canonical_name, name_sim)

    # 1日前（7日ウィンドウ内）
    score_recent = _score_candidate(
        entity_text, canonical_name, candidate_id,
        set(), empty_cooc,
        event_date=now, last_seen=now - timedelta(days=1),
    )

    # 30日前（7日ウィンドウ外）
    score_old = _score_candidate(
        entity_text, canonical_name, candidate_id,
        set(), empty_cooc,
        event_date=now, last_seen=now - timedelta(days=30),
    )

    # 時間情報なし
    score_no_temporal = _score_candidate(
        entity_text, canonical_name, candidate_id,
        set(), empty_cooc,
        event_date=None, last_seen=None,
    )

    temporal_boost = score_recent - score_no_temporal
    ok = score_recent > score_old and temporal_boost > 0
    detail = (
        f"recent(1d)={score_recent:.4f}, old(30d)={score_old:.4f}, "
        f"no_temporal={score_no_temporal:.4f}, boost={temporal_boost:.4f}"
    )
    record("2. 7日以内エンティティの優先マッチ", ok, detail)


async def test_3_constant_query_count(pool):
    """エンティティ解決の DB クエリ数がエンティティ数に依存しない

    バッチ設計: fetch_all(1) + fetch_cooccurrence(1) + batch_create(1) = 3回
    （全て新規エンティティの場合、batch_update は entity_ids=[] でスキップ）
    """
    EXPECTED_QUERY_COUNT = 3

    class QueryCounter:
        def __init__(self, real_conn):
            self._conn = real_conn
            self.count = 0

        async def fetch(self, *args, **kwargs):
            self.count += 1
            return await self._conn.fetch(*args, **kwargs)

        async def fetchrow(self, *args, **kwargs):
            self.count += 1
            return await self._conn.fetchrow(*args, **kwargs)

        async def execute(self, *args, **kwargs):
            self.count += 1
            return await self._conn.execute(*args, **kwargs)

        async def executemany(self, *args, **kwargs):
            self.count += 1
            return await self._conn.executemany(*args, **kwargs)

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _setup_test_bank(conn)

                for i in range(10):
                    await conn.execute(
                        """INSERT INTO entities (bank_id, canonical_name, entity_type, last_seen)
                           VALUES ($1::uuid, $2, 'person', NOW())""",
                        TEST_BANK_ID,
                        f"Person{i}",
                    )

                # --- 1 エンティティで解決 ---
                await conn.execute("SAVEPOINT sp_count1")
                counter1 = QueryCounter(conn)
                await resolve_entities(counter1, TEST_BANK_ID, ["NewAlpha"])
                count_1 = counter1.count
                await conn.execute("ROLLBACK TO SAVEPOINT sp_count1")

                # --- 10 エンティティで解決 ---
                await conn.execute("SAVEPOINT sp_count10")
                counter10 = QueryCounter(conn)
                names_10 = [f"NewEntity{i}" for i in range(10)]
                await resolve_entities(counter10, TEST_BANK_ID, names_10)
                count_10 = counter10.count
                await conn.execute("ROLLBACK TO SAVEPOINT sp_count10")

                ok = count_1 == count_10 and count_1 == EXPECTED_QUERY_COUNT
                detail = (
                    f"1 entity: {count_1} queries, 10 entities: {count_10} queries, "
                    f"expected: {EXPECTED_QUERY_COUNT}"
                )
                record("3. DB クエリ数がエンティティ数に依存しない", ok, detail)

                raise _RollbackTest()
    except _RollbackTest:
        pass


async def test_4_cooccurrences_updated_on_retain(pool):
    """entity_cooccurrences テーブルが Retain 時に更新される

    LLM + Embedding を使用した統合テスト。
    LLM の出力は非決定的なため、最低限の検証（cooccurrence_count > 0）を行う。
    """
    from memory.retain import retain

    async with pool.acquire() as conn:
        await _setup_test_bank(conn)

    content = (
        "Yesterday Alice and Bob went to the park together. "
        "They met Charlie at the cafe."
    )

    try:
        result = await retain(pool, TEST_BANK_ID, content, "test context")
        logger.info("Retain result: stored=%d", result["stored"])

        if result["stored"] == 0:
            record(
                "4. Retain 時の entity_cooccurrences 更新",
                False,
                "No facts stored — LLM extraction returned empty",
            )
            return

        async with pool.acquire() as conn:
            cooc_count = await conn.fetchval(
                """SELECT COUNT(*) FROM entity_cooccurrences
                   WHERE bank_id = $1::uuid""",
                TEST_BANK_ID,
            )
            entity_count = await conn.fetchval(
                """SELECT COUNT(*) FROM entities
                   WHERE bank_id = $1::uuid""",
                TEST_BANK_ID,
            )

        ok = cooc_count > 0
        detail = (
            f"entities={entity_count}, cooccurrences={cooc_count}, "
            f"links={result.get('links', {})}"
        )
        record("4. Retain 時の entity_cooccurrences 更新", ok, detail)

    finally:
        async with pool.acquire() as conn:
            await _cleanup_test_bank(conn)


# ========================================================================
# メイン
# ========================================================================


async def main():
    logger.info("=" * 60)
    logger.info("Phase 2.8 検証開始")
    logger.info("=" * 60)

    # --- 純粋関数テスト ---
    logger.info("\n--- 純粋関数テスト (項目 5-7) ---")
    test_5_last_month_calendar_boundary()
    test_6_absolute_year_month()
    test_7_last_weekday()

    # --- DB 統合テスト ---
    logger.info("\n--- DB 統合テスト (項目 1-4) ---")
    pool = await get_pool()

    try:
        await test_2_temporal_proximity_boost(pool)
        await test_1_cooccurrence_entity_merging(pool)
        await test_3_constant_query_count(pool)
        await test_4_cooccurrences_updated_on_retain(pool)
    finally:
        await close_pool()

    # --- サマリ ---
    logger.info("\n" + "=" * 60)
    logger.info("検証結果サマリ")
    logger.info("=" * 60)

    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)

    for name, ok, detail in results:
        tag = "PASS" if ok else "FAIL"
        logger.info("  [%s] %s", tag, name)
        if detail and not ok:
            logger.info("         %s", detail)

    logger.info("-" * 60)
    logger.info("結果: %d/%d 通過", passed, total)
    logger.info("=" * 60)

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
