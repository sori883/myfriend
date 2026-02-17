"""Phase 1.2 動作確認スクリプト

Usage: uv run python test_memory.py
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# agentcore/ をモジュール検索パスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from memory.engine import MemoryEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BANK_ID = "00000000-0000-0000-0000-000000000001"


async def test_retain():
    """retain() の動作確認"""
    engine = MemoryEngine()

    logger.info("=== Test 1: retain() ===")

    content = (
        "Yesterday I had lunch with Alice at the Italian restaurant near Shibuya station. "
        "She told me she got a promotion at Google last month. "
        "Alice mentioned she's planning to move to San Francisco in March."
    )
    context = "Casual lunch conversation with a friend"

    result = await engine.retain(BANK_ID, content, context)
    logger.info("Retain result: %s", json.dumps(result, indent=2))

    assert result["stored"] > 0, "No facts were stored!"
    logger.info("PASS: %d facts stored, %d duplicates", result["stored"], result["duplicates"])

    return result


async def test_retain_duplicate(prev_result: dict):
    """重複検出の動作確認"""
    engine = MemoryEngine()

    logger.info("=== Test 2: duplicate detection ===")

    # 同じ内容を再度保存 → 重複検出されるはず
    content = (
        "I had lunch with Alice yesterday near Shibuya. "
        "She got promoted at Google recently."
    )

    result = await engine.retain(BANK_ID, content)
    logger.info("Retain (duplicate) result: %s", json.dumps(result, indent=2))
    logger.info("Stored: %d, Duplicates: %d", result["stored"], result["duplicates"])


async def test_recall():
    """recall() の動作確認"""
    engine = MemoryEngine()

    logger.info("=== Test 3: recall() ===")

    queries = [
        "What happened with Alice?",
        "restaurants in Shibuya",
        "who got promoted recently?",
    ]

    for query in queries:
        result = await engine.recall(BANK_ID, query, "mid")
        memories = result["memories"]
        logger.info(
            "Query: '%s' → %d results (total found: %d)",
            query,
            result["returned"],
            result["total_found"],
        )
        for m in memories[:3]:
            logger.info(
                "  [%.4f] %s (type=%s, kind=%s)",
                m["score"],
                m["text"][:100],
                m["fact_type"],
                m["fact_kind"],
            )

    assert result["returned"] > 0, "No recall results!"
    logger.info("PASS: recall returned results")


async def test_entity_resolution():
    """エンティティ解決の動作確認"""
    engine = MemoryEngine()

    logger.info("=== Test 4: entity resolution ===")

    # "Alice" を含む別の会話を保存
    content = (
        "Alice called me today. She is excited about her new role at Google. "
        "Bob was also on the call, he works at the same team."
    )

    result = await engine.retain(BANK_ID, content)
    logger.info("Retain result: %s", json.dumps(result, indent=2))

    # DB からエンティティを確認
    from memory.db import get_pool
    pool = await get_pool()
    entities = await pool.fetch(
        "SELECT canonical_name, entity_type, mention_count FROM entities WHERE bank_id = $1::uuid ORDER BY mention_count DESC",
        BANK_ID,
    )
    logger.info("Entities in DB:")
    for e in entities:
        logger.info(
            "  %s (type=%s, mentions=%d)",
            e["canonical_name"],
            e["entity_type"],
            e["mention_count"],
        )

    if entities:
        logger.info("PASS: entities resolved")
    else:
        logger.info("WARN: no entities found (may depend on LLM extraction)")


async def test_db_state():
    """DB の最終状態を確認"""
    from memory.db import get_pool

    logger.info("=== Test 5: DB state summary ===")

    pool = await get_pool()

    count = await pool.fetchrow(
        "SELECT COUNT(*) as cnt FROM memory_units WHERE bank_id = $1::uuid",
        BANK_ID,
    )
    logger.info("Total memory_units: %d", count["cnt"])

    entities = await pool.fetchrow(
        "SELECT COUNT(*) as cnt FROM entities WHERE bank_id = $1::uuid",
        BANK_ID,
    )
    logger.info("Total entities: %d", entities["cnt"])

    links = await pool.fetchrow(
        "SELECT COUNT(*) as cnt FROM unit_entities",
    )
    logger.info("Total unit_entities: %d", links["cnt"])


async def cleanup():
    """テスト前にテストバンクのデータをクリアする"""
    from memory.db import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM unit_entities WHERE unit_id IN (SELECT id FROM memory_units WHERE bank_id = $1::uuid)",
                BANK_ID,
            )
            await conn.execute("DELETE FROM memory_units WHERE bank_id = $1::uuid", BANK_ID)
            await conn.execute("DELETE FROM entities WHERE bank_id = $1::uuid", BANK_ID)
    logger.info("=== Cleanup: test data cleared ===")


async def main():
    try:
        await cleanup()
        print()
        prev = await test_retain()
        print()
        await test_retain_duplicate(prev)
        print()
        await test_recall()
        print()
        await test_entity_resolution()
        print()
        await test_db_state()
        print()
        logger.info("=== All tests completed ===")
    except Exception as e:
        logger.error("Test failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        from memory.db import close_pool
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
