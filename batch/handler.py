"""Batch consolidation Lambda handler.

EventBridge で定期実行。ローカルテストも可能。

Event payload:
  {}                    → 全 bank sweep
  {"bank_id": "uuid"}   → 単一 bank

環境変数:
  DATABASE_URL                  (required)
  AWS_REGION                    (default: ap-northeast-1)
  CONSOLIDATION_MODEL_ID        (optional)
  REFLECT_MODEL_ID              (optional)
  EMBEDDING_MODEL_ID            (optional)
  BATCH_MAX_BANKS               (default: 50)
  LAMBDA_SAFETY_TIMEOUT_SECONDS (default: 780)
"""

import asyncio
import json
import logging
import os
import time

from memory.db import close_pool, get_pool

from consolidation import consolidate

logger = logging.getLogger(__name__)


async def _run_batch(event: dict) -> dict:
    """バッチ処理の async 本体

    毎回 pool を作成・破棄する設計。Lambda ウォームスタートでの
    プール再利用は行わない（接続の stale 化を回避するため）。
    """
    max_banks = int(os.environ.get("BATCH_MAX_BANKS", "50"))
    safety_timeout = int(os.environ.get("LAMBDA_SAFETY_TIMEOUT_SECONDS", "780"))

    pool = await get_pool()
    try:
        bank_id = event.get("bank_id")

        if bank_id:
            result = await consolidate(pool, bank_id)
            return {"mode": "single", "bank_id": bank_id, **result}

        # sweep mode
        started_at = time.monotonic()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM banks ORDER BY id LIMIT $1", max_banks
            )

        results = {}
        total_processed = 0
        attempted = 0

        for row in rows:
            bid = str(row["id"])
            if time.monotonic() - started_at > safety_timeout:
                logger.warning(
                    "Timeout safety: stopping after %d banks", attempted
                )
                break

            attempted += 1
            try:
                res = await consolidate(pool, bid)
                results[bid] = res
                total_processed += res.get("processed", 0)
            except Exception:
                logger.exception("Failed for bank %s", bid)
                results[bid] = {"error": True}

        return {
            "mode": "sweep",
            "banks_total": len(rows),
            "banks_attempted": attempted,
            "total_processed": total_processed,
            "results": results,
            "elapsed_ms": round((time.monotonic() - started_at) * 1000),
        }
    finally:
        await close_pool()


def handler(event, context):
    """Lambda handler"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Batch invoked: %s", json.dumps(event))
    result = asyncio.run(_run_batch(event))
    logger.info("Batch complete: %s", json.dumps(result, default=str))
    return result
