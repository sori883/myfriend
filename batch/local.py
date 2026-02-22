"""ローカル開発用 Consolidation CLI

Usage:
    cd batch
    uv run python local.py                    # 単発実行
    uv run python local.py --interval 60      # 60秒間隔で連続実行
    uv run python local.py --bank-id <uuid>   # 単一 bank
"""

from dotenv import load_dotenv

load_dotenv()

import asyncio
import json
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from memory.db import close_pool, get_pool

from consolidation import consolidate

logger = logging.getLogger(__name__)


async def _run_sweep(pool) -> dict:
    """全 bank を1回 sweep する"""
    started_at = time.monotonic()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM banks ORDER BY id")

    results = {}
    total = 0
    for row in rows:
        bid = str(row["id"])
        try:
            res = await consolidate(pool, bid)
            results[bid] = res
            total += res.get("processed", 0)
        except Exception:
            logger.exception("Failed for bank %s", bid)

    elapsed = (time.monotonic() - started_at) * 1000
    return {
        "banks": len(rows),
        "total_processed": total,
        "elapsed_ms": round(elapsed),
    }


async def _main(bank_id: str | None, interval: int | None) -> None:
    pool = await get_pool()
    try:
        if bank_id:
            result = await consolidate(pool, bank_id)
            print(json.dumps(result, indent=2, default=str))
            return

        if interval is not None:
            logger.info(
                "Continuous mode: every %ds (Ctrl+C to stop)", interval
            )
            while True:
                result = await _run_sweep(pool)
                print(json.dumps(result, indent=2, default=str))
                await asyncio.sleep(interval)
        else:
            result = await _run_sweep(pool)
            print(json.dumps(result, indent=2, default=str))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await close_pool()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Consolidation batch CLI")
    parser.add_argument("--bank-id", type=str, default=None)
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Continuous mode: run every N seconds",
    )
    args = parser.parse_args()

    asyncio.run(_main(args.bank_id, args.interval))
