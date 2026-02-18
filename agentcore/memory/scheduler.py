"""Consolidation スケジューラー

asyncio ベースの定期実行スケジューラー。
MemoryEngine 起動時にバックグラウンドタスクとして開始し、
5分間隔（デフォルト）で未統合 Fact の Consolidation を実行する。

商用化時は EventBridge + Step Functions に置き換える。
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)

CONSOLIDATION_INTERVAL_SECONDS = max(
    10, int(os.environ.get("CONSOLIDATION_INTERVAL_SECONDS", "300"))
)


class ConsolidationScheduler:
    """asyncio ベースの Consolidation 定期実行スケジューラー"""

    def __init__(
        self,
        pool: asyncpg.Pool,
        interval_seconds: int = CONSOLIDATION_INTERVAL_SECONDS,
    ) -> None:
        self._pool = pool
        self._interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """バックグラウンドタスクとして定期実行を開始する"""
        if self._task is not None and not self._task.done():
            logger.warning("Scheduler is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Consolidation scheduler started (interval=%ds)", self._interval
        )

    async def stop(self) -> None:
        """スケジューラーを停止する"""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Consolidation scheduler stopped")

    async def trigger(self) -> dict:
        """手動トリガー（開発・デバッグ用）

        Returns:
            Consolidation 実行結果
        """
        logger.info("Manual consolidation triggered")
        return await self._execute_consolidation()

    async def _run_loop(self) -> None:
        """内部ループ: interval_seconds 間隔で Consolidation を実行する"""
        # 起動直後は1回待ってから開始
        await asyncio.sleep(self._interval)

        while self._running:
            try:
                result = await self._execute_consolidation()
                logger.info("Scheduled consolidation completed: %s", result)
            except asyncio.CancelledError:
                break
            except asyncpg.PostgresError as e:
                logger.error("Consolidation DB error: %s", e)
            except Exception:
                logger.exception("Unexpected error in consolidation loop")

            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break

    async def _execute_consolidation(self) -> dict:
        """1回分の Consolidation を実行する

        全 bank を列挙し、各 bank に対して consolidation を実行する。

        Returns:
            実行結果の辞書
        """
        from memory.consolidation import consolidate

        started_at = time.monotonic()

        async with self._pool.acquire() as conn:
            bank_rows = await conn.fetch("SELECT id FROM banks")

        results = {}
        total_processed = 0
        for row in bank_rows:
            bank_id = str(row["id"])
            try:
                result = await consolidate(self._pool, bank_id)
                results[bank_id] = result
                total_processed += result.get("processed", 0)
            except Exception:
                logger.exception("Consolidation failed for bank %s", bank_id)
                results[bank_id] = {"error": True, "processed": 0}

        elapsed_ms = (time.monotonic() - started_at) * 1000

        logger.info(
            "Consolidation: %d banks processed, %d facts consolidated (%.0fms)",
            len(bank_rows),
            total_processed,
            elapsed_ms,
        )

        return {
            "banks_processed": len(bank_rows),
            "total_processed": total_processed,
            "results": results,
            "elapsed_ms": round(elapsed_ms),
        }


# ==========================================================
# CLI: 手動実行
# ==========================================================

async def _run_cli(interval: int | None) -> None:
    """CLI 実行のメインロジック"""
    from memory.db import close_pool, get_pool

    pool = await get_pool()
    try:
        effective_interval = interval if interval is not None else CONSOLIDATION_INTERVAL_SECONDS
        scheduler = ConsolidationScheduler(pool, interval_seconds=effective_interval)

        if interval is not None:
            # 連続実行モード
            logger.info("Running consolidation every %ds (Ctrl+C to stop)", interval)
            while True:
                try:
                    result = await scheduler.trigger()
                    print(f"Result: {result}")
                    await asyncio.sleep(interval)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    break
        else:
            # 単発実行モード
            result = await scheduler.trigger()
            print(f"Result: {result}")
    finally:
        await close_pool()


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Consolidation scheduler CLI"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Continuous mode: run every N seconds (default: single run)",
    )
    args = parser.parse_args()

    asyncio.run(_run_cli(args.interval))
