"""Consolidation スケジューラー

asyncio ベースの定期実行スケジューラー。
MemoryEngine 起動時にバックグラウンドタスクとして開始し、
5分間隔（デフォルト）で未統合 Fact の Consolidation を実行する。

商用化時は EventBridge + Step Functions に置き換える。
"""

import asyncio
import logging
import os
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

        Phase 2.2.1 で実装予定。現在は未統合 Fact 数をカウントするスタブ。

        Returns:
            実行結果の辞書
        """
        started_at = datetime.now(timezone.utc)

        async with self._pool.acquire() as conn:
            # NOTE: 全バンク横断でカウント（Phase 2.2.1 で bank_id 単位の処理に変更予定）
            unconsolidated_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM memory_units
                WHERE consolidated_at IS NULL
                  AND fact_type IN ('world', 'experience')
                """
            )

        elapsed_ms = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds() * 1000

        logger.info(
            "Consolidation stub: %d unconsolidated facts found (%.0fms)",
            unconsolidated_count,
            elapsed_ms,
        )

        return {
            "unconsolidated_count": unconsolidated_count,
            "processed": 0,
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
