"""DB 接続確認スクリプト

Usage: uv run python test_script/check_db.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# agentcore/ をモジュール検索パスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from memory.db import get_pool, close_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def check_db():
    """DB 接続確認"""
    pool = await get_pool()

    row = await pool.fetchrow("SELECT 1 AS ok")
    logger.info("DB connection OK: %s", row)

    tables = await pool.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    logger.info("Tables: %s", [t["tablename"] for t in tables])

    extensions = await pool.fetch(
        "SELECT extname FROM pg_extension ORDER BY extname"
    )
    logger.info("Extensions: %s", [e["extname"] for e in extensions])

    await close_pool()


if __name__ == "__main__":
    asyncio.run(check_db())
