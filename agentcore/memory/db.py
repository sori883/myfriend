import logging
import os

import asyncpg
from pgvector.asyncpg import register_vector

logger = logging.getLogger(__name__)

# MemoryEngine (1.2.1) で encapsulate 予定。
# 現時点ではモジュールレベルのシングルトンとして管理。
_pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    """接続初期化コールバック: pgvector 型を登録"""
    await register_vector(conn)


async def get_pool() -> asyncpg.Pool:
    """接続プールを取得する（未作成なら作成）

    DATABASE_URL 環境変数から接続文字列を読み込む。
    商用化時は RDS Proxy エンドポイントに差し替えるだけで移行可能。
    """
    global _pool
    # asyncpg.Pool には公開 API としての closed 判定がないため _closed を使用
    if _pool is not None and not _pool._closed:
        return _pool

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    try:
        _pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=2,
            max_size=10,
            init=_init_connection,
        )
    except asyncpg.InvalidCatalogNameError as e:
        raise RuntimeError(
            f"Database not found. Ensure PostgreSQL is running and the database exists: {e}"
        ) from e
    except OSError as e:
        raise RuntimeError(
            f"Cannot connect to database. Ensure PostgreSQL is running: {e}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Failed to create database connection pool: {e}") from e

    logger.info("Database connection pool created (min=2, max=10)")
    return _pool


async def close_pool() -> None:
    """接続プールを閉じる"""
    global _pool
    if _pool is not None and not _pool._closed:
        await _pool.close()
        logger.info("Database connection pool closed")
    _pool = None
