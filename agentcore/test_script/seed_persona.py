"""テスト用ペルソナ発話データの自動DB登録スクリプト

docs/ペルソナ/ユーザー発話.md の発話を1行ずつ順番に
retain（記憶抽出）→ preference extract（嗜好抽出）の順で同期的にDBに投入する。
一連の会話として処理するため、行間にウェイトを入れてスロットリングを回避する。

Usage:
    cd agentcore
    uv run python test_script/seed_persona.py
    uv run python test_script/seed_persona.py --bank-id <UUID>
    uv run python test_script/seed_persona.py --file ../docs/ペルソナ/ユーザー発話.md
"""

import argparse
import asyncio
import json
import logging
import sys

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()  # .env もフォールバックとして読む

from memory.engine import MemoryEngine
from recommendation import PreferenceEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_FILE = Path(__file__).resolve().parent.parent.parent / "docs" / "ペルソナ" / "ユーザー発話.md"
THROTTLE_SECONDS = 2
OPERATION_TIMEOUT = 60


def parse_utterances(file_path: Path) -> list[str]:
    """発話ファイルを1行ずつリストに分割する（空行はスキップ）"""
    text = file_path.read_text(encoding="utf-8")
    return [line.strip() for line in text.splitlines() if line.strip()]


async def seed(bank_id: str, file_path: Path) -> None:
    """発話データを1行ずつ同期的にDBに登録する"""
    lines = parse_utterances(file_path)
    if not lines:
        logger.error("発話データが見つかりません: %s", file_path)
        return

    logger.info("bank_id: %s", bank_id)
    logger.info("発話ファイル: %s", file_path)
    logger.info("発話行数: %d", len(lines))

    memory_engine = MemoryEngine()
    preference_engine = PreferenceEngine()

    try:
        await memory_engine.initialize()
        await memory_engine.ensure_bank(bank_id)

        total_facts = 0
        total_prefs = 0
        errors = 0

        for i, line in enumerate(lines, 1):
            logger.info("--- [%d/%d] %s ---", i, len(lines), line[:60])

            # retain → preference extract の順で同期的に実行
            try:
                fact_result = await asyncio.wait_for(
                    memory_engine.retain(bank_id, line),
                    timeout=OPERATION_TIMEOUT,
                )
                logger.info("[%d] retain: %s", i, json.dumps(fact_result, ensure_ascii=False))
                total_facts += fact_result.get("stored", 0) if isinstance(fact_result, dict) else 0
            except Exception:
                logger.error("[%d] retain 失敗", i, exc_info=True)
                errors += 1

            try:
                pref_result = await asyncio.wait_for(
                    preference_engine.extract(bank_id, line),
                    timeout=OPERATION_TIMEOUT,
                )
                logger.info("[%d] preference: %s", i, json.dumps(pref_result, ensure_ascii=False))
                total_prefs += pref_result.get("stored", 0) if isinstance(pref_result, dict) else 0
            except Exception:
                logger.error("[%d] preference extract 失敗", i, exc_info=True)
                errors += 1

            if i < len(lines):
                await asyncio.sleep(THROTTLE_SECONDS)

        logger.info("=== 完了 ===")
        logger.info("bank_id: %s", bank_id)
        logger.info("保存ファクト数: %d", total_facts)
        logger.info("保存嗜好数: %d", total_prefs)
        if errors:
            logger.warning("エラー数: %d", errors)

    finally:
        await memory_engine.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="ペルソナ発話データをDBに登録する")
    parser.add_argument(
        "--bank-id",
        default="00000000-0000-4000-8000-000000000001",
        help="メモリバンクID（デフォルト: 00000000-0000-4000-8000-000000000001）",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=DEFAULT_FILE,
        help=f"発話ファイルパス（デフォルト: {DEFAULT_FILE}）",
    )
    args = parser.parse_args()

    if not args.file.is_file():
        logger.error(
            "ファイルが見つかりません: %s\n"
            "  カレントディレクトリが agentcore であることを確認してください",
            args.file.resolve(),
        )
        sys.exit(1)

    asyncio.run(seed(args.bank_id, args.file))


if __name__ == "__main__":
    main()
