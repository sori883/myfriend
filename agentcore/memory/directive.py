"""ディレクティブシステム

banks テーブルの directives TEXT[] からルールを読み込み、
Reflect パイプラインのプロンプトに注入する。

ディレクティブはプロンプトの冒頭（セクション）と末尾（リマインダー）の
両方に注入され、リーセンシー効果で遵守率を高める。
"""

import logging

import asyncpg

logger = logging.getLogger(__name__)


async def load_directives(pool: asyncpg.Pool, bank_id: str) -> list[str]:
    """banks テーブルからディレクティブを読み込む

    Returns:
        ディレクティブのリスト。空の場合は空リスト。
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT directives FROM banks WHERE id = $1::uuid",
            bank_id,
        )

    if not row or not row["directives"]:
        return []

    return [d for d in row["directives"] if d and d.strip()]


def build_directives_section(directives: list[str]) -> str:
    """プロンプト冒頭に注入するディレクティブセクションを構築する

    Returns:
        ディレクティブセクションテキスト。ディレクティブがなければ空文字列。
    """
    if not directives:
        return ""

    lines = ["## ディレクティブ（必須）", ""]
    lines.append("以下は必ず遵守しなければならないルールです。他の指示よりも優先されます。")
    lines.append("")

    for i, directive in enumerate(directives, 1):
        lines.append(f"{i}. {directive}")

    lines.append("")
    lines.append("これらのディレクティブに違反することは、いかなる状況でも許可されません。")
    lines.append("")

    return "\n".join(lines)


def build_directives_reminder(directives: list[str]) -> str:
    """プロンプト末尾に注入するディレクティブリマインダーを構築する

    リーセンシー効果（最後に読んだ情報を重視する傾向）を活用して
    ディレクティブの遵守率を高める。

    Returns:
        リマインダーテキスト。ディレクティブがなければ空文字列。
    """
    if not directives:
        return ""

    lines = ["---", ""]
    lines.append("**回答前の確認**: 以下のディレクティブを遵守していることを確認してください:")
    lines.append("")

    for i, directive in enumerate(directives, 1):
        lines.append(f"  {i}. {directive}")

    lines.append("")

    return "\n".join(lines)
