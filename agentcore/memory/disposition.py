"""Disposition（性格特性）モジュール

banks テーブルの disposition JSONB から 3 軸の性格特性を読み込み、
Reflect パイプラインのプロンプトに注入するテキストを生成する。

3 軸:
- skepticism (1-5): 懐疑性（高 = 主張を疑う、低 = 信頼する）
- literalism (1-5): 文字通り度（高 = 正確に解釈、低 = 行間を読む）
- empathy (1-5): 共感性（高 = 感情を考慮、低 = 事実に集中）
"""

import json
import logging
from dataclasses import dataclass

import asyncpg

logger = logging.getLogger(__name__)

DEFAULT_VALUE = 3


@dataclass(frozen=True)
class Disposition:
    """性格特性"""
    skepticism: int = DEFAULT_VALUE
    literalism: int = DEFAULT_VALUE
    empathy: int = DEFAULT_VALUE


async def load_disposition(pool: asyncpg.Pool, bank_id: str) -> Disposition:
    """banks テーブルから Disposition を読み込む

    disposition カラムが NULL または不正な場合はデフォルト値を返す。
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT disposition FROM banks WHERE id = $1::uuid",
            bank_id,
        )

    if not row or not row["disposition"]:
        return Disposition()

    d = row["disposition"]
    if isinstance(d, str):
        d = json.loads(d)

    return Disposition(
        skepticism=_clamp(d.get("skepticism", DEFAULT_VALUE)),
        literalism=_clamp(d.get("literalism", DEFAULT_VALUE)),
        empathy=_clamp(d.get("empathy", DEFAULT_VALUE)),
    )


def build_disposition_prompt(disposition: Disposition) -> str:
    """Disposition に基づいたプロンプトテキストを生成する

    中間値（3）の軸はテキストを生成しない（ニュートラル）。

    Returns:
        プロンプトテキスト。全軸がニュートラルなら空文字列。
    """
    guidelines = []

    # Skepticism
    if disposition.skepticism >= 4:
        guidelines.append(
            "主張に懐疑的に対応してください。矛盾する証拠を積極的に探し、"
            "裏付けのない主張には注意を促してください。"
        )
    elif disposition.skepticism <= 2:
        guidelines.append(
            "提供された情報を信頼し、額面通りに受け取ってください。"
            "特に疑わしい点がなければ追加検証は不要です。"
        )

    # Literalism
    if disposition.literalism >= 4:
        guidelines.append(
            "文字通りに解釈してください。正確な約束、具体的な数値、"
            "明示的に述べられた事実に注目してください。"
        )
    elif disposition.literalism <= 2:
        guidelines.append(
            "行間を読み、暗示的な意味を考慮してください。"
            "文脈から推測される意図やニュアンスも回答に反映してください。"
        )

    # Empathy
    if disposition.empathy >= 4:
        guidelines.append(
            "感情状態や置かれた状況を考慮してください。"
            "共感的な視点を持ち、心理的な側面にも注目してください。"
        )
    elif disposition.empathy <= 2:
        guidelines.append(
            "事実と結果に焦点を当ててください。"
            "客観的なデータと論理的な分析を優先してください。"
        )

    if not guidelines:
        return ""

    lines = ["## 推論ガイダンス", ""]
    lines.extend(f"- {guideline}" for guideline in guidelines)
    lines.append("")

    return "\n".join(lines)


def _clamp(value) -> int:
    """値を 1-5 の範囲にクランプする（型安全）"""
    if value is None:
        return DEFAULT_VALUE
    try:
        return max(1, min(5, int(value)))
    except (ValueError, TypeError):
        return DEFAULT_VALUE
