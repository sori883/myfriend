"""タグベース可視性制御

4つのマッチモードでタグフィルタリングを提供する:
- any:        OR マッチ、未タグも含む（デフォルト）
- all:        AND マッチ、未タグも含む
- any_strict: OR マッチ、未タグを除外
- all_strict: AND マッチ、未タグを除外（セキュリティ重視）
"""

import logging

logger = logging.getLogger(__name__)

VALID_MATCH_MODES = frozenset({"any", "all", "any_strict", "all_strict"})
ALLOWED_COLUMNS = frozenset({"tags"})


def build_tags_where_clause(
    tags: list[str],
    param_offset: int = 1,
    match_mode: str = "any",
    column: str = "tags",
) -> tuple[str, list, int]:
    """タグフィルタ用の SQL WHERE 句を構築する

    Args:
        tags: フィルタ対象のタグリスト
        param_offset: パラメータプレースホルダの開始番号（$N）
        match_mode: マッチモード（any / all / any_strict / all_strict）
        column: タグカラム名

    Returns:
        (sql_clause, params, next_param_offset)
        sql_clause は WHERE の中身のみ（WHERE キーワードを含まない）
    """
    if column not in ALLOWED_COLUMNS:
        raise ValueError(f"Invalid column name: {column}")

    if match_mode not in VALID_MATCH_MODES:
        logger.warning("Invalid match_mode '%s', falling back to 'any'", match_mode)
        match_mode = "any"

    param_placeholder = f"${param_offset}"
    params = [tags]
    next_offset = param_offset + 1

    is_strict = match_mode.endswith("_strict")
    base_mode = match_mode.replace("_strict", "")

    if base_mode == "any":
        # OR: いずれかのタグが一致
        operator = "&&"
    else:
        # AND: 全タグが一致
        operator = "@>"

    tag_condition = f"{column} {operator} {param_placeholder}::text[]"

    if is_strict:
        # strict: 未タグを除外
        clause = (
            f"{column} IS NOT NULL AND {column} != '{{}}' "
            f"AND {tag_condition}"
        )
    else:
        # non-strict: 未タグも含む
        clause = (
            f"({column} IS NULL OR {column} = '{{}}' "
            f"OR {tag_condition})"
        )

    return clause, params, next_offset


def filter_results_by_tags(
    results: list[dict],
    tags: list[str],
    match_mode: str = "any",
    tag_key: str = "tags",
) -> list[dict]:
    """Python 側でタグフィルタリングする

    SQL を使えないケース（グラフトラバーサル結果など）で使用。

    Args:
        results: フィルタ対象のリスト
        tags: フィルタ対象のタグリスト
        match_mode: マッチモード
        tag_key: 各 dict 内のタグキー名

    Returns:
        フィルタ後のリスト
    """
    if not tags:
        return results

    if match_mode not in VALID_MATCH_MODES:
        match_mode = "any"

    is_strict = match_mode.endswith("_strict")
    base_mode = match_mode.replace("_strict", "")
    tags_set = set(tags)

    filtered = []
    for item in results:
        item_tags = item.get(tag_key) or []

        if not item_tags:
            # 未タグの場合
            if not is_strict:
                filtered.append(item)
            continue

        item_tags_set = set(item_tags)

        if base_mode == "any":
            if item_tags_set & tags_set:
                filtered.append(item)
        else:
            if tags_set.issubset(item_tags_set):
                filtered.append(item)

    return filtered
