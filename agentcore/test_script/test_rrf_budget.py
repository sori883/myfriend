"""RRF融合スコア + トークンバジェット検証スクリプト

Usage: uv run python test_script/test_rrf_budget.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from memory.recall import _rrf_fuse, _trim_to_budget, RRF_K, CHARS_PER_TOKEN, MemoryResult


class FakeRecord(dict):
    """asyncpg.Record を模倣する辞書"""
    def __getitem__(self, key):
        return super().__getitem__(key)


def make_record(id_str, text="test", **kwargs):
    return FakeRecord(
        id=id_str,
        text=text,
        context=kwargs.get("context"),
        fact_type=kwargs.get("fact_type", "world"),
        fact_kind=kwargs.get("fact_kind", "event"),
        event_date=kwargs.get("event_date"),
    )


def test_rrf_single_list():
    """単一リストの RRF スコアが 1/(K+rank) になることを確認"""
    records = [make_record(f"id-{i}") for i in range(3)]
    results = _rrf_fuse([records])

    assert len(results) == 3
    for rank, r in enumerate(results):
        expected = 1.0 / (RRF_K + rank)
        assert abs(r.score - expected) < 1e-9, f"rank={rank}: expected {expected}, got {r.score}"

    print("PASS: test_rrf_single_list")


def test_rrf_two_lists_overlap():
    """2リストに同一ドキュメントがある場合、スコアが合算されることを確認"""
    list1 = [make_record("A"), make_record("B"), make_record("C")]
    list2 = [make_record("B"), make_record("A"), make_record("D")]

    results = _rrf_fuse([list1, list2])
    scores = {r.id: r.score for r in results}

    # A: rank0 in list1 + rank1 in list2
    expected_a = 1.0 / (RRF_K + 0) + 1.0 / (RRF_K + 1)
    assert abs(scores["A"] - expected_a) < 1e-9, f"A: expected {expected_a}, got {scores['A']}"

    # B: rank1 in list1 + rank0 in list2
    expected_b = 1.0 / (RRF_K + 1) + 1.0 / (RRF_K + 0)
    assert abs(scores["B"] - expected_b) < 1e-9, f"B: expected {expected_b}, got {scores['B']}"

    # A と B は同スコア（対称）
    assert abs(scores["A"] - scores["B"]) < 1e-9, "A and B should have equal scores"

    # C: rank2 in list1 only
    expected_c = 1.0 / (RRF_K + 2)
    assert abs(scores["C"] - expected_c) < 1e-9

    # D: rank2 in list2 only
    expected_d = 1.0 / (RRF_K + 2)
    assert abs(scores["D"] - expected_d) < 1e-9

    # 両方に出現した A, B がトップにくる
    assert results[0].id in ("A", "B")
    assert results[1].id in ("A", "B")

    print("PASS: test_rrf_two_lists_overlap")


def test_rrf_ordering():
    """RRF スコアの降順でソートされることを確認"""
    list1 = [make_record("X"), make_record("Y"), make_record("Z")]
    list2 = [make_record("Z"), make_record("X")]

    results = _rrf_fuse([list1, list2])
    scores = [r.score for r in results]

    for i in range(len(scores) - 1):
        assert scores[i] >= scores[i + 1], f"Not sorted: {scores}"

    print("PASS: test_rrf_ordering")


def test_trim_max_results():
    """max_results でトリミングされることを確認"""
    results = [
        MemoryResult(id=f"id-{i}", text="x" * 10, context=None,
                     fact_type="world", fact_kind="event", event_date=None, score=1.0 - i * 0.1)
        for i in range(10)
    ]
    trimmed = _trim_to_budget(results, max_tokens=999999, max_results=3)
    assert len(trimmed) == 3, f"Expected 3 results, got {len(trimmed)}"
    assert trimmed[0].id == "id-0"
    print("PASS: test_trim_max_results")


def test_trim_token_budget():
    """トークンバジェットでトリミングされることを確認"""
    # 各結果: text=100文字 → 100/CHARS_PER_TOKEN ≒ 33トークン
    results = [
        MemoryResult(id=f"id-{i}", text="a" * 100, context=None,
                     fact_type="world", fact_kind="event", event_date=None, score=1.0)
        for i in range(10)
    ]
    # max_tokens=100 → max_chars=300 → 100文字のテキストが3件入る
    trimmed = _trim_to_budget(results, max_tokens=100, max_results=50)
    assert len(trimmed) == 3, f"Expected 3 results for 100 token budget, got {len(trimmed)}"
    print("PASS: test_trim_token_budget")


def test_trim_first_result_always_included():
    """最初の1件はバジェット超過でも含まれることを確認"""
    results = [
        MemoryResult(id="big", text="x" * 10000, context=None,
                     fact_type="world", fact_kind="event", event_date=None, score=1.0),
    ]
    trimmed = _trim_to_budget(results, max_tokens=10, max_results=50)
    assert len(trimmed) == 1, "First result should always be included"
    print("PASS: test_trim_first_result_always_included")


def test_budget_low_mid_high():
    """low < mid < high の順にバジェットが大きいことを確認"""
    from memory.recall import BUDGETS
    assert BUDGETS["low"]["max_tokens"] < BUDGETS["mid"]["max_tokens"] < BUDGETS["high"]["max_tokens"]
    assert BUDGETS["low"]["max_results"] < BUDGETS["mid"]["max_results"] < BUDGETS["high"]["max_results"]
    print("PASS: test_budget_low_mid_high")


if __name__ == "__main__":
    test_rrf_single_list()
    test_rrf_two_lists_overlap()
    test_rrf_ordering()
    test_trim_max_results()
    test_trim_token_budget()
    test_trim_first_result_always_included()
    test_budget_low_mid_high()
    print("\n=== All RRF/Budget tests passed ===")
