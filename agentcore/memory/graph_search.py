"""MPFP グラフ検索モジュール

Meta-Path Forward Push アルゴリズムによるグラフベースの記憶検索。

特性:
- 7つのメタパスパターンで異なる検索意図をカバー
- ホップ同期によるエッジ遅延ロード（DB クエリ数 = O(hops)）
- LATERAL JOIN で効率的なエッジ取得
- RRF 融合でパターン結果を統合
- LLM 不要（走査中に LLM を呼ばない）
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import asyncpg

logger = logging.getLogger(__name__)

# ---------- メタパスパターン ----------

PATTERNS_SEMANTIC: tuple[tuple[str, ...], ...] = (
    ("semantic", "semantic"),      # トピック拡張
    ("entity", "temporal"),        # エンティティタイムライン
    ("semantic", "causes"),        # 推論チェーン（順方向）
    ("semantic", "caused_by"),     # 推論チェーン（逆方向）
    ("entity", "semantic"),        # エンティティコンテキスト
)

PATTERNS_TEMPORAL: tuple[tuple[str, ...], ...] = (
    ("temporal", "semantic"),      # その時何が起こっていたか
    ("temporal", "entity"),        # その時誰が関与していたか
)

RRF_K = 60


# ---------- データ構造 ----------


@dataclass(frozen=True)
class SeedNode:
    """エントリポイントノード"""

    node_id: str
    score: float


@dataclass(frozen=True)
class EdgeTarget:
    """隣接ノード"""

    node_id: str
    weight: float


@dataclass(frozen=True)
class MPFPConfig:
    """MPFP アルゴリズム設定"""

    alpha: float = 0.15           # teleport/keep 確率
    threshold: float = 1e-6       # マス pruning 閾値
    top_k_neighbors: int = 20     # ノード毎のファンアウト上限


@dataclass(frozen=True)
class PatternResult:
    """パターン走査の結果"""

    pattern: tuple[str, ...]
    scores: dict[str, float]


@dataclass
class EdgeCache:
    """遅延ロード共有エッジキャッシュ

    ホップ毎にフロンティアノードのエッジをロードし、
    パターン間で共有する。

    NOTE: スレッドセーフではない。_traverse_hop_synchronized 内の
    パターン走査は逐次実行でなければならない。
    """

    _graphs: dict[str, dict[str, list[EdgeTarget]]] = field(
        default_factory=dict
    )
    _fully_loaded: set[str] = field(default_factory=set)
    db_queries: int = 0

    def get_neighbors(self, edge_type: str, node_id: str) -> list[EdgeTarget]:
        """指定エッジタイプの隣接ノードを取得"""
        return self._graphs.get(edge_type, {}).get(node_id, [])

    def get_normalized_neighbors(
        self, edge_type: str, node_id: str, top_k: int
    ) -> list[EdgeTarget]:
        """正規化済み（重み合計=1）の top-k 隣接ノードを取得"""
        neighbors = self.get_neighbors(edge_type, node_id)[:top_k]
        if not neighbors:
            return []
        total = sum(n.weight for n in neighbors)
        if total == 0:
            return []
        return [
            EdgeTarget(node_id=n.node_id, weight=n.weight / total)
            for n in neighbors
        ]

    def is_fully_loaded(self, node_id: str) -> bool:
        return node_id in self._fully_loaded

    def get_uncached(self, node_ids: set[str]) -> list[str]:
        return [n for n in node_ids if not self.is_fully_loaded(n)]

    def add_all_edges(
        self,
        edges_by_type: dict[str, dict[str, list[EdgeTarget]]],
        queried_ids: list[str],
    ) -> None:
        """ロードしたエッジをキャッシュに追加"""
        for edge_type, edges in edges_by_type.items():
            if edge_type not in self._graphs:
                self._graphs[edge_type] = {}
            for node_id, targets in edges.items():
                self._graphs[edge_type][node_id] = targets
        self._fully_loaded.update(queried_ids)


@dataclass
class _PatternState:
    """パターン走査の内部状態（ミュータブル）"""

    pattern: tuple[str, ...]
    hop_index: int
    scores: dict[str, float]
    frontier: dict[str, float]


# ---------- 公開 API ----------


async def graph_search(
    pool: asyncpg.Pool,
    bank_id: str,
    semantic_seeds: list[SeedNode],
    temporal_seeds: list[SeedNode] | None = None,
    config: MPFPConfig | None = None,
    budget: int = 50,
) -> list[tuple[str, float]]:
    """MPFP グラフ検索を実行する

    Args:
        pool: DB 接続プール
        bank_id: メモリバンクID
        semantic_seeds: セマンティック検索のシードノード
        temporal_seeds: 時間検索のシードノード（省略可）
        config: アルゴリズム設定（デフォルト使用可）
        budget: 返却する最大結果数

    Returns:
        (unit_id, score) のリスト（スコア降順）
    """
    if config is None:
        config = MPFPConfig()

    # パターンジョブを構築
    pattern_jobs: list[tuple[list[SeedNode], tuple[str, ...]]] = []

    if semantic_seeds:
        for pattern in PATTERNS_SEMANTIC:
            pattern_jobs.append((semantic_seeds, pattern))

    if temporal_seeds:
        for pattern in PATTERNS_TEMPORAL:
            pattern_jobs.append((temporal_seeds, pattern))

    if not pattern_jobs:
        return []

    # ホップ同期走査
    cache = EdgeCache()

    # シードノードのエッジをプリウォーム
    all_seed_ids = list({s.node_id for seeds, _ in pattern_jobs for s in seeds})
    if all_seed_ids:
        edges_by_type = await _load_edges_for_frontier(
            pool, bank_id, all_seed_ids, config.top_k_neighbors
        )
        cache.db_queries += 1
        cache.add_all_edges(edges_by_type, all_seed_ids)

    # 全パターンをホップ同期で走査
    pattern_results = await _traverse_hop_synchronized(
        pool, bank_id, pattern_jobs, config, cache
    )

    # RRF 融合
    return _rrf_fusion(pattern_results, k=RRF_K, top_k=budget)


# ---------- エッジ遅延ロード ----------


async def _load_edges_for_frontier(
    pool: asyncpg.Pool,
    bank_id: str,
    node_ids: list[str],
    top_k_per_type: int = 20,
) -> dict[str, dict[str, list[EdgeTarget]]]:
    """LATERAL JOIN でフロンティアノードのエッジをロードする

    1クエリで全ノード × 全エッジタイプの top-k エッジを取得。
    """
    if not node_ids:
        return {}

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH frontier(node_id) AS (SELECT unnest($1::uuid[]))
            SELECT f.node_id AS from_unit_id,
                   lt.link_type,
                   edges.to_unit_id,
                   edges.weight
            FROM frontier f
            CROSS JOIN (VALUES
                ('semantic'), ('temporal'), ('entity'), ('causes'), ('caused_by')
            ) AS lt(link_type)
            CROSS JOIN LATERAL (
                SELECT ml.to_unit_id, ml.weight
                FROM memory_links ml
                WHERE ml.from_unit_id = f.node_id
                  AND ml.link_type = lt.link_type
                  AND ml.bank_id = $3::uuid
                  AND ml.weight >= 0.1
                ORDER BY ml.weight DESC
                LIMIT $2
            ) edges
            """,
            node_ids,
            top_k_per_type,
            bank_id,
        )

    result: dict[str, dict[str, list[EdgeTarget]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        edge_type = row["link_type"]
        from_id = str(row["from_unit_id"])
        to_id = str(row["to_unit_id"])
        result[edge_type][from_id].append(
            EdgeTarget(node_id=to_id, weight=row["weight"])
        )

    return {et: dict(edges) for et, edges in result.items()}


# ---------- コアアルゴリズム ----------


def _init_pattern_state(
    seeds: list[SeedNode], pattern: tuple[str, ...]
) -> _PatternState:
    """シードからパターン状態を初期化する（スコアを正規化）"""
    if not seeds:
        return _PatternState(pattern=pattern, hop_index=0, scores={}, frontier={})

    total = sum(s.score for s in seeds)
    if total == 0:
        total = float(len(seeds))

    frontier = {s.node_id: s.score / total for s in seeds}
    return _PatternState(pattern=pattern, hop_index=0, scores={}, frontier=frontier)


def _execute_hop(
    state: _PatternState, cache: EdgeCache, config: MPFPConfig
) -> set[str]:
    """1ホップ実行: α×mass を保持、(1-α)×mass を隣接ノードに伝搬

    Returns:
        次ホップで必要な未キャッシュノード ID の集合
    """
    if state.hop_index >= len(state.pattern):
        return set()

    edge_type = state.pattern[state.hop_index]
    next_frontier: dict[str, float] = {}
    uncached_for_next: set[str] = set()

    for node_id, mass in state.frontier.items():
        if mass < config.threshold:
            continue

        # α 分を保持
        state.scores[node_id] = state.scores.get(node_id, 0) + config.alpha * mass

        # (1-α) 分を隣接ノードに伝搬
        push_mass = (1 - config.alpha) * mass
        neighbors = cache.get_normalized_neighbors(
            edge_type, node_id, config.top_k_neighbors
        )

        for neighbor in neighbors:
            nid = neighbor.node_id
            next_frontier[nid] = (
                next_frontier.get(nid, 0) + push_mass * neighbor.weight
            )
            if not cache.is_fully_loaded(nid):
                uncached_for_next.add(nid)

    state.frontier = next_frontier
    state.hop_index += 1

    return uncached_for_next


def _finalize_pattern(
    state: _PatternState, config: MPFPConfig
) -> PatternResult:
    """残余フロンティアマスを scores に加算して最終化"""
    for node_id, mass in state.frontier.items():
        if mass >= config.threshold:
            state.scores[node_id] = state.scores.get(node_id, 0) + mass

    return PatternResult(pattern=state.pattern, scores=dict(state.scores))


async def _traverse_hop_synchronized(
    pool: asyncpg.Pool,
    bank_id: str,
    pattern_jobs: list[tuple[list[SeedNode], tuple[str, ...]]],
    config: MPFPConfig,
    cache: EdgeCache,
) -> list[PatternResult]:
    """全パターンをホップ同期で走査する

    各ホップで:
    1. 全パターンのホップを実行（キャッシュ済みエッジ使用）
    2. 全パターンの未キャッシュフロンティアノードを収集
    3. 1クエリで全未キャッシュノードのエッジをロード
    """
    states = [
        _init_pattern_state(seeds, pattern)
        for seeds, pattern in pattern_jobs
    ]

    max_hops = max((len(p) for _, p in pattern_jobs), default=0)

    for _hop in range(max_hops):
        all_uncached: set[str] = set()

        for state in states:
            if state.hop_index < len(state.pattern):
                uncached = _execute_hop(state, cache, config)
                all_uncached.update(uncached)

        # 未キャッシュノードのエッジをプリウォーム
        uncached_list = cache.get_uncached(all_uncached)
        if uncached_list:
            edges_by_type = await _load_edges_for_frontier(
                pool, bank_id, uncached_list, config.top_k_neighbors
            )
            cache.db_queries += 1
            cache.add_all_edges(edges_by_type, uncached_list)

    return [_finalize_pattern(state, config) for state in states]


# ---------- RRF 融合 ----------


def _rrf_fusion(
    results: list[PatternResult],
    k: int = 60,
    top_k: int = 50,
) -> list[tuple[str, float]]:
    """Reciprocal Rank Fusion で全パターン結果を融合する

    score(d) = Σ_patterns 1/(k + rank_in_pattern)
    """
    fused: dict[str, float] = {}

    for result in results:
        if not result.scores:
            continue
        ranked = sorted(
            result.scores.keys(),
            key=lambda n: result.scores[n],
            reverse=True,
        )
        for rank, node_id in enumerate(ranked):
            fused[node_id] = fused.get(node_id, 0) + 1.0 / (k + rank + 1)

    sorted_results = sorted(fused.items(), key=lambda x: x[1], reverse=True)
    return sorted_results[:top_k]
