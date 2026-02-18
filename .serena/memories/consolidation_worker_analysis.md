# Consolidation Worker (2.2.1) 実装情報

## 概要

中期記憶 Consolidation Worker の実装に必要な全情報。Raw Facts を Observations に自動統合する中核ロジック。

---

## 1. タスク仕様（設計書より）

### 1.1 目的と入出力

**入力**: 未統合 Raw Facts（`memory_units` where `consolidated_at IS NULL`）

**処理フロー**:
1. 未統合 Fact 取得: `LIMIT 10` バッチ単位
2. 各 Fact の関連 Observation 検索
3. LLM 判定（Bedrock Converse, Claude Haiku 4.5）
4. 判定結果に応じた処理:
   - `REDUNDANT`: 同情報 → `proof_count` 更新のみ
   - `CONTRADICTION`: 矛盾 → 時間マーカー付きで更新
   - `UPDATE`: 新状態 → 既存 Observation 更新
   - `NEW`: 新知識 → Observation 新規作成
5. `consolidated_at` 更新（トランザクション確定）

**出力**: `consolidated_at` が設定された Fact 数、作成/更新された Observation 数

### 1.2 重要な制約

- 異なる人物の事実をマージしない
- 無関係なトピックをマージしない
- 矛盾は両方の状態を時間マーカーで示す（`used to X, now Y` パターン）
- 1つの Observation は1人の特定トピックに焦点

### 1.3 スケジューラー連携

- `agentcore/memory/scheduler.py` の `ConsolidationScheduler` が 5分間隔で `_execute_consolidation()` を呼び出す
- 現在は stub（未統合数をカウントするだけ）→ Phase 2.2.1 で実装する
- 手動トリガー CLI も対応予定：`python -m memory.scheduler --interval 60`

---

## 2. テーブル構造

### 2.1 短期記憶テーブル（既に存在）

**`memory_units`** - 全記憶の統合テーブル
```sql
id UUID PRIMARY KEY
bank_id UUID (FK: banks)
text TEXT -- ファクトテキスト
fact_type TEXT -- 'world'|'experience'|'observation'
fact_kind TEXT -- 'event'|'conversation'
who TEXT[] -- 関係する人物
what TEXT -- 何が起きたか
when_description TEXT -- いつ起きたか
where_description TEXT -- どこで起きたか
why_description TEXT -- なぜ重要か

embedding vector(1024) -- Titan Embed V2

-- Observation-specific
proof_count INTEGER DEFAULT 0 -- 証拠数
source_memory_ids UUID[] -- 根拠となった memory_unit IDs
history JSONB DEFAULT '[]' -- 変更履歴
confidence_score FLOAT DEFAULT 1.0

consolidated_at TIMESTAMPTZ -- NULL なら未統合
tags TEXT[]
created_at TIMESTAMPTZ
updated_at TIMESTAMPTZ
```

**`entities`** - 正規化エンティティマスタ
```sql
id UUID PRIMARY KEY
bank_id UUID (FK: banks)
canonical_name TEXT
entity_type TEXT -- 'person'|'organization'|'location'|'concept'|'event'|'other'
mention_count INTEGER
first_seen TIMESTAMPTZ
last_seen TIMESTAMPTZ
```

**`banks`** - メモリバンク
```sql
id UUID PRIMARY KEY
name TEXT
mission TEXT
background TEXT
disposition JSONB -- {"skepticism": 3, "literalism": 3, "empathy": 3}
directives TEXT[]
metadata JSONB
```

### 2.2 中期記憶テーブル（既に存在）

**`memory_links`** - グラフエッジ
```sql
id UUID PRIMARY KEY
bank_id UUID (FK: banks)
from_unit_id UUID (FK: memory_units)
to_unit_id UUID (FK: memory_units)
link_type TEXT -- 'temporal'|'semantic'|'entity'|'causes'|'caused_by'
weight FLOAT DEFAULT 1.0
entity_id UUID (nullable) -- link の根拠となるエンティティ
created_at TIMESTAMPTZ
UNIQUE(from_unit_id, to_unit_id, link_type, entity_id)
```

**`entity_cooccurrences`** - 共起頻度キャッシュ
```sql
entity_id_1 UUID (FK: entities)
entity_id_2 UUID (FK: entities)
bank_id UUID
cooccurrence_count INTEGER DEFAULT 1
last_cooccurred TIMESTAMPTZ
CHECK (entity_id_1 < entity_id_2)
```

**`async_operations`** - バックグラウンドジョブ追跡
```sql
id UUID PRIMARY KEY
bank_id UUID
operation_type TEXT
status TEXT -- 'pending'|'processing'|'completed'|'failed'
worker_id TEXT
payload JSONB
result JSONB
error_message TEXT
created_at TIMESTAMPTZ
started_at TIMESTAMPTZ
completed_at TIMESTAMPTZ
```

### 2.3 インデックス

**Consolidation 関連**:
- `idx_memory_units_unconsolidated` (memory_units where consolidated_at IS NULL)
- `idx_memory_units_embedding_observation` (fact_type='observation' 用 HNSW)
- `idx_memory_links_traversal`, `idx_memory_links_weighted` (グラフ検索)

---

## 3. 既存コード構造

### 3.1 MemoryEngine（core）

**ファイル**: `agentcore/memory/engine.py`

```python
class MemoryEngine:
    async def initialize() -> None
        # DB プール初期化 + ConsolidationScheduler 起動
    
    async def close() -> None
        # リソース解放
    
    async def retain(...) -> dict
        # Retain パイプライン実行
    
    async def recall(...) -> dict
        # Recall パイプライン実行
    
    async def trigger_consolidation() -> dict
        # 手動トリガー（開発用）
```

**キー**: `_pool: asyncpg.Pool` と `_scheduler: ConsolidationScheduler` を管理

### 3.2 ConsolidationScheduler

**ファイル**: `agentcore/memory/scheduler.py`

```python
class ConsolidationScheduler:
    def __init__(pool: asyncpg.Pool, interval_seconds: int = 300)
        # interval_seconds デフォルト 300秒（5分）
    
    async def start() -> None
        # バックグラウンドタスク開始
    
    async def trigger() -> dict
        # 手動トリガー（開発・デバッグ用）
    
    async def _execute_consolidation() -> dict
        # 【現在は stub】未統合 Fact 数をカウント
        # Phase 2.2.1 で実装する
```

**現状の stub**:
```python
async def _execute_consolidation(self) -> dict:
    unconsolidated_count = await conn.fetchval(
        """SELECT COUNT(*) FROM memory_units
           WHERE consolidated_at IS NULL
           AND fact_type IN ('world', 'experience')"""
    )
    return {
        "unconsolidated_count": unconsolidated_count,
        "processed": 0,  # ← 常に 0（stub）
        "elapsed_ms": ...
    }
```

### 3.3 Retain パイプライン（参考）

**ファイル**: `agentcore/memory/retain.py`

- ファクト抽出（Bedrock Converse）
- Embedding 生成（Titan Embed V2）
- 重複チェック（12時間バケット + コサイン類似度 >= 0.9）
- DB トランザクション内でエンティティ解決 + INSERT
- **Consolidation ジョブキュー**: 現在はスタブ（Phase 2 で実装）

```python
async def retain(pool, bank_id, content, context="") -> dict:
    # 1. ファクト抽出
    facts = await extract_facts(content, context)
    
    # 2. Embedding 生成
    embeddings = await generate_embeddings([...])
    
    # 3-4. 重複チェック + DB 保存
    async with pool.acquire() as conn:
        async with conn.transaction():
            for fact, embedding in zip(facts, embeddings):
                # 重複チェック
                is_dup = await _check_duplicate(conn, bank_id, embedding, fact)
                if is_dup: continue
                
                # 保存
                unit_id = await _insert_memory_unit(...)
                
                # エンティティ解決
                entities = await resolve_entities(conn, bank_id, fact.who)
    
    # 5. Consolidation ジョブキュー（stub）
    if stored_ids:
        logger.info("Consolidation job queued... (stub - Phase 2)")
```

### 3.4 Recall パイプライン（参考）

**ファイル**: `agentcore/memory/recall.py`

- 2方向検索（セマンティック + BM25）
- RRF 融合
- トークンバジェット管理

**Phase 2** で拡張: 4方向検索（グラフ + 時間）+ クロスエンコーダリランキング

### 3.5 LLM 関連モジュール

**Bedrock クライアント**: `agentcore/memory/bedrock_client.py`

**ファクト抽出**: `agentcore/memory/extraction.py`
- Bedrock Converse API（Claude Haiku 4.5）
- `Fact` dataclass: text, what, who, when_description, where_description, why_description, event_date, occurred_start, occurred_end, fact_kind, fact_type

**Embedding 生成**: `agentcore/memory/embedding.py`
- Bedrock Titan Embed V2（1024 次元）
- 並列数制限: `_EMBEDDING_CONCURRENCY = 5`

**エンティティ解決**: `agentcore/memory/entity.py`
- pg_trgm `similarity()` で既存エンティティ検索（閾値 0.6）
- マッチしなければ新規作成
- `ResolvedEntity`: entity_id, canonical_name, entity_type, is_new

### 3.6 DB 接続

**ファイル**: `agentcore/memory/db.py`

```python
async def get_pool() -> asyncpg.Pool
    # DATABASE_URL から接続プール作成
    # pgvector 型登録

async def close_pool() -> None
    # プール閉じる
```

---

## 4. Hindsight 実装参考（hindsight-api/consolidation）

### 4.1 consolidator.py の全体フロー

```python
async def run_consolidation_job(memory_engine, bank_id, request_context):
    """Main entry point for consolidation"""
    
    # 1. Bank プロファイル取得
    bank_row = await conn.fetchrow(
        "SELECT bank_id, name, mission FROM banks WHERE bank_id = $1",
        bank_id
    )
    mission = bank_row["mission"] or "General consolidation"
    
    # 2. 未統合数カウント
    total_count = await conn.fetchval(
        """SELECT COUNT(*) FROM memory_units
           WHERE bank_id = $1 AND consolidated_at IS NULL
           AND fact_type IN ('experience', 'world')"""
    )
    
    # 3. バッチ処理（LIMIT 10 ずつ）
    while True:
        memories = await conn.fetch(
            """SELECT id, text, fact_type, tags, event_date, ...
               FROM memory_units
               WHERE bank_id = $1 AND consolidated_at IS NULL
               ORDER BY created_at ASC
               LIMIT $2""",
            bank_id, max_memories_per_batch
        )
        
        if not memories: break
        
        # 4. 各 memory を処理
        for memory in memories:
            result = await _process_memory(
                conn, memory_engine, bank_id, memory, mission, request_context
            )
            
            # Mark consolidated
            await conn.execute(
                "UPDATE memory_units SET consolidated_at = NOW() WHERE id = $1",
                memory["id"]
            )
            
            # 統計更新
            if result["action"] == "created":
                stats["observations_created"] += 1
            elif result["action"] == "updated":
                stats["observations_updated"] += 1
            # ...
    
    # 5. Mental Model リフレッシュトリガー（Phase 3）
    mental_models_refreshed = await _trigger_mental_model_refreshes(...)
    
    return {
        "status": "completed",
        "bank_id": bank_id,
        "memories_processed": ...,
        "observations_created": ...,
        ...
    }
```

### 4.2 _process_memory（未実装のコア）

```python
async def _process_memory(
    conn: Connection,
    memory_engine: MemoryEngine,
    bank_id: str,
    memory: dict,  # id, text, fact_type, who, event_date, ...
    mission: str,
    request_context: RequestContext,
    perf: ConsolidationPerfLog
) -> dict:
    """
    1 つの memory を処理して Observation を作成・更新する
    
    Returns:
        {"action": "created"|"updated"|"merged"|"skipped", ...}
    """
    # 1. 関連 Observation を検索（Recall パイプライン）
    existing_observations = await recall_for_consolidation(
        memory_engine,
        bank_id,
        memory["text"],
        memory.get("who"),
        max_results=50
    )
    
    # 2. LLM 判定
    # CONSOLIDATION_SYSTEM_PROMPT + CONSOLIDATION_USER_PROMPT を使用
    # Bedrock Converse API で LLM 呼び出し
    
    llm_response = await bedrock.converse(
        systemPrompt=CONSOLIDATION_SYSTEM_PROMPT,
        userMessage=f"""
Analyze this new fact and consolidate into knowledge.
Mission: {mission}

NEW FACT: {memory['text']}

EXISTING OBSERVATIONS:
{serialize_observations(existing_observations)}

Instructions:
1. Extract DURABLE KNOWLEDGE from the new fact
2. Compare with existing observations
3. Output JSON array of actions
[
  {{"action": "update", "learning_id": "uuid", "text": "...", "reason": "..."}},
  {{"action": "create", "text": "...", "reason": "..."}}
]
        """
    )
    
    # 3. JSON パース
    actions = json.loads(llm_response)
    
    # 4. 各 action 実行
    total_actions = 0
    for action in actions:
        if action["action"] == "create":
            # 新規 Observation 作成
            obs_id = await create_observation(
                conn, bank_id,
                text=action["text"],
                source_memory_ids=[memory["id"]],
                tags=memory.get("tags"),
                fact_type="observation"
            )
            total_actions += 1
        
        elif action["action"] == "update":
            # 既存 Observation 更新
            learning_id = action["learning_id"]
            await update_observation(
                conn, learning_id,
                text=action["text"],
                source_memory_ids=[memory["id"]],  # 追記
                proof_count_increment=1
            )
            total_actions += 1
    
    return {
        "action": "multiple" if total_actions > 1 else ("created" if total_actions == 1 else "skipped"),
        "total_actions": total_actions,
        "created": sum(1 for a in actions if a["action"] == "create"),
        "updated": sum(1 for a in actions if a["action"] == "update"),
    }
```

### 4.3 LLM プロンプト構造

**CONSOLIDATION_SYSTEM_PROMPT**:
- ファクトから "durable knowledge" を抽出すること（ephemeral state ではなく）
- 異なる人物・トピックをマージしない
- 矛盾は両方の状態を時間マーカーで示す（`used to X, now Y`）
- 出力は JSON 配列のみ

**CONSOLIDATION_USER_PROMPT**:
```
Mission: {mission}

NEW FACT: {memory_text}

EXISTING OBSERVATIONS:
[
  {
    "id": "uuid",
    "text": "Observation content",
    "proof_count": 3,
    "tags": [...],
    "created_at": "2024-06-15T...",
    "updated_at": "2024-06-20T...",
    "source_memories": [
      {"id": "uuid", "text": "Supporting fact", "date": "2024-06-15T..."},
      ...
    ]
  }
]

Instructions:
1. Extract DURABLE KNOWLEDGE from the new fact
2. Review source_memories to understand evidence
3. Compare with observations:
   - Same topic → UPDATE with learning_id
   - New topic → CREATE
   - Ephemeral → return []

Output:
[
  {"action": "update", "learning_id": "uuid", "text": "...", "reason": "..."},
  {"action": "create", "text": "...", "reason": "..."}
]
```

### 4.4 パフォーマンス測定

Hindsight では `ConsolidationPerfLog` で:
- `recall` 時間
- `llm` 呼び出し時間
- `embedding` 生成時間
- `db_write` 実行時間

をトラッキング

---

## 5. 実装要件サマリ

### 5.1 新規ファイル

**`agentcore/memory/consolidation.py`**
- `async def consolidate(pool: asyncpg.Pool, bank_id: str) -> dict`
  - 未統合 Fact を取得（LIMIT 10）
  - `_process_memory()` で処理
  - `consolidated_at` 更新
  - 統計情報を返す

### 5.2 既存ファイルの修正

**`agentcore/memory/scheduler.py`**
- `_execute_consolidation()` の stub を → 実装版に変更
- `consolidation.py` の `consolidate()` を呼び出す

**`agentcore/memory/engine.py`**
- 特に修正なし（既に scheduler 連携済み）

### 5.3 依存モジュール

- `memory.extraction`: Fact dataclass, `extract_facts()`
- `memory.embedding`: `generate_embedding()`, `generate_embeddings()`
- `memory.entity`: `resolve_entities()`, `ResolvedEntity`
- `memory.recall`: 既存 Observation 検索用（Phase 2.4.1 で拡張）
- `memory.bedrock_client`: LLM 呼び出し
- `asyncpg`: DB 接続

### 5.4 LLM インテグレーション

**Bedrock Converse API** で:
- Model: Claude Haiku 4.5（`anthropic.claude-3-haiku-20240307-v1:0`）
- Temperature: 0.0（決定的）
- Max tokens: 2048

### 5.5 DB トランザクション

Hindsight の実装:
- 各 memory の処理後に即座に `consolidated_at = NOW()` を UPDATE
- **個別 commit** で crash recovery に対応

myproject 案:
- 同様に個別 commit か、バッチトランザクションか要設計

---

## 6. 検証チェックリスト（2.5 より）

- [ ] 未統合 Fact が Observation に統合される
- [ ] REDUNDANT/CONTRADICTION/UPDATE/NEW の4パターンが正しく処理される
- [ ] `consolidated_at` が正しく更新される
- [ ] ローカルスケジューラーが 5分間隔で実行される
- [ ] CLI から手動でトリガーできる
- [ ] 異なる人物の事実がマージされない
- [ ] 矛盾が時間マーカーで記録される
- [ ] proof_count が正しく増分される

---

## 7. ファイルパスまとめ

### 設計書
- `/Users/const/sori883/myfriend/docs/設計/記憶システム実装タスク.md` (L227-245)

### 既存コード
- `/Users/const/sori883/myfriend/agentcore/memory/engine.py`
- `/Users/const/sori883/myfriend/agentcore/memory/scheduler.py`
- `/Users/const/sori883/myfriend/agentcore/memory/retain.py`
- `/Users/const/sori883/myfriend/agentcore/memory/recall.py`
- `/Users/const/sori883/myfriend/agentcore/memory/extraction.py`
- `/Users/const/sori883/myfriend/agentcore/memory/embedding.py`
- `/Users/const/sori883/myfriend/agentcore/memory/entity.py`
- `/Users/const/sori883/myfriend/agentcore/memory/db.py`

### DB スキーマ
- `/Users/const/sori883/myfriend/postgresql/init/002_short_term.sql` (memory_units, entities, etc.)
- `/Users/const/sori883/myfriend/postgresql/init/003_mid_term.sql` (memory_links, async_operations, etc.)

### Hindsight 参考実装
- `/Users/const/sori883/myfriend/docs/hindsight実装参考/hindsight-api/hindsight_api/engine/consolidation/consolidator.py`
- `/Users/const/sori883/myfriend/docs/hindsight実装参考/hindsight-api/hindsight_api/engine/consolidation/prompts.py`
