# 記憶システム

Hindsight 論文に基づく3段階記憶モデル。会話から抽出した事実を短期・中期・長期の3階層で管理し、段階的に抽象化・統合していく。

https://arxiv.org/pdf/2512.12818

## アーキテクチャ概要

```mermaid
graph TB
    subgraph Input
        A[会話テキスト]
    end

    subgraph "短期記憶 (Raw Facts)"
        B[Fact 抽出<br/>LLM: Claude Haiku]
        C[Embedding 生成<br/>Titan Embed V2]
        D[重複チェック]
        E[memory_units<br/>fact_type: world / experience]
        F[エンティティ解決<br/>pg_trgm similarity]
        G[グラフリンク構築]
    end

    subgraph "中期記憶 (Observations)"
        H[Consolidation Worker<br/>5分間隔]
        I[LLM 判定<br/>CREATE / UPDATE]
        J[memory_units<br/>fact_type: observation]
        K[鮮度追跡<br/>Freshness Status]
    end

    subgraph "長期記憶 (Mental Models)"
        L[自動生成トリガー<br/>Observation >= 5]
        M[Reflect パイプライン<br/>エージェントループ]
        N[mental_models テーブル]
    end

    A --> B --> C --> D --> E
    E --> F --> G
    E --> H --> I --> J
    J --> K
    J --> L --> M --> N

    style E fill:#4a9eff,color:#fff
    style J fill:#ff9f43,color:#fff
    style N fill:#ee5a24,color:#fff
```

## 3段階の記憶階層

| 階層 | 名称 | fact_type | 内容 | 生成タイミング |
|------|------|-----------|------|----------------|
| 短期 | Raw Facts | `world` / `experience` | 会話から抽出した5W1H事実 | Retain 実行時（即座） |
| 中期 | Observations | `observation` | Raw Facts のパターン統合・矛盾解消 | Consolidation Worker（5分間隔） |
| 長期 | Mental Models | - | キュレーション済みサマリ | Consolidation 後の自動生成 + Reflect |

## データフロー全体像

```mermaid
sequenceDiagram
    participant User as ユーザー
    participant Agent as Strands Agent
    participant Retain as Retain パイプライン
    participant DB as PostgreSQL + pgvector
    participant Scheduler as Consolidation Scheduler
    participant Consolidation as Consolidation Worker
    participant Reflect as Reflect パイプライン

    User->>Agent: 会話
    Agent->>Retain: remember(content)
    Retain->>Retain: 1. LLM Fact 抽出
    Retain->>Retain: 2. Embedding 生成
    Retain->>Retain: 3. 重複チェック
    Retain->>DB: 4. INSERT memory_units (Raw Facts)
    Retain->>DB: 5. エンティティ解決 + グラフリンク

    Note over Scheduler: 5分間隔で実行
    Scheduler->>Consolidation: 未統合 Facts 処理
    Consolidation->>DB: 関連 Observation 検索
    Consolidation->>Consolidation: LLM 判定 (CREATE/UPDATE)
    Consolidation->>DB: Observation 作成/更新
    Consolidation->>DB: 鮮度ステータス更新
    Consolidation->>DB: Mental Model リフレッシュ/自動生成

    User->>Agent: 質問
    Agent->>DB: recall_memories(query)
    Note over DB: 4方向並列検索<br/>セマンティック+BM25+グラフ+時間
    DB-->>Agent: 検索結果

    Agent->>Reflect: reflect_on(topic)
    Reflect->>DB: Mental Models 検索
    Reflect->>DB: Observations 検索
    Reflect->>DB: Raw Facts 検索
    Reflect-->>Agent: 証拠に基づく推論結果
```

## テクノロジースタック

| コンポーネント | 技術 |
|----------------|------|
| AI フレームワーク | Strands Agent (Python) |
| LLM (Fact 抽出/Consolidation) | AWS Bedrock - Claude Haiku 4.5 |
| LLM (Reflect) | AWS Bedrock - Claude Haiku（環境変数で変更可能） |
| Embedding | AWS Bedrock - Titan Embed V2 (1024次元) |
| リランキング | AWS Bedrock - Rerank API |
| データベース | PostgreSQL + pgvector + pg_trgm |
| 非同期処理 | asyncio + asyncpg |

## 主要モジュール構成

```
memory/src/memory/
  engine.py          # MemoryEngine: 公開 API (retain/recall/reflect)
  retain.py          # Retain パイプライン
  recall.py          # Recall パイプライン (4方向検索)
  reflect.py         # Reflect パイプライン (エージェントループ)
  extraction.py      # LLM Fact 抽出
  embedding.py       # Embedding 生成 (Titan Embed V2)
  entity.py          # エンティティ解決 (3要素スコアリング)
  graph.py           # グラフリンク構築
  graph_search.py    # MPFP グラフ検索
  temporal_search.py # 時間範囲検索
  reranker.py        # クロスエンコーダリランキング
  freshness.py       # 鮮度追跡
  mental_model.py    # Mental Model CRUD
  visibility.py      # タグベース可視性制御
  disposition.py     # 性格特性プロンプト
  directive.py       # ディレクティブ管理
  bedrock_client.py  # Bedrock クライアント共有
  db.py              # DB 接続プール管理
```

## DB スキーマ概要

```mermaid
erDiagram
    banks ||--o{ memory_units : "has"
    banks ||--o{ entities : "has"
    banks ||--o{ mental_models : "has"
    banks ||--o{ memory_links : "has"
    banks ||--o{ documents : "has"
    banks ||--o{ async_operations : "has"

    documents ||--o{ memory_units : "source"
    memory_units ||--o{ unit_entities : "linked"
    entities ||--o{ unit_entities : "linked"
    memory_units ||--o{ memory_links : "from/to"
    entities ||--o{ memory_links : "referenced"
    memory_units ||--o{ chunks : "has"
    entities ||--o{ mental_models : "referenced"

    banks {
        uuid id PK
        text name
        text mission
        text background
        jsonb disposition
        text[] directives
        jsonb metadata
    }

    documents {
        uuid id PK
        uuid bank_id FK
        text external_id
        text content_hash
        jsonb metadata
    }

    memory_units {
        uuid id PK
        uuid bank_id FK
        uuid document_id FK
        text text
        vector embedding "1024dim"
        text fact_type "world|experience|observation"
        text[] who
        text what
        text when_description
        int proof_count "Observation用"
        uuid[] source_memory_ids "Observation用"
        text freshness_status "鮮度"
        timestamptz consolidated_at
        jsonb metadata
    }

    entities {
        uuid id PK
        uuid bank_id FK
        text canonical_name
        text entity_type "NOT NULL + CHECK"
        jsonb metadata
        timestamptz first_seen
        timestamptz last_seen
        int mention_count
    }

    memory_links {
        uuid id PK
        uuid from_unit_id FK
        uuid to_unit_id FK
        text link_type "temporal|semantic|entity|..."
        uuid entity_id FK
        float weight
    }

    mental_models {
        uuid id PK
        uuid bank_id FK
        text name
        text content
        vector embedding "1024dim"
        uuid entity_id FK
        uuid[] source_observation_ids
        jsonb trigger
    }

    chunks {
        uuid id PK
        uuid memory_unit_id FK
        text text
        vector embedding "1024dim"
    }

    unit_entities {
        uuid unit_id FK
        uuid entity_id FK
    }

    async_operations {
        uuid id PK
        uuid bank_id FK
        text operation_type
        text status "pending|processing|completed|failed"
        jsonb payload
        jsonb result
    }
```

## 関連ドキュメント

- [短期記憶 (Raw Facts)](./短期記憶.md)
- [中期記憶 (Observations)](./中期記憶.md)
- [長期記憶 (Mental Models)](./長期記憶.md)
