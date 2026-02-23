# AgentCore 設計

## 構成

```mermaid
graph TB
    subgraph "API Gateway"
        APIGW["REST API<br/>+ API Key 認証<br/>+ Usage Plan"]
    end

    subgraph "Proxy Lambda"
        PL["myfriend-agentcore-proxy<br/>Node.js 24 / 512MB<br/>Timeout: 15min"]
    end

    subgraph "Bedrock AgentCore"
        AC["myfriend_agent<br/>Runtime (Docker)<br/>VPC モード"]
    end

    subgraph "VPC - Isolated"
        AURORA["Aurora<br/>PostgreSQL 16.4"]
    end

    subgraph "Bedrock"
        LLM["Claude<br/>(Converse API)"]
        EMB["Titan Embed V2<br/>(1024次元)"]
    end

    SM["Secrets Manager"]

    APIGW -->|"Lambda Response<br/>Streaming"| PL
    PL -->|"InvokeAgentRuntime"| AC
    AC -->|"TCP 5432"| AURORA
    AC -->|"Converse / Embed"| LLM
    AC -->|"Embed"| EMB
    AC -->|"GetSecretValue"| SM
```

## AgentCore Runtime

| 項目 | 値 |
|---|---|
| Runtime 名 | `myfriend_agent` |
| アーティファクト | `agentcore/Dockerfile` からビルド |
| ネットワーク | VPC モード（Isolated サブネット） |
| 環境変数 | `AWS_REGION`, `DB_SECRET_ARN`, `DB_HOST`, `DB_NAME` |

### IAM 権限

| 権限 | リソース | 用途 |
|---|---|---|
| `bedrock:*` | `*` | Converse API, Embedding, Rerank |
| Secrets Manager Read | DB シークレット | DB 認証情報取得 |

### SG 接続

AgentCore Runtime の SG → Aurora SG へ TCP 5432 を許可。

## Proxy Lambda

| 項目 | 値 |
|---|---|
| 関数名 | `myfriend-agentcore-proxy` |
| ランタイム | Node.js 24 |
| メモリ | 512MB |
| タイムアウト | 15分 |
| 環境変数 | `AGENT_RUNTIME_ARN` |

### 処理フロー

```mermaid
sequenceDiagram
    participant Client as Next.js (Vercel)
    participant APIGW as API Gateway
    participant PL as Proxy Lambda
    participant AC as AgentCore Runtime

    Client->>APIGW: POST /v1<br/>x-api-key ヘッダー<br/>{prompt, bank_id, messages}
    APIGW->>PL: Lambda Response Streaming

    PL->>PL: リクエストパース<br/>bank_id UUID バリデーション<br/>prompt 長さチェック (≤10000文字)
    PL->>AC: InvokeAgentRuntime<br/>{prompt, bank_id, messages}

    loop SSE ストリーム
        AC-->>PL: data: "テキストチャンク"
        PL-->>APIGW: プレーンテキスト転送
        APIGW-->>Client: ストリーミングレスポンス
    end
```

### バリデーション

- `bank_id`: UUID 形式必須
- `prompt`: 必須、文字列型、最大10,000文字
- `sessionId`: 省略時は自動生成（UUID v4）

## API Gateway

| 項目 | 値 |
|---|---|
| API 名 | `MyfriendApi` |
| エンドポイント | Regional |
| ステージ | `v1` |
| 認証 | API Key 必須 |
| ストリーミング | Lambda Response Streaming 対応 |
| 統合タイムアウト | 15分 |

### Usage Plan

| 項目 | 値 |
|---|---|
| レートリミット | 10 req/sec |
| バーストリミット | 20 req |
| 日次クォータ | 50 req/day |
