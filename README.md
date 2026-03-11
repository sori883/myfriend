# イメージ
かわいい。
![かわいい](./image.jpg)

# アーキテクチャ

## システム構成図

```mermaid
graph TB
    subgraph Frontend["フロントエンド (Vercel)"]
        Next["Next.js / AITuberKit"]
    end

    subgraph AWS["AWS (ap-northeast-1)"]
        APIGW["API Gateway<br/>REST API + API Key認証"]

        subgraph VPC["VPC (Private Subnet)"]
            Proxy["Proxy Lambda<br/>Node.js 24<br/>Response Streaming"]
            AgentCore["Bedrock AgentCore<br/>Strands Agent (Python)"]
            Aurora["Aurora Serverless v2<br/>PostgreSQL 16.4<br/>pgvector / pg_trgm / AGE"]
            Batch["Batch Lambda<br/>Python (Docker)"]
        end

        EventBridge["EventBridge<br/>5分間隔"]
        Bedrock["Amazon Bedrock"]
        SecretsManager["Secrets Manager"]
    end

    Next -->|POST /v1| APIGW
    APIGW --> Proxy
    Proxy -->|InvokeAgentRuntime| AgentCore
    AgentCore -->|recall / retain / reflect| Aurora
    AgentCore -->|Converse API<br/>Claude Sonnet| Bedrock
    EventBridge --> Batch
    Batch -->|Consolidation| Aurora
    Batch -->|Claude Haiku| Bedrock
    AgentCore -->|Titan Embed V2<br/>Rerank API| Bedrock
    Aurora -.->|認証情報| SecretsManager
```

# セットアップ

## 前提条件

| ツール | 用途 |
|--------|------|
| Docker | PostgreSQL (pgvector) |
| [uv](https://docs.astral.sh/uv/) | Python パッケージ管理 (agentcore / batch) |
| [pnpm](https://pnpm.io/) | Node.js パッケージ管理 (front) |
| AWS CLI | デプロイ・リストア |

## 環境変数

各サービスの `.env.example` をコピーして `.env.local` を作成し、必要な値を設定する。

```bash
cp agentcore/.env.example agentcore/.env.local
cp batch/.env.example batch/.env.local
cp front/.env.example front/.env.local
```

### agentcore/.env.local

| 変数 | 必須 | 説明 |
|------|:----:|------|
| `AWS_ACCESS_KEY_ID` | o | AWS アクセスキー |
| `AWS_SECRET_ACCESS_KEY` | o | AWS シークレットキー |
| `AWS_REGION` | | デフォルト: `ap-northeast-1` |
| `DATABASE_URL` | | デフォルト: `postgresql://postgres:postgres@localhost:5432/myfriend` |
| `AGENT_MODEL_ID` | | エージェント用 Bedrock モデル ID |
| `EXTRACTION_MODEL_ID` | | 事実抽出用モデル ID |
| `CONSOLIDATION_MODEL_ID` | | 統合用モデル ID |
| `EMBEDDING_MODEL_ID` | | Embedding モデル ID |
| `RERANK_MODEL_ID` | | Rerank モデル ID |
| `REFLECT_MODEL_ID` | | Reflect 用モデル ID |
| `TAVILY_API_KEY` | | Tavily Web 検索 API キー |

### batch/.env.local

| 変数 | 必須 | 説明 |
|------|:----:|------|
| `AWS_ACCESS_KEY_ID` | o | AWS アクセスキー |
| `AWS_SECRET_ACCESS_KEY` | o | AWS シークレットキー |
| `AWS_REGION` | | デフォルト: `ap-northeast-1` |
| `DATABASE_URL` | | デフォルト: `postgresql://postgres:postgres@localhost:5432/myfriend` |
| `CONSOLIDATION_MODEL_ID` | | 統合用モデル ID |
| `REFLECT_MODEL_ID` | | Reflect 用モデル ID |
| `EMBEDDING_MODEL_ID` | | Embedding モデル ID |
| `BATCH_MAX_BANKS` | | 1回の実行で処理するバンク数上限（デフォルト: 50） |

### front/.env.local

| 変数 | 必須 | 説明 |
|------|:----:|------|
| `NEXT_PUBLIC_SELECT_AI_SERVICE` | o | `agentcore` を指定 |
| `AGENTCORE_URL` | o | AgentCore の URL（デフォルト: `http://localhost:8080`） |
| `AGENTCORE_API_KEY` | | API Gateway API キー（Vercel デプロイ時） |
| `AGENTCORE_BANK_ID` | o | メモリバンク ID（UUID） |
| `SELECTED_VRM_PATH` | o | VRM モデルファイルのパス |

その他オプション設定（音声合成、YouTube 連携等）は `front/.env.example` を参照。

### cdk/.env.dev（デプロイ時のみ）

| 変数 | 必須 | 説明 |
|------|:----:|------|
| `ACCOUNT_ID` | o | AWS アカウント ID |
| `AGENT_MODEL_ID` | | エージェント用 Bedrock モデル ID |
| `TAVILY_API_KEY` | | Tavily API キー |

## クイックスタート

```bash
# 全サービスを一括起動（DB + agent + batch + front）
make dev
```

## Make コマンド一覧

### 起動

| コマンド | 説明 |
|----------|------|
| `make dev` | DB + agent + batch + front を一括起動 |
| `make db` | PostgreSQL を起動 |
| `make agent` | エージェント (Strands Agent) を起動 |
| `make batch` | Consolidation バッチ（60秒間隔）を起動 |
| `make front` | フロントエンド (Next.js) を起動 |

### 停止・リセット

| コマンド | 説明 |
|----------|------|
| `make down` | 全停止（プロセス + DB） |
| `make clean` | front + agent + batch のプロセスを停止 |
| `make db-down` | PostgreSQL を停止 |
| `make db-reset` | PostgreSQL のデータを削除して再初期化 |

### バックアップ・リストア

| コマンド | 説明 |
|----------|------|
| `make db-backup` | データのみバックアップ |
| `make db-backup-full` | スキーマ+データの完全バックアップ |
| `make db-backup-list` | バックアップ一覧を表示 |
| `make db-restore` | 最新のバックアップをリストア |
| `make db-restore-file FILE=path` | 指定ファイルからリストア |

### テスト・その他

| コマンド | 説明 |
|----------|------|
| `make seed` | ペルソナ発話データを DB に投入 |
| `make test-e2e` | E2E テストを実行 |
| `make logs` | Docker コンテナの状態を確認 |
| `make help` | コマンド一覧を表示 |

## デプロイ (CDK)

```bash
pnpm cdk:dev deploy
```

# モデル

- 原則ローカルで使用する予定で外部に露見させない
- 外部で使用する場合は容易に取り出せない状態とする
  - 対象者を絞った限定的な公開
  - Next.jsのミドルウェアで認証設定
  - モデルを暗号化


詳細は下記を参照
docs/設計/フロントエンド/モデル保護.md


https://mk22.booth.pm/items/5007531

# docs

下記参照

- docs/設計
  - AWS
  - フロントエンド
  - 記憶システム

# 記憶システム

- Hindsight
  - https://arxiv.org/pdf/2512.12818

# 実装マイルストーン
[x] 記憶保持、検索

[x] 外見の創造

[x] 知らないことを教えて欲しい

[ ] 最終目標:ハッピーエンド
- 現実とのお別れ
