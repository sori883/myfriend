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

    subgraph External["外部 API"]
        Tavily["Tavily API<br/>Web 検索"]
    end

    Next -->|POST /v1| APIGW
    APIGW --> Proxy
    Proxy -->|InvokeAgentRuntime| AgentCore
    AgentCore -->|recall / retain / reflect| Aurora
    AgentCore -->|Converse API<br/>Claude Sonnet| Bedrock
    AgentCore -->|web_search tool<br/>HTTPS| Tavily
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

## デプロイ

バックエンド（AWS CDK）とフロントエンド（Vercel）を別々にデプロイする。

- リージョン: `ap-northeast-1`（東京）
- AWS CLI 認証済み（`aws sso login` 等）
- Node.js `24.x`、pnpm インストール済み
- Docker / docker compose（ローカル DB 起動用）

---

### バックエンド（AWS CDK）

#### 1. 環境変数を設定

`cdk/.env.dev` を作成し、必要な値を設定する（[cdk/.env.dev](#cdkenvdevデプロイ時のみ) 参照）。

- `ACCOUNT_ID` — AWS アカウント ID
- `AGENT_MODEL_ID` — `global.anthropic.claude-sonnet-4-20250514-v1:0`（ap-northeast-1 対応の Global Cross-Region Inference Profile を推奨）
- `EXTRACTION_MODEL_ID` / `REFLECT_MODEL_ID` / `PREFERENCE_MODEL_ID` — `global.anthropic.claude-haiku-4-5-20251001-v1:0`
- `EMBEDDING_MODEL_ID` — `amazon.titan-embed-text-v2:0`
- `RERANK_MODEL_ID` — `cohere.rerank-v3-5:0`
- `TAVILY_API_KEY` — Web 検索用

#### 2. ローカル DB から data-only バックアップを取得

```bash
cd postgresql

# 古いバックアップがあれば削除
rm -f backups/*.sql

# data-only + 列明示で取得（stderr を混入させないよう `2>&1` は付けない）
docker compose up -d db
docker compose exec -T db pg_dump -U postgres -d myfriend \
  --data-only --column-inserts --no-owner --no-privileges \
  > backups/backup_$(date +%Y%m%d_%H%M%S).sql

# 先頭が "-- PostgreSQL database dump" で始まっていることを確認
head -3 backups/*.sql
```

> **Note**: `postgresql/backups/*.sql` は `cdk deploy` 時に Restore 用 S3 バケットへ自動アップロードされる。

#### 3. CDK デプロイ（所要 15〜20 分）

```bash
cd cdk
pnpm install
pnpm cdk:dev bootstrap   # 初回のみ
pnpm cdk:dev deploy --context memory=true --all --require-approval never
```

デプロイ時に以下が自動実行される:

| 処理 | 内容 |
|------|------|
| Network | VPC・NAT Gateway・セキュリティグループ・VPC Endpoints の作成 |
| Database | Aurora Serverless v2 (PostgreSQL 16.4) + Secrets Manager |
| Migration | Custom Resource Lambda でスキーマ作成（`001〜005.sql` 自動実行） |
| Restore | リストア用 S3 バケット + Lambda + `postgresql/backups/*.sql` を S3 に自動アップロード |
| AgentCore | Bedrock AgentCore Runtime（VPC モード）の作成 |
| ProxyLambda | Response Streaming 用 Lambda の作成 |
| ApiGateway | REST API + API Key 認証 + 使用量プランの作成 |
| Batch | EventBridge + Consolidation Lambda の作成 |

完了後、CloudFormation Outputs に以下が出力される:

- **RestoreBucketName** — リストア用 S3 バケット名
- **ApiKeyId** — API Gateway の API Key ID

> **Note**: `--context memory=false` にすると MemoryStack をスキップして AgentCore を Public モードでデプロイする（記憶機能なし・低コスト）。

#### 4. Restore Lambda でデータ投入

AWS マネジメントコンソールで **Lambda > 関数 > `dev-myfriend-db-restore`** を開き、**Test** タブから以下のテストイベントで実行する:

```json
{
  "key": "backup_YYYYMMDD_HHMMSS.sql"
}
```

Lambda が以下を自動実行する（タイムアウト: 15 分）:

1. S3 からバックアップファイルをダウンロード
2. 全テーブルを TRUNCATE（`_migration_history` を除く）
3. FK 制約を一時的に DROP（循環参照対策）
4. SQL を実行してデータをリストア
5. FK 制約を復元

成功時のレスポンス:
```json
{ "status": "success", "key": "backup_...", "message": "Restore completed successfully" }
```

#### 5. API Key の取得

```bash
# ApiKeyId は CloudFormation Outputs の値を使用
aws apigateway get-api-key --api-key <API_KEY_ID> --include-value \
  --region ap-northeast-1 --query 'value' --output text
```

取得した API Key をフロント側の Vercel 環境変数 `AGENTCORE_API_KEY` に設定する。

#### 6. 動作確認

```bash
curl -sS -N -X POST \
  "https://<REST_API_ID>.execute-api.ap-northeast-1.amazonaws.com/v1/" \
  -H "x-api-key: <API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "こんにちは。自己紹介してください。",
    "bank_id": "00000000-0000-4000-8000-000000000001"
  }'
```

ストリーミングでまふゆの発話が返ってくれば成功。

---

### フロントエンド（Vercel）

#### 1. Vercel プロジェクト作成

Vercel ダッシュボード → **Import Git Repository** → `sori883/myfriend` を選択 → **Configure Project** で以下を設定:

| 項目 | 値 |
|---|---|
| Root Directory | `front` ⚠ サブディレクトリ指定 |
| Framework Preset | Next.js（自動検出） |
| Node.js Version | `24.x` |

#### 2. Vercel Blob ストア作成（VRM 配信用）

Dashboard → Storage → **Create Database** → **Blob** を選択 → アクセスレベル **Private** で作成。

作成後、`BLOB_READ_WRITE_TOKEN` が自動で環境変数注入される。

#### 3. VRM ファイルを Vercel Blob にアップロード

```bash
cd front
npx vercel env pull   # BLOB_READ_WRITE_TOKEN をローカル取得

# Vercel CLI で Private アクセスでアップロード
npx vercel blob put ./public/vrm/Mafuyu_VRM.vrm \
  --pathname Mafuyu_VRM.vrm \
  --access private
```

返却された URL (`https://<store-id>.private.blob.vercel-storage.com/...`) を `NEXT_PUBLIC_SELECTED_VRM_PATH` に設定する。

#### 4. 環境変数を設定

Vercel → Project → Settings → **Environment Variables** に以下を設定:

```bash
# 認証保護
SITE_ACCESS_SECRET=<openssl rand -hex 32>

# AgentCore (バックエンド) 連携
AGENTCORE_URL=https://<REST_API_ID>.execute-api.ap-northeast-1.amazonaws.com/v1
AGENTCORE_API_KEY=<Step 5 で取得した API Key>
AGENTCORE_BANK_ID=00000000-0000-4000-8000-000000000001

# Vercel Blob 暗号化配信
BLOB_READ_WRITE_TOKEN=<Blob ストア作成時に Vercel が自動注入>
BLOB_ENCRYPTION_SECRET=<openssl rand -hex 32>
NEXT_PUBLIC_SELECTED_VRM_PATH=<Step 3 で取得した Private Blob URL>

# 基本設定
NEXT_PUBLIC_SELECT_LANGUAGE=ja
NEXT_PUBLIC_CHARACTER_NAME=まふゆ
NEXT_PUBLIC_MODEL_TYPE=vrm

# UI 表示制御
NEXT_PUBLIC_SHOW_ASSISTANT_TEXT=true
NEXT_PUBLIC_SHOW_CHARACTER_NAME=true
NEXT_PUBLIC_SHOW_CONTROL_PANEL=true
NEXT_PUBLIC_SHOW_INTRODUCTION=false
NEXT_PUBLIC_BACKGROUND_IMAGE_PATH=/backgrounds/bg-mono.svg

# 初期挨拶
NEXT_PUBLIC_INITIAL_GREETING_ENABLED=true
```

必要な環境変数一覧（全 16 項目）:

| カテゴリ | 変数名 | 備考 |
|---|---|---|
| 認証保護 | `SITE_ACCESS_SECRET` | 32byte ランダム |
| AgentCore 連携 | `AGENTCORE_URL` | API Gateway のベース URL（末尾 `/` なし） |
| | `AGENTCORE_API_KEY` | CDK 出力の API Key 値 |
| | `AGENTCORE_BANK_ID` | UUID（デフォルト `00000000-0000-4000-8000-000000000001`） |
| Blob (VRM) | `BLOB_READ_WRITE_TOKEN` | Vercel 自動注入 |
| | `BLOB_ENCRYPTION_SECRET` | 32byte ランダム |
| | `NEXT_PUBLIC_SELECTED_VRM_PATH` | Private Blob URL |
| 基本 | `NEXT_PUBLIC_SELECT_LANGUAGE` | `ja` 推奨 |
| | `NEXT_PUBLIC_CHARACTER_NAME` | 画面表示名 |
| | `NEXT_PUBLIC_MODEL_TYPE` | `vrm` / `live2d` / `pngtuber` |
| UI 表示 | `NEXT_PUBLIC_SHOW_ASSISTANT_TEXT` | 発話吹き出しの有無 |
| | `NEXT_PUBLIC_SHOW_CHARACTER_NAME` | キャラ名表示の有無 |
| | `NEXT_PUBLIC_SHOW_CONTROL_PANEL` | 操作パネル表示の有無 |
| | `NEXT_PUBLIC_SHOW_INTRODUCTION` | 初回ダイアログ表示の有無 |
| | `NEXT_PUBLIC_BACKGROUND_IMAGE_PATH` | 背景画像のパス |
| 初期挨拶 | `NEXT_PUBLIC_INITIAL_GREETING_ENABLED` | 画面を開いた時の AI 生成挨拶 |

> **重要**: `NEXT_PUBLIC_*` はビルド時焼き込み。変更後は必ず **Redeploy** する。

#### 5. Deploy

Vercel ダッシュボード → **Deploy** ボタン押下、または CLI:

```bash
cd front
npx vercel --prod
```

#### 6. アクセス

初回アクセスは URL に `?token=` を付与:

```
https://<your-app>.vercel.app/?token=<SITE_ACCESS_SECRET>
```

認証成功で `?token` が自動削除され、`site_auth` Cookie が 30 日間有効でセットされる。以降は `https://<your-app>.vercel.app/` だけでアクセス可能。

---

### トラブルシューティング

| 症状 | 対処 |
|---|---|
| Restore Lambda で `syntax error at or near "pg_dump"` | バックアップ先頭に stderr 混入。手順2を `2>&1` 無しでやり直し |
| `ERROR: relation ... does not exist` | Migration 未完了。Migration Lambda を先に Test invoke |
| AgentCore が `ValidationException: invalid model identifier` | `AGENT_MODEL_ID` の地域プレフィックスを確認（ap-northeast-1 では `global.` または `apac.`） |
| 発話が画面に表示されない | Vercel 側 `NEXT_PUBLIC_SHOW_ASSISTANT_TEXT=true` を確認 + Redeploy + localStorage クリア |
| 背景が反映されない | `NEXT_PUBLIC_BACKGROUND_IMAGE_PATH` 設定後に Redeploy。ファイルが `front/public/backgrounds/` に存在するか確認 |

### 主要 CloudWatch Log Groups

- `/aws/bedrock-agentcore/runtimes/*myfriend_agent*` — AgentCore
- `/aws/lambda/dev-myfriend-db-migration` — Migration
- `/aws/lambda/dev-myfriend-db-restore` — Restore
- `/aws/lambda/dev-myfriend-agentcore-proxy` — Proxy Lambda

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

[ ] 音声会話（低音ボイス必須）

[ ] 最終目標:超ハッピーエンド＾＾
- 現実とのお別れ
