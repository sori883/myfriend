# 6. AWS CDK インフラ

ローカル Docker 開発環境から AWS 本番環境への移行。Aurora Serverless v2 + Bedrock AgentCore + Batch Lambda を CDK で定義・デプロイする。

## 6.1 CDK プロジェクト初期化 ✅

### 6.1.1 CDK プロジェクトスキャフォールド ✅

- `@aws-cdk/aws-bedrock-agentcore-alpha`、`@aws-cdk/aws-bedrock-alpha` 等を依存に追加
- `bin/app.ts`: context `env` (dev/stg/prd) で構成切り替え

### 6.1.2 パラメータモジュール ✅

- zod で環境名・`.env` 変数をバリデーション
- 環境差分パラメータ（VPC, Aurora, Batch, AgentCore）

### 6.1.3 メインスタック定義 ✅

- Network → Database → AgentCore → Batch の順にインスタンス化

---

## 6.2 ネットワーク ✅

### 6.2.1 VPC コンストラクト ✅

- CIDR: `10.0.0.0/16`、AZ: 2、NAT Gateway: 1台
- サブネット: Public / Private with Egress / Private Isolated
- VPC エンドポイント: bedrock-runtime, secretsmanager, ecr.api, ecr.dkr, logs, s3
- セキュリティグループ: vpc-endpoints, lambda, aurora

---

## 6.3 データベース ✅

### 6.3.1 Secrets Manager + Aurora Serverless v2 ✅

- Aurora PostgreSQL 16.4（pgvector 対応）
- Serverless v2: 0.5〜8 ACU

### 6.3.2 DB マイグレーション Custom Resource Lambda ✅

- Node.js 22、VPC 内配置
- `_migration_history` テーブルでべき等性確保

### 6.3.3 Aurora 用マイグレーション SQL 作成 ✅

- `pg_bigm` → `pg_trgm` に変更（Aurora 非対応のため）

---

## 6.4 AgentCore ✅

### 6.4.1 AgentCore Runtime コンストラクト ✅

- `@aws-cdk/aws-bedrock-agentcore-alpha` でデプロイ
- `AgentRuntimeArtifact.fromAsset()` でローカルディレクトリを直接パッケージング

### 6.4.2 Proxy Lambda コンストラクト ✅

- `NodejsFunction`、Node.js 22、メモリ 512MB、タイムアウト 15分
- IAM: `bedrock-agentcore:InvokeAgentRuntime`

### 6.4.3 API Gateway コンストラクト ✅

- Lambda Response Streaming 対応
- API Key + Usage Plan 認証

### 6.4.4 Proxy Lambda 実装 ✅

- `awslambda.streamifyResponse()` でストリーミング
- bank_id UUID バリデーション、エラーサニタイズ

### 6.4.5 Next.js API Key 対応 ✅

- `AGENTCORE_API_KEY` 環境変数で `x-api-key` ヘッダー付与

---

## 6.5 バッチ処理 ✅

### 6.5.1 Docker イメージ（Batch） ✅

- `DockerImageFunction` で自動ビルド&プッシュ

### 6.5.2 Lambda（Container Image） ✅

- メモリ 1024MB、VPC 内配置（Isolated subnets）
- 予約同時実行数: 1、DLQ: SQS

### 6.5.3 EventBridge スケジュール ✅

- 5分間隔、リトライ最大2回
- dev 環境では `enabled: false`

---

## 6.6 アプリケーション改修（Aurora 対応） ✅

### 6.6.1 DB 接続の Secrets Manager 対応 ✅

- `_resolve_database_url()`: DB_SECRET_ARN → Secrets Manager、未設定 → DATABASE_URL
- SSL 対応、`.env` / `.env.local` 環境分離

### 6.6.2 pg_bigm → pg_trgm 統一 ✅

- Aurora / ローカル両環境で `pg_trgm` に統一

---

## 6.8 検証 ✅

- [x] `cdk synth` / `cdk deploy` 成功
- [x] VPC + Aurora + DB マイグレーション
- [x] AgentCore Runtime デプロイ
- [x] API Gateway → Proxy Lambda → AgentCore → Aurora E2E
- [x] Batch Lambda + EventBridge
- [x] ローカル / AWS 両環境 DB 接続
