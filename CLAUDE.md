# プロジェクト概要

Hindsight 論文ベースの3段階記憶モデルを持つ AI エージェントシステム。AWS Bedrock AgentCore + Strands Agent（Python）で構成。

## アーキテクチャ

```
API Gateway → Proxy Lambda (Node.js) → Bedrock AgentCore → Strands Agent (Python)
                                                                 │
                                                           Memory Module
                                                                 │
                                                     PostgreSQL (pgvector)
```

## ディレクトリ構成

- `agentcore/` — Strands Agent エントリポイント + 記憶システム（Python 3.14+）
  - `main.py` — AgentCore デプロイ用エントリポイント（BedrockAgentCoreApp）
  - `local.py` — ローカル開発用 HTTP サーバー（aiohttp, port 8080）
  - `core.py` — Agent 共通ロジック（main.py / local.py 共用）
- `requester/` — ローカルテスト用チャット UI（HonoX + Cloudflare Workers + Tailwind）
- `cdk/` — AWS CDK インフラ（TypeScript）
- `postgresql/` — ローカル開発用 Docker（pgvector/pgvector:pg16 + pg_bigm）
- `docs/設計/` — アーキテクチャ設計書・実装タスク

## 記憶システム（3段階）

- **短期記憶（Raw Facts）** — 会話から5W1H事実を抽出、Embedding付きで永続化
- **中期記憶（Observations）** — Raw Factsの自動統合・パターン検出・鮮度追跡
- **長期記憶（Mental Models）** — キュレーション済みサマリ、Reflect による深い推論

## 技術スタック

- **Agent**: Strands Agents SDK, Bedrock AgentCore
- **LLM**: Bedrock Converse API（Claude Haiku 4.5 / Sonnet 4.5）
- **Embedding**: Bedrock Titan Embed V2（1024次元）
- **DB**: PostgreSQL 16 + pgvector + pg_trgm + pg_bigm（日本語全文検索）
- **開発DB**: Docker（`postgresql/docker-compose.yml`）
- **本番DB**: Aurora Serverless v2（商用化フェーズで移行）
- **IaC**: AWS CDK（TypeScript）
- **ローカルテスト**: `requester/`（HonoX + Tailwind）、`agentcore/local.py`（aiohttp）

## 実装進捗

タスク詳細は `docs/設計/記憶システム実装タスク.md` を参照。各タスクの見出しに ✅ が付いているものは完了済み。![alt text](<ss 1.png>)

## 開発フロー

開発フローに沿って作業を行う。
必要に応じてエージェント、スキルを使用する。

実装タスクは`docs/設計/記憶システム実装タスク.md`を参照すること。

### 1. 計画

- 非自明なタスクは必ず `EnterPlanMode` で計画を立ててから実装する
- 探索や調査には `Explore` サブエージェントを使う
- 計画時に `Plan` サブエージェントでアプローチを設計する

### 2. 実装

- 複数ステップのタスクは `TaskCreate` で進捗を管理する
- ビルドや型エラーが出たら `build-error-resolver` サブエージェントで解決する
- ライブラリの最新APIが不明な場合は `tech-docs-searcher` サブエージェントで調査する
- 複雑なワークフロー（分析→実装→テスト等）は `orchestrator` サブエージェントで分割・並列実行する
- リファクタリングや不要コード削除は `refactor-cleaner` サブエージェントに委任する

### 3. レビュー

実装後は必ず以下を実行する:

- `code-reviewer` サブエージェントでコードレビュー
- セキュリティに関わるコードは `security-reviewer` サブエージェントでレビュー

### ４. 進捗管理

- 実装タスクの進捗状況を更新する
  - `docs/設計/記憶システム実装タスク.md`
