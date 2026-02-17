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
- `cdk/` — AWS CDK インフラ（TypeScript）
- `postgresql/` — ローカル開発用 Docker（pgvector/pgvector:pg16）
- `docs/設計/` — アーキテクチャ設計書・実装タスク

## 記憶システム（3段階）

- **短期記憶（Raw Facts）** — 会話から5W1H事実を抽出、Embedding付きで永続化
- **中期記憶（Observations）** — Raw Factsの自動統合・パターン検出・鮮度追跡
- **長期記憶（Mental Models）** — キュレーション済みサマリ、Reflect による深い推論

## 技術スタック

- **Agent**: Strands Agents SDK, Bedrock AgentCore
- **LLM**: Bedrock Converse API（Claude Haiku 4.5 / Sonnet 4.5）
- **Embedding**: Bedrock Titan Embed V2（1024次元）
- **DB**: PostgreSQL 16 + pgvector + pg_trgm
- **開発DB**: Docker（`postgresql/docker-compose.yml`）
- **本番DB**: Aurora Serverless v2（商用化フェーズで移行）
- **IaC**: AWS CDK（TypeScript）

## 実装進捗

タスク詳細は `docs/設計/記憶システム実装タスク.md` を参照。各タスクの見出しに ✅ が付いているものは完了済み。

### Phase 1: 短期記憶

| タスク | ステータス | 備考 |
|---|---|---|
| 1.1.1 Docker PostgreSQL 環境の拡張 | ✅ 完了 | Dockerfile, docker-compose.yml 修正済み |
| 1.1.2 DB マイグレーション（短期記憶テーブル） | ✅ 完了 | 001_extensions.sql, 002_short_term.sql 作成済み |
| 1.1.3 Python DB 接続設定 | ✅ 完了 | memory/db.py, pyproject.toml, .env 更新済み |
| 1.2.1 Memory Engine コアモジュール | 未着手 | |
| 1.2.2 LLM ファクト抽出 | 未着手 | |
| 1.2.3 Embedding 生成 | 未着手 | |
| 1.2.4 エンティティ抽出・解決 | 未着手 | |
| 1.2.5 Retain パイプライン | 未着手 | |
| 1.2.6 Recall パイプライン（基本版） | 未着手 | |
| 1.2.7 Strands Agent ツール統合 | 未着手 | |
| 1.3 検証 | 未着手 | Docker起動確認は未実施 |

### Phase 2: 中期記憶

| タスク | ステータス |
|---|---|
| 2.1.1 DB マイグレーション（中期記憶テーブル） | 未着手 |
| 2.1.2 ローカル Consolidation スケジューラー | 未着手 |
| 2.2.1 Consolidation Worker コアロジック | 未着手 |
| 2.2.2 鮮度追跡 | 未着手 |
| 2.3.1 グラフリンク構築 | 未着手 |
| 2.3.2 MPFP グラフ検索 | 未着手 |
| 2.3.3 時間検索 | 未着手 |
| 2.4.1 4方向並列検索統合 | 未着手 |
| 2.4.2 クロスエンコーダリランキング | 未着手 |
| 2.4.3 バッチクエリ最適化 | 未着手 |

### Phase 3: 長期記憶

| タスク | ステータス |
|---|---|
| 3.1.1 DB マイグレーション（長期記憶テーブル） | 未着手 |
| 3.2.1 Mental Model CRUD | 未着手 |
| 3.2.2 Mental Model 自動リフレッシュ | 未着手 |
| 3.2.3 Reflect パイプライン | 未着手 |
| 3.2.4 Disposition（性格特性）適用 | 未着手 |
| 3.2.5 ディレクティブシステム | 未着手 |
| 3.2.6 Strands Agent Reflect ツール統合 | 未着手 |
| 3.3.1 並行制御 | 未着手 |
| 3.3.2 タグベース可視性制御 | 未着手 |

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
