# プロジェクト概要

TanStack Start + Hono RPC + React Query のモノレポ構成。Cloudflare Workers にデプロイ。

## パッケージ構成

- `apps/web` — TanStack Start フロントエンド + サーバー
- `packages/api` — Hono API（webのサーバープロセス内で動作。独立サーバーではない）
- `packages/db` — Drizzle ORM + Turso
- `packages/auth` — better-auth

## 開発フロー

開発フローに沿って作業を行う。
必要に応じてエージェント、スキルを使用する。

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

- `pnpm --filter web cf-typegen` 型を生成
- `pnpm build` 型を生成
- `code-reviewer` サブエージェントでコードレビュー
- `pnpm typecheck` で型チェック
- `pnpm lint` でlint
- セキュリティに関わるコードは `security-reviewer` サブエージェントでレビュー
