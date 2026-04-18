---
name: task-management
description: todos CLI を使った開発タスク管理の実行手順。フェーズごとのコマンドパターンとテンプレートを提供する。
---

# タスク管理スキル

**ワークフロールール: `.claude/rules/dev-workflow.md` が正。本スキルは実行手順の補足。**
**コマンド仕様: `todos --help` / `todos <cmd> --help` が正。**

## タスク作成テンプレート

```bash
# 1. 親タスク作成
todos add "機能名: 概要" --label <category> --project <name> --created-by ai
# label: feature | bug | improvement | refactor | documentation | chore
# project: 機能グループ名（archive, tui-form 等）

# 2. 親タスク ID 取得（前方一致 prefix を使用）
todos list --format json

# 3. サブタスク一括作成（Phase 順）
todos add "設計・計画" --parent <PREFIX> --created-by ai
todos add "テスト設計" --parent <PREFIX> --created-by ai
todos add "実装: xxx" --parent <PREFIX> --created-by ai   # 実装単位ごとに分割
todos add "テスト実装・実行" --parent <PREFIX> --created-by ai
todos add "検証・レビュー" --parent <PREFIX> --created-by ai
```

## フェーズ実行パターン

各フェーズ開始時に `todos status <ID> in_progress`、完了時に `todos status <ID> done`。

### Phase 1-2: 設計・テスト設計（承認ゲート）

```bash
# テストケースを content に記録
todos edit <ID> --content "1. 正常系: ... 2. 異常系: ... 3. 境界値: ..."
```

ユーザーに設計/テストケースを提示 → **承認を得てから** done にする。

### Phase 3: 実装（1サブタスクずつ）

```bash
todos status <ID> in_progress   # 着手
# コード実装 → cargo build パス
todos status <ID> done          # 完了してから次へ
```

### Phase 5: 検証（CI 準拠）

CI（`setup-rust-toolchain@v1`）は `RUSTFLAGS="-D warnings"` を設定するため、テストでもコンパイル警告がエラーになる。ローカルで同条件を再現すること。

```bash
cargo fmt --all -- --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && RUSTFLAGS="-D warnings" cargo test --all-features
```

### Phase 6: 完了

```bash
# 全サブタスク done 確認後
todos status <PARENT_ID> done
```

## 進捗確認

```bash
todos list -P <project>              # プロジェクト内タスク一覧
todos list -P <project> --all        # done/cancelled 含む
todos show <ID>                      # 個別タスク詳細
```
