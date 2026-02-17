---
name: skill-create
description: ローカルのgit履歴を分析してコーディングパターンを抽出し、SKILL.mdファイルを生成する。Skill Creator GitHub Appのローカル版。
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /skill-create - ローカルスキル生成

リポジトリのgit履歴を分析してコーディングパターンを抽出し、チームの慣習をClaudeに教えるSKILL.mdファイルを生成する。

## 使い方

```bash
/skill-create                    # 現在のリポジトリを分析
/skill-create --commits 100      # 直近100件のコミットを分析
/skill-create --output ./skills  # カスタム出力ディレクトリ
/skill-create --instincts        # continuous-learning-v2用のインスティンクトも生成
```

## 機能

1. **Git履歴を解析** - コミット、ファイル変更、パターンを分析
2. **パターンを検出** - 繰り返しのワークフローと慣習を特定
3. **SKILL.mdを生成** - 有効なClaude Codeスキルファイルを作成
4. **オプションでインスティンクトを作成** - continuous-learning-v2システム用

## 分析ステップ

### ステップ1: Gitデータの収集

```bash
# ファイル変更を含む直近のコミットを取得
git log --oneline -n ${COMMITS:-200} --name-only --pretty=format:"%H|%s|%ad" --date=short

# ファイル別コミット頻度を取得
git log --oneline -n 200 --name-only | grep -v "^$" | grep -v "^[a-f0-9]" | sort | uniq -c | sort -rn | head -20

# コミットメッセージのパターンを取得
git log --oneline -n 200 | cut -d' ' -f2- | head -50
```

### ステップ2: パターンの検出

以下のパターンタイプを探す:

| パターン | 検出方法 |
|---------|---------|
| **コミット規約** | コミットメッセージに対する正規表現（feat:、fix:、chore:） |
| **ファイルの同時変更** | 常に一緒に変更されるファイル |
| **ワークフローシーケンス** | 繰り返されるファイル変更パターン |
| **アーキテクチャ** | フォルダ構造と命名規約 |
| **テストパターン** | テストファイルの場所、命名、カバレッジ |

### ステップ3: SKILL.mdの生成

出力フォーマット:

```markdown
---
name: {リポジトリ名}-patterns
description: {リポジトリ名}から抽出されたコーディングパターン
version: 1.0.0
source: local-git-analysis
analyzed_commits: {件数}
---

# {リポジトリ名} パターン

## コミット規約
{検出されたコミットメッセージパターン}

## コードアーキテクチャ
{検出されたフォルダ構造と組織}

## ワークフロー
{検出された繰り返しファイル変更パターン}

## テストパターン
{検出されたテスト慣習}
```

### ステップ4: インスティンクトの生成（--instincts指定時）

continuous-learning-v2統合用:

```yaml
---
id: {リポジトリ}-commit-convention
trigger: "コミットメッセージを書く時"
confidence: 0.8
domain: git
source: local-repo-analysis
---

# Conventional Commitsを使用

## アクション
コミットのプレフィックス: feat:、fix:、chore:、docs:、test:、refactor:

## エビデンス
- {n}件のコミットを分析
- {percentage}%がConventional Commitフォーマットに従っている
```

## 出力例

TypeScriptプロジェクトで `/skill-create` を実行した場合の例:

```markdown
---
name: my-app-patterns
description: my-appリポジトリからのコーディングパターン
version: 1.0.0
source: local-git-analysis
analyzed_commits: 150
---

# My App パターン

## コミット規約

このプロジェクトは**Conventional Commits**を使用:
- `feat:` - 新機能
- `fix:` - バグ修正
- `chore:` - メンテナンスタスク
- `docs:` - ドキュメント更新

## コードアーキテクチャ

```
src/
├── components/     # Reactコンポーネント（PascalCase.tsx）
├── hooks/          # カスタムフック（use*.ts）
├── utils/          # ユーティリティ関数
├── types/          # TypeScript型定義
└── services/       # APIと外部サービス
```

## ワークフロー

### 新しいコンポーネントの追加
1. `src/components/ComponentName.tsx` を作成
2. `src/components/__tests__/ComponentName.test.tsx` にテストを追加
3. `src/components/index.ts` からエクスポート

### データベースマイグレーション
1. `src/db/schema.ts` を変更
2. `pnpm db:generate` を実行
3. `pnpm db:migrate` を実行

## テストパターン

- テストファイル: `__tests__/` ディレクトリまたは `.test.ts` サフィックス
- カバレッジ目標: 80%以上
- フレームワーク: Vitest
```

## GitHub App統合

高度な機能（10k以上のコミット、チーム共有、自動PR）には、[Skill Creator GitHub App](https://github.com/apps/skill-creator)を使用:

- インストール: [github.com/apps/skill-creator](https://github.com/apps/skill-creator)
- 任意のissueに `/skill-creator analyze` とコメント
- 生成されたスキルのPRを受け取る

## 関連コマンド

- `/instinct-import` - 生成されたインスティンクトをインポート
- `/instinct-status` - 学習済みインスティンクトを表示
- `/evolve` - インスティンクトをスキル/エージェントにクラスタリング

---

*[Everything Claude Code](https://github.com/affaan-m/everything-claude-code)の一部*
