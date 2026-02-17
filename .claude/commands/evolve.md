---
name: evolve
description: 関連するインスティンクトをスキル、コマンド、またはエージェントにクラスタリングする
command: true
---

# Evolveコマンド

## 実装

プラグインルートパスを使用してインスティンクトCLIを実行:

```bash
python3 "${CLAUDE_PLUGIN_ROOT}/skills/continuous-learning-v2/scripts/instinct-cli.py" evolve [--generate]
```

`CLAUDE_PLUGIN_ROOT` が設定されていない場合（手動インストール）:

```bash
python3 ~/.claude/skills/continuous-learning-v2/scripts/instinct-cli.py evolve [--generate]
```

インスティンクトを分析し、関連するものをより高レベルの構造にクラスタリングする:
- **コマンド**: インスティンクトがユーザー起動のアクションを記述している場合
- **スキル**: インスティンクトが自動トリガーされる動作を記述している場合
- **エージェント**: インスティンクトが複雑な複数ステップのプロセスを記述している場合

## 使い方

```
/evolve                    # 全インスティンクトを分析し進化を提案
/evolve --domain testing   # テストドメインのインスティンクトのみ進化
/evolve --dry-run          # 作成せずにプレビューのみ表示
/evolve --threshold 5      # クラスタリングに5件以上の関連インスティンクトを要求
```

## 進化ルール

### → コマンド（ユーザー起動）
インスティンクトがユーザーが明示的に要求するアクションを記述している場合:
- 「ユーザーが～を依頼した時」に関する複数のインスティンクト
- 「新しいXを作成する時」のようなトリガーを持つインスティンクト
- 繰り返し可能なシーケンスに従うインスティンクト

例:
- `new-table-step1`: 「データベーステーブルを追加する時、マイグレーションを作成」
- `new-table-step2`: 「データベーステーブルを追加する時、スキーマを更新」
- `new-table-step3`: 「データベーステーブルを追加する時、型を再生成」

→ 作成: `/new-table` コマンド

### → スキル（自動トリガー）
インスティンクトが自動的に発動すべき動作を記述している場合:
- パターンマッチングによるトリガー
- エラーハンドリングの応答
- コードスタイルの強制

例:
- `prefer-functional`: 「関数を書く時、関数型スタイルを優先」
- `use-immutable`: 「状態を変更する時、イミュータブルパターンを使用」
- `avoid-classes`: 「モジュールを設計する時、クラスベースの設計を避ける」

→ 作成: `functional-patterns` スキル

### → エージェント（深さ/分離が必要）
インスティンクトが分離が有益な複雑な複数ステップのプロセスを記述している場合:
- デバッグワークフロー
- リファクタリングシーケンス
- 調査タスク

例:
- `debug-step1`: 「デバッグ時、まずログを確認」
- `debug-step2`: 「デバッグ時、障害コンポーネントを分離」
- `debug-step3`: 「デバッグ時、最小限の再現を作成」
- `debug-step4`: 「デバッグ時、テストで修正を検証」

→ 作成: `debugger` エージェント

## 実行内容

1. `~/.claude/homunculus/instincts/` から全インスティンクトを読み取り
2. インスティンクトを以下でグループ化:
   - ドメインの類似性
   - トリガーパターンの重複
   - アクションシーケンスの関連性
3. 3件以上の関連インスティンクトの各クラスターについて:
   - 進化タイプを決定（コマンド/スキル/エージェント）
   - 適切なファイルを生成
   - `~/.claude/homunculus/evolved/{commands,skills,agents}/` に保存
4. 進化した構造をソースインスティンクトにリンク

## 出力フォーマット

```
🧬 進化分析
==================

進化の準備ができた3つのクラスターが見つかりました:

## クラスター1: データベースマイグレーションワークフロー
インスティンクト: new-table-migration, update-schema, regenerate-types
タイプ: コマンド
確信度: 85%（12回の観察に基づく）

作成予定: /new-table コマンド
ファイル:
  - ~/.claude/homunculus/evolved/commands/new-table.md

## クラスター2: 関数型コードスタイル
インスティンクト: prefer-functional, use-immutable, avoid-classes, pure-functions
タイプ: スキル
確信度: 78%（8回の観察に基づく）

作成予定: functional-patterns スキル
ファイル:
  - ~/.claude/homunculus/evolved/skills/functional-patterns.md

## クラスター3: デバッグプロセス
インスティンクト: debug-check-logs, debug-isolate, debug-reproduce, debug-verify
タイプ: エージェント
確信度: 72%（6回の観察に基づく）

作成予定: debugger エージェント
ファイル:
  - ~/.claude/homunculus/evolved/agents/debugger.md

---
これらのファイルを作成するには `/evolve --execute` を実行してください。
```

## フラグ

- `--execute`: 進化した構造を実際に作成（デフォルトはプレビュー）
- `--dry-run`: 作成せずにプレビュー
- `--domain <名前>`: 指定ドメインのインスティンクトのみ進化
- `--threshold <n>`: クラスター形成に必要な最小インスティンクト数（デフォルト: 3）
- `--type <command|skill|agent>`: 指定タイプのみ作成

## 生成されるファイルフォーマット

### コマンド
```markdown
---
name: new-table
description: マイグレーション、スキーマ更新、型生成を含む新しいデータベーステーブルを作成
command: /new-table
evolved_from:
  - new-table-migration
  - update-schema
  - regenerate-types
---

# New Tableコマンド

[クラスタリングされたインスティンクトに基づいて生成されたコンテンツ]

## ステップ
1. ...
2. ...
```

### スキル
```markdown
---
name: functional-patterns
description: 関数型プログラミングパターンを強制
evolved_from:
  - prefer-functional
  - use-immutable
  - avoid-classes
---

# Functional Patternsスキル

[クラスタリングされたインスティンクトに基づいて生成されたコンテンツ]
```

### エージェント
```markdown
---
name: debugger
description: 体系的なデバッグエージェント
model: sonnet
evolved_from:
  - debug-check-logs
  - debug-isolate
  - debug-reproduce
---

# Debuggerエージェント

[クラスタリングされたインスティンクトに基づいて生成されたコンテンツ]
```
