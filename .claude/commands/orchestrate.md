# オーケストレートコマンド

複雑なタスクのための順次エージェントワークフロー。

## 使い方

`/orchestrate [ワークフロータイプ] [タスクの説明]`

## ワークフロータイプ

### feature
機能実装の完全なワークフロー:
```
planner -> tdd-guide -> code-reviewer -> security-reviewer
```

### bugfix
バグ調査と修正のワークフロー:
```
explorer -> tdd-guide -> code-reviewer
```

### refactor
安全なリファクタリングワークフロー:
```
architect -> code-reviewer -> tdd-guide
```

### security
セキュリティ重点レビュー:
```
security-reviewer -> code-reviewer -> architect
```

## 実行パターン

ワークフロー内の各エージェントに対して:

1. **エージェントを呼び出し** 前のエージェントからのコンテキストを渡す
2. **出力を収集** 構造化された引き継ぎドキュメントとして
3. **次のエージェントに渡す** チェーン内の次へ
4. **結果を集約** 最終レポートにまとめる

## 引き継ぎドキュメントフォーマット

エージェント間で引き継ぎドキュメントを作成:

```markdown
## HANDOFF: [前のエージェント] -> [次のエージェント]

### コンテキスト
[実行内容の要約]

### 発見事項
[主要な発見や決定]

### 変更されたファイル
[変更されたファイルのリスト]

### 未解決の質問
[次のエージェントへの未解決項目]

### 推奨事項
[提案する次のステップ]
```

## 例: 機能ワークフロー

```
/orchestrate feature "ユーザー認証を追加"
```

実行内容:

1. **Plannerエージェント**
   - 要件を分析
   - 実装計画を作成
   - 依存関係を特定
   - 出力: `HANDOFF: planner -> tdd-guide`

2. **TDD Guideエージェント**
   - plannerの引き継ぎを読み取り
   - テストを先に作成
   - テストをパスするよう実装
   - 出力: `HANDOFF: tdd-guide -> code-reviewer`

3. **Code Reviewerエージェント**
   - 実装をレビュー
   - 問題をチェック
   - 改善を提案
   - 出力: `HANDOFF: code-reviewer -> security-reviewer`

4. **Security Reviewerエージェント**
   - セキュリティ監査
   - 脆弱性チェック
   - 最終承認
   - 出力: 最終レポート

## 最終レポートフォーマット

```
オーケストレーションレポート
====================
ワークフロー: feature
タスク: ユーザー認証を追加
エージェント: planner -> tdd-guide -> code-reviewer -> security-reviewer

サマリー
-------
[1段落の要約]

エージェント出力
-------------
Planner: [要約]
TDD Guide: [要約]
Code Reviewer: [要約]
Security Reviewer: [要約]

変更されたファイル
-------------
[変更された全ファイルのリスト]

テスト結果
------------
[テストのパス/フェイルサマリー]

セキュリティステータス
---------------
[セキュリティの発見事項]

推奨
--------------
[リリース可 / 修正必要 / ブロック中]
```

## 並列実行

独立したチェックの場合、エージェントを並列実行:

```markdown
### 並列フェーズ
同時に実行:
- code-reviewer（品質）
- security-reviewer（セキュリティ）
- architect（設計）

### 結果のマージ
出力を単一のレポートに統合
```

## 引数

$ARGUMENTS:
- `feature <説明>` - 機能実装の完全なワークフロー
- `bugfix <説明>` - バグ修正ワークフロー
- `refactor <説明>` - リファクタリングワークフロー
- `security <説明>` - セキュリティレビューワークフロー
- `custom <エージェント> <説明>` - カスタムエージェントシーケンス

## カスタムワークフローの例

```
/orchestrate custom "architect,tdd-guide,code-reviewer" "キャッシュレイヤーを再設計"
```

## ヒント

1. **複雑な機能はplannerから開始** する
2. **マージ前には常にcode-reviewerを含める**
3. **認証/決済/PIIにはsecurity-reviewerを使用**
4. **引き継ぎは簡潔に** - 次のエージェントが必要とする情報に焦点を当てる
5. **必要に応じてエージェント間で検証を実行**
