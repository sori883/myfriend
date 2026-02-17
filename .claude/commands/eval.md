# Evalコマンド

Eval駆動開発ワークフローを管理する。

## 使い方

`/eval [define|check|report|list] [機能名]`

## Evalの定義

`/eval define 機能名`

新しいEval定義を作成:

1. `.claude/evals/機能名.md` にテンプレートを作成:

```markdown
## EVAL: 機能名
作成日: $(date)

### 機能Eval
- [ ] [機能1の説明]
- [ ] [機能2の説明]

### リグレッションEval
- [ ] [既存動作1が引き続き動作する]
- [ ] [既存動作2が引き続き動作する]

### 成功基準
- 機能Evalのpass@3 > 90%
- リグレッションEvalのpass^3 = 100%
```

2. ユーザーに具体的な基準の入力を促す

## Evalのチェック

`/eval check 機能名`

機能のEvalを実行:

1. `.claude/evals/機能名.md` からEval定義を読み取り
2. 各機能Evalについて:
   - 基準の検証を試行
   - PASS/FAILを記録
   - `.claude/evals/機能名.log` に試行を記録
3. 各リグレッションEvalについて:
   - 関連テストを実行
   - ベースラインと比較
   - PASS/FAILを記録
4. 現在のステータスを報告:

```
EVAL CHECK: 機能名
========================
機能: X/Y パス
リグレッション: X/Y パス
ステータス: 進行中 / 準備完了
```

## Evalレポート

`/eval report 機能名`

包括的なEvalレポートを生成:

```
EVAL REPORT: 機能名
=========================
生成日: $(date)

機能EVAL
----------------
[eval-1]: PASS (pass@1)
[eval-2]: PASS (pass@2) - リトライが必要だった
[eval-3]: FAIL - 注記を参照

リグレッションEVAL
----------------
[test-1]: PASS
[test-2]: PASS
[test-3]: PASS

メトリクス
-------
機能 pass@1: 67%
機能 pass@3: 100%
リグレッション pass^3: 100%

注記
-----
[問題、エッジケース、または所見]

推奨
--------------
[リリース可 / 修正必要 / ブロック中]
```

## Eval一覧

`/eval list`

全Eval定義を表示:

```
EVAL定義一覧
================
feature-auth      [3/5パス] 進行中
feature-search    [5/5パス] 準備完了
feature-export    [0/4パス] 未開始
```

## 引数

$ARGUMENTS:
- `define <名前>` - 新しいEval定義を作成
- `check <名前>` - Evalを実行しチェック
- `report <名前>` - 完全なレポートを生成
- `list` - 全Evalを表示
- `clean` - 古いEvalログを削除（最新10回分を保持）
