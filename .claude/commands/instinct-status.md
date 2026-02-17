---
name: instinct-status
description: 学習済みインスティンクトの確信度レベルを全て表示する
command: true
---

# インスティンクトステータスコマンド

学習済みの全インスティンクトをドメインごとにグループ化し、確信度スコアと共に表示する。

## 実装

プラグインルートパスを使用してインスティンクトCLIを実行:

```bash
python3 "${CLAUDE_PLUGIN_ROOT}/skills/continuous-learning-v2/scripts/instinct-cli.py" status
```

`CLAUDE_PLUGIN_ROOT` が設定されていない場合（手動インストール）:

```bash
python3 ~/.claude/skills/continuous-learning-v2/scripts/instinct-cli.py status
```

## 使い方

```
/instinct-status
/instinct-status --domain code-style
/instinct-status --low-confidence
```

## 実行内容

1. `~/.claude/homunculus/instincts/personal/` から全インスティンクトファイルを読み取り
2. `~/.claude/homunculus/instincts/inherited/` から継承インスティンクトを読み取り
3. ドメインごとにグループ化し、確信度バーと共に表示

## 出力フォーマット

```
📊 インスティンクトステータス
==================

## コードスタイル（4インスティンクト）

### prefer-functional-style
トリガー: 新しい関数を書く時
アクション: クラスよりも関数型パターンを使用
確信度: ████████░░ 80%
ソース: session-observation | 最終更新: 2025-01-22

### use-path-aliases
トリガー: モジュールをインポートする時
アクション: 相対インポートの代わりに@/パスエイリアスを使用
確信度: ██████░░░░ 60%
ソース: repo-analysis (github.com/acme/webapp)

## テスト（2インスティンクト）

### test-first-workflow
トリガー: 新機能を追加する時
アクション: テストを先に書き、次に実装
確信度: █████████░ 90%
ソース: session-observation

## ワークフロー（3インスティンクト）

### grep-before-edit
トリガー: コードを変更する時
アクション: Grepで検索、Readで確認、次にEdit
確信度: ███████░░░ 70%
ソース: session-observation

---
合計: 9インスティンクト（4個人、5継承）
オブザーバー: 実行中（最終分析: 5分前）
```

## フラグ

- `--domain <名前>`: ドメインでフィルタ（code-style、testing、gitなど）
- `--low-confidence`: 確信度0.5未満のインスティンクトのみ表示
- `--high-confidence`: 確信度0.7以上のインスティンクトのみ表示
- `--source <タイプ>`: ソースでフィルタ（session-observation、repo-analysis、inherited）
- `--json`: プログラム利用用にJSON形式で出力
