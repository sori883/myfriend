---
name: instinct-export
description: チームメイトや他のプロジェクトと共有するためにインスティンクトをエクスポートする
command: /instinct-export
---

# インスティンクトエクスポートコマンド

インスティンクトを共有可能な形式にエクスポートする。以下に最適:
- チームメイトとの共有
- 新しいマシンへの転送
- プロジェクト慣習への貢献

## 使い方

```
/instinct-export                           # 全個人インスティンクトをエクスポート
/instinct-export --domain testing          # テスト用インスティンクトのみエクスポート
/instinct-export --min-confidence 0.7      # 高確信度のインスティンクトのみエクスポート
/instinct-export --output team-instincts.yaml
```

## 実行内容

1. `~/.claude/homunculus/instincts/personal/` からインスティンクトを読み取り
2. フラグに基づいてフィルタリング
3. 機密情報を除去:
   - セッションIDを削除
   - ファイルパスを削除（パターンのみ保持）
   - 「先週」より古いタイムスタンプを削除
4. エクスポートファイルを生成

## 出力フォーマット

YAMLファイルを作成:

```yaml
# インスティンクトエクスポート
# 生成日: 2025-01-22
# ソース: personal
# 件数: 12インスティンクト

version: "2.0"
exported_by: "continuous-learning-v2"
export_date: "2025-01-22T10:30:00Z"

instincts:
  - id: prefer-functional-style
    trigger: "新しい関数を書く時"
    action: "クラスよりも関数型パターンを使用"
    confidence: 0.8
    domain: code-style
    observations: 8

  - id: test-first-workflow
    trigger: "新機能を追加する時"
    action: "テストを先に書き、次に実装"
    confidence: 0.9
    domain: testing
    observations: 12

  - id: grep-before-edit
    trigger: "コードを変更する時"
    action: "Grepで検索、Readで確認、次にEdit"
    confidence: 0.7
    domain: workflow
    observations: 6
```

## プライバシーに関する考慮事項

エクスポートに含まれるもの:
- ✅ トリガーパターン
- ✅ アクション
- ✅ 確信度スコア
- ✅ ドメイン
- ✅ 観察回数

エクスポートに含まれないもの:
- ❌ 実際のコードスニペット
- ❌ ファイルパス
- ❌ セッション記録
- ❌ 個人識別情報

## フラグ

- `--domain <名前>`: 指定ドメインのみエクスポート
- `--min-confidence <n>`: 最小確信度閾値（デフォルト: 0.3）
- `--output <ファイル>`: 出力ファイルパス（デフォルト: instincts-export-YYYYMMDD.yaml）
- `--format <yaml|json|md>`: 出力フォーマット（デフォルト: yaml）
- `--include-evidence`: エビデンステキストを含める（デフォルト: 除外）
