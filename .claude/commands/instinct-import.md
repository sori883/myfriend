---
name: instinct-import
description: チームメイト、Skill Creator、その他のソースからインスティンクトをインポートする
command: true
---

# インスティンクトインポートコマンド

## 実装

プラグインルートパスを使用してインスティンクトCLIを実行:

```bash
python3 "${CLAUDE_PLUGIN_ROOT}/skills/continuous-learning-v2/scripts/instinct-cli.py" import <file-or-url> [--dry-run] [--force] [--min-confidence 0.7]
```

`CLAUDE_PLUGIN_ROOT` が設定されていない場合（手動インストール）:

```bash
python3 ~/.claude/skills/continuous-learning-v2/scripts/instinct-cli.py import <file-or-url>
```

以下からインスティンクトをインポート:
- チームメイトのエクスポート
- Skill Creator（リポジトリ分析）
- コミュニティコレクション
- 以前のマシンのバックアップ

## 使い方

```
/instinct-import team-instincts.yaml
/instinct-import https://github.com/org/repo/instincts.yaml
/instinct-import --from-skill-creator acme/webapp
```

## 実行内容

1. インスティンクトファイルを取得（ローカルパスまたはURL）
2. フォーマットを解析・検証
3. 既存インスティンクトとの重複をチェック
4. 新しいインスティンクトをマージまたは追加
5. `~/.claude/homunculus/instincts/inherited/` に保存

## インポートプロセス

```
📥 インスティンクトをインポート中: team-instincts.yaml
================================================

12件のインスティンクトが見つかりました。

競合を分析中...

## 新規インスティンクト（8件）
以下が追加されます:
  ✓ use-zod-validation（確信度: 0.7）
  ✓ prefer-named-exports（確信度: 0.65）
  ✓ test-async-functions（確信度: 0.8）
  ...

## 重複インスティンクト（3件）
類似のインスティンクトが既に存在:
  ⚠️ prefer-functional-style
     ローカル: 確信度0.8、12回の観察
     インポート: 確信度0.7
     → ローカルを保持（確信度が高い）

  ⚠️ test-first-workflow
     ローカル: 確信度0.75
     インポート: 確信度0.9
     → インポートに更新（確信度が高い）

## 競合インスティンクト（1件）
ローカルインスティンクトと矛盾:
  ❌ use-classes-for-services
     競合対象: avoid-classes
     → スキップ（手動解決が必要）

---
8件を新規追加、1件を更新、3件をスキップしますか？
```

## マージ戦略

### 重複の場合
既存のインスティンクトと一致するインスティンクトをインポートする場合:
- **確信度が高い方が優先**: 確信度の高い方を保持
- **エビデンスを統合**: 観察回数を合算
- **タイムスタンプを更新**: 最近検証済みとしてマーク

### 競合の場合
既存のインスティンクトと矛盾するインスティンクトをインポートする場合:
- **デフォルトでスキップ**: 競合するインスティンクトはインポートしない
- **レビュー用にフラグ**: 両方に注意が必要とマーク
- **手動解決**: ユーザーがどちらを保持するか決定

## ソース追跡

インポートされたインスティンクトには以下がマークされる:
```yaml
source: "inherited"
imported_from: "team-instincts.yaml"
imported_at: "2025-01-22T10:30:00Z"
original_source: "session-observation"  # または "repo-analysis"
```

## Skill Creator統合

Skill Creatorからインポートする場合:

```
/instinct-import --from-skill-creator acme/webapp
```

リポジトリ分析から生成されたインスティンクトを取得:
- ソース: `repo-analysis`
- 初期確信度が高い（0.7以上）
- ソースリポジトリにリンク

## フラグ

- `--dry-run`: インポートせずにプレビュー
- `--force`: 競合があってもインポート
- `--merge-strategy <higher|local|import>`: 重複の処理方法
- `--from-skill-creator <owner/repo>`: Skill Creator分析からインポート
- `--min-confidence <n>`: 閾値以上のインスティンクトのみインポート

## 出力

インポート後:
```
✅ インポート完了！

追加: 8インスティンクト
更新: 1インスティンクト
スキップ: 3インスティンクト（2重複、1競合）

新しいインスティンクトの保存先: ~/.claude/homunculus/instincts/inherited/

全インスティンクトを確認するには /instinct-status を実行してください。
```
