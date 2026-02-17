---
name: eval-harness
description: Claude Codeセッション用の形式的な評価フレームワーク。eval駆動開発（EDD）の原則を実装する。
tools: Read, Write, Edit, Bash, Grep, Glob
---

# Evalハーネススキル

Claude Codeセッション用の形式的な評価フレームワーク。eval駆動開発（EDD）の原則を実装する。

## 哲学

Eval駆動開発はevalを「AI開発のユニットテスト」として扱う:
- 実装の前に期待する動作を定義
- 開発中にevalを継続的に実行
- 各変更でリグレッションを追跡
- 信頼性測定にpass@kメトリクスを使用

## Evalの種類

### ケイパビリティEval
Claudeが以前できなかったことができるかテスト:
```markdown
[CAPABILITY EVAL: 機能名]
タスク: Claudeが達成すべき内容の説明
成功基準:
  - [ ] 基準1
  - [ ] 基準2
  - [ ] 基準3
期待される出力: 期待される結果の説明
```

### リグレッションEval
変更が既存の機能を壊さないことを確認:
```markdown
[REGRESSION EVAL: 機能名]
ベースライン: SHAまたはチェックポイント名
テスト:
  - 既存テスト1: PASS/FAIL
  - 既存テスト2: PASS/FAIL
  - 既存テスト3: PASS/FAIL
結果: X/Yパス（以前はY/Y）
```

## グレーダーの種類

### 1. コードベースのグレーダー
コードを使った決定的チェック:
```bash
# ファイルに期待するパターンが含まれているか確認
grep -q "export function handleAuth" src/auth.ts && echo "PASS" || echo "FAIL"

# テストがパスするか確認
npm test -- --testPathPattern="auth" && echo "PASS" || echo "FAIL"

# ビルドが成功するか確認
npm run build && echo "PASS" || echo "FAIL"
```

### 2. モデルベースのグレーダー
Claudeを使ってオープンエンドの出力を評価:
```markdown
[モデルグレーダープロンプト]
以下のコード変更を評価:
1. 述べられた問題を解決しているか？
2. 適切に構造化されているか？
3. エッジケースは処理されているか？
4. エラーハンドリングは適切か？

スコア: 1-5（1=不良、5=優秀）
理由: [説明]
```

### 3. ヒューマングレーダー
手動レビュー用にフラグ:
```markdown
[人間のレビューが必要]
変更: 変更内容の説明
理由: 人間のレビューが必要な理由
リスクレベル: LOW/MEDIUM/HIGH
```

## メトリクス

### pass@k
「k回の試行で少なくとも1回成功」
- pass@1: 初回試行の成功率
- pass@3: 3回以内の成功
- 典型的な目標: pass@3 > 90%

### pass^k
「k回の試行全てが成功」
- 信頼性のより高い基準
- pass^3: 3回連続成功
- クリティカルパスに使用

## Evalワークフロー

### 1. 定義（コーディング前）
```markdown
## EVAL定義: feature-xyz

### ケイパビリティEval
1. 新しいユーザーアカウントを作成できる
2. メールフォーマットを検証できる
3. パスワードを安全にハッシュ化できる

### リグレッションEval
1. 既存のログインが引き続き動作する
2. セッション管理に変更なし
3. ログアウトフローが正常

### 成功メトリクス
- ケイパビリティevalのpass@3 > 90%
- リグレッションevalのpass^3 = 100%
```

### 2. 実装
定義されたevalをパスするコードを書く。

### 3. 評価
```bash
# ケイパビリティevalを実行
[各ケイパビリティevalを実行し、PASS/FAILを記録]

# リグレッションevalを実行
npm test -- --testPathPattern="existing"

# レポートを生成
```

### 4. レポート
```markdown
EVALレポート: feature-xyz
========================

ケイパビリティEval:
  create-user:     PASS (pass@1)
  validate-email:  PASS (pass@2)
  hash-password:   PASS (pass@1)
  全体:            3/3パス

リグレッションEval:
  login-flow:      PASS
  session-mgmt:    PASS
  logout-flow:     PASS
  全体:            3/3パス

メトリクス:
  pass@1: 67% (2/3)
  pass@3: 100% (3/3)

ステータス: レビュー準備完了
```

## 統合パターン

### 実装前
```
/eval define 機能名
```
`.claude/evals/機能名.md` にeval定義ファイルを作成

### 実装中
```
/eval check 機能名
```
現在のevalを実行しステータスを報告

### 実装後
```
/eval report 機能名
```
完全なevalレポートを生成

## Evalの保存

プロジェクト内にevalを保存:
```
.claude/
  evals/
    feature-xyz.md      # Eval定義
    feature-xyz.log     # Eval実行履歴
    baseline.json       # リグレッションベースライン
```

## ベストプラクティス

1. **コーディング前にevalを定義** - 成功基準について明確に考えることを強制
2. **evalを頻繁に実行** - リグレッションを早期にキャッチ
3. **pass@kを経時的に追跡** - 信頼性の傾向を監視
4. **可能な限りコードグレーダーを使用** - 決定的 > 確率的
5. **セキュリティは人間がレビュー** - セキュリティチェックを完全に自動化しない
6. **evalを高速に保つ** - 遅いevalは実行されない
7. **evalをコードとバージョン管理** - evalはファーストクラスの成果物

## 例: 認証の追加

```markdown
## EVAL: add-authentication

### フェーズ1: 定義（10分）
ケイパビリティEval:
- [ ] ユーザーがメール/パスワードで登録できる
- [ ] ユーザーが有効な認証情報でログインできる
- [ ] 無効な認証情報が適切なエラーで拒否される
- [ ] セッションがページリロード後も維持される
- [ ] ログアウトでセッションがクリアされる

リグレッションEval:
- [ ] パブリックルートに引き続きアクセス可能
- [ ] APIレスポンスに変更なし
- [ ] データベーススキーマが互換

### フェーズ2: 実装（可変）
[コードを書く]

### フェーズ3: 評価
実行: /eval check add-authentication

### フェーズ4: レポート
EVALレポート: add-authentication
==============================
ケイパビリティ: 5/5パス (pass@3: 100%)
リグレッション: 3/3パス (pass^3: 100%)
ステータス: リリース可
```
