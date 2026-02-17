# 検証ループスキル

Claude Codeセッション用の包括的な検証システム。

## 使用タイミング

以下の場合にこのスキルを呼び出す:
- 機能や重要なコード変更の完了後
- PR作成前
- 品質ゲートの通過を確認したい時
- リファクタリング後

## 検証フェーズ

### フェーズ1: ビルド検証
```bash
# プロジェクトがビルドできるか確認
npm run build 2>&1 | tail -20
# または
pnpm build 2>&1 | tail -20
```

ビルドが失敗した場合、続行前にSTOPして修正する。

### フェーズ2: 型チェック
```bash
# TypeScriptプロジェクト
npx tsc --noEmit 2>&1 | head -30

# Pythonプロジェクト
pyright . 2>&1 | head -30
```

全ての型エラーを報告する。続行前にクリティカルなものを修正する。

### フェーズ3: Lintチェック
```bash
# JavaScript/TypeScript
npm run lint 2>&1 | head -30

# Python
ruff check . 2>&1 | head -30
```

### フェーズ4: テストスイート
```bash
# カバレッジ付きでテストを実行
npm run test -- --coverage 2>&1 | tail -50

# カバレッジ閾値を確認
# 目標: 最低80%
```

レポート:
- 総テスト数: X
- パス: X
- フェイル: X
- カバレッジ: X%

### フェーズ5: セキュリティスキャン
```bash
# シークレットのチェック
grep -rn "sk-" --include="*.ts" --include="*.js" . 2>/dev/null | head -10
grep -rn "api_key" --include="*.ts" --include="*.js" . 2>/dev/null | head -10

# console.logのチェック
grep -rn "console.log" --include="*.ts" --include="*.tsx" src/ 2>/dev/null | head -10
```

### フェーズ6: 差分レビュー
```bash
# 変更内容を表示
git diff --stat
git diff HEAD~1 --name-only
```

変更されたファイルごとに以下をレビュー:
- 意図しない変更
- エラーハンドリングの欠落
- 潜在的なエッジケース

## 出力フォーマット

全フェーズ実行後、検証レポートを作成:

```
検証レポート
==================

ビルド:     [PASS/FAIL]
型:         [PASS/FAIL] (Xエラー)
Lint:       [PASS/FAIL] (X警告)
テスト:     [PASS/FAIL] (X/Yパス、Z%カバレッジ)
セキュリティ: [PASS/FAIL] (X件の問題)
差分:       [Xファイル変更]

総合:       PR[準備完了/未完了]

修正すべき問題:
1. ...
2. ...
```

## 継続モード

長時間セッションでは、15分ごとまたは大きな変更後に検証を実行:

```markdown
メンタルチェックポイントを設定:
- 各関数の完了後
- コンポーネントの完成後
- 次のタスクに移る前

実行: /verify
```

## フックとの統合

このスキルはPostToolUseフックを補完するが、より深い検証を提供する。
フックは問題を即座にキャッチし、このスキルは包括的なレビューを提供する。
