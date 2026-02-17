---
description: お好みのパッケージマネージャー（npm/pnpm/yarn/bun）を設定する
disable-model-invocation: true
---

# パッケージマネージャーのセットアップ

このプロジェクトまたはグローバルにお好みのパッケージマネージャーを設定する。

## 使い方

```bash
# 現在のパッケージマネージャーを検出
node scripts/setup-package-manager.js --detect

# グローバル設定を変更
node scripts/setup-package-manager.js --global pnpm

# プロジェクト設定を変更
node scripts/setup-package-manager.js --project bun

# 利用可能なパッケージマネージャーを一覧表示
node scripts/setup-package-manager.js --list
```

## 検出の優先順位

使用するパッケージマネージャーを決定する際、以下の順序でチェックされる:

1. **環境変数**: `CLAUDE_PACKAGE_MANAGER`
2. **プロジェクト設定**: `.claude/package-manager.json`
3. **package.json**: `packageManager` フィールド
4. **ロックファイル**: package-lock.json、yarn.lock、pnpm-lock.yaml、bun.lockb の存在
5. **グローバル設定**: `~/.claude/package-manager.json`
6. **フォールバック**: 最初に利用可能なパッケージマネージャー（pnpm > bun > yarn > npm）

## 設定ファイル

### グローバル設定
```json
// ~/.claude/package-manager.json
{
  "packageManager": "pnpm"
}
```

### プロジェクト設定
```json
// .claude/package-manager.json
{
  "packageManager": "bun"
}
```

### package.json
```json
{
  "packageManager": "pnpm@8.6.0"
}
```

## 環境変数

`CLAUDE_PACKAGE_MANAGER` を設定すると、他の全ての検出方法をオーバーライドする:

```bash
# Windows (PowerShell)
$env:CLAUDE_PACKAGE_MANAGER = "pnpm"

# macOS/Linux
export CLAUDE_PACKAGE_MANAGER=pnpm
```

## 検出の実行

現在のパッケージマネージャー検出結果を確認するには:

```bash
node scripts/setup-package-manager.js --detect
```
