---
description: テスト駆動開発ワークフローを強制する。インターフェースのスキャフォールド、テストを先に生成、次に最小限のコードで実装。80%以上のカバレッジを確保。
---

# TDDコマンド

このコマンドは**tdd-guide**エージェントを呼び出し、テスト駆動開発の方法論を強制する。

## このコマンドの機能

1. **インターフェースのスキャフォールド** - 型/インターフェースを最初に定義
2. **テストを先に生成** - 失敗するテストを書く（RED）
3. **最小限のコードで実装** - パスするのに十分なコードのみ書く（GREEN）
4. **リファクタリング** - テストをグリーンに保ちながらコードを改善（REFACTOR）
5. **カバレッジを検証** - 80%以上のテストカバレッジを確保

## 使用タイミング

以下の場合に `/tdd` を使用:
- 新機能の実装
- 新しい関数/コンポーネントの追加
- バグ修正（まずバグを再現するテストを書く）
- 既存コードのリファクタリング
- クリティカルなビジネスロジックの構築

## 動作の仕組み

tdd-guideエージェントが以下を実行:

1. **インターフェースを定義** 入出力用
2. **失敗するテストを書く** （コードがまだ存在しないため）
3. **テストを実行** し正しい理由で失敗することを確認
4. **最小限の実装を書く** テストをパスさせるため
5. **テストを実行** しパスすることを確認
6. **コードをリファクタリング** テストをグリーンに保ちながら
7. **カバレッジを確認** 80%未満ならテストを追加

## TDDサイクル

```
RED → GREEN → REFACTOR → REPEAT

RED:      失敗するテストを書く
GREEN:    パスするための最小限のコードを書く
REFACTOR: テストをパスさせたままコードを改善
REPEAT:   次の機能/シナリオへ
```

## 使用例

```
ユーザー: /tdd マーケット流動性スコアを計算する関数が必要

エージェント（tdd-guide）:
# TDDセッション: マーケット流動性スコア計算機

## ステップ1: インターフェースの定義（SCAFFOLD）

```typescript
// lib/liquidity.ts
export interface MarketData {
  totalVolume: number
  bidAskSpread: number
  activeTraders: number
  lastTradeTime: Date
}

export function calculateLiquidityScore(market: MarketData): number {
  // TODO: 実装
  throw new Error('Not implemented')
}
```

## ステップ2: 失敗するテストを書く（RED）

```typescript
// lib/liquidity.test.ts
import { calculateLiquidityScore } from './liquidity'

describe('calculateLiquidityScore', () => {
  it('流動性の高いマーケットで高スコアを返すべき', () => {
    const market = {
      totalVolume: 100000,
      bidAskSpread: 0.01,
      activeTraders: 500,
      lastTradeTime: new Date()
    }

    const score = calculateLiquidityScore(market)

    expect(score).toBeGreaterThan(80)
    expect(score).toBeLessThanOrEqual(100)
  })

  it('流動性の低いマーケットで低スコアを返すべき', () => {
    const market = {
      totalVolume: 100,
      bidAskSpread: 0.5,
      activeTraders: 2,
      lastTradeTime: new Date(Date.now() - 86400000) // 1日前
    }

    const score = calculateLiquidityScore(market)

    expect(score).toBeLessThan(30)
    expect(score).toBeGreaterThanOrEqual(0)
  })

  it('エッジケース: ゼロボリュームを処理するべき', () => {
    const market = {
      totalVolume: 0,
      bidAskSpread: 0,
      activeTraders: 0,
      lastTradeTime: new Date()
    }

    const score = calculateLiquidityScore(market)

    expect(score).toBe(0)
  })
})
```

## ステップ3: テスト実行 - 失敗を確認

```bash
npm test lib/liquidity.test.ts

FAIL lib/liquidity.test.ts
  ✕ 流動性の高いマーケットで高スコアを返すべき (2 ms)
    Error: Not implemented

1 test failed, 0 passed
```

✅ テストが期待通りに失敗。実装準備完了。

## ステップ4: 最小限のコードで実装（GREEN）

```typescript
// lib/liquidity.ts
export function calculateLiquidityScore(market: MarketData): number {
  // ゼロボリュームのエッジケースを処理
  if (market.totalVolume === 0) {
    return 0
  }

  // コンポーネントスコアを計算（0-100スケール）
  const volumeScore = Math.min(market.totalVolume / 1000, 100)
  const spreadScore = Math.max(100 - (market.bidAskSpread * 1000), 0)
  const traderScore = Math.min(market.activeTraders / 10, 100)

  // 直近のアクティビティボーナス
  const hoursSinceLastTrade = (Date.now() - market.lastTradeTime.getTime()) / (1000 * 60 * 60)
  const recencyScore = Math.max(100 - (hoursSinceLastTrade * 10), 0)

  // 加重平均
  const score = (
    volumeScore * 0.4 +
    spreadScore * 0.3 +
    traderScore * 0.2 +
    recencyScore * 0.1
  )

  return Math.min(Math.max(score, 0), 100) // 0-100にクランプ
}
```

## ステップ5: テスト実行 - パスを確認

```bash
npm test lib/liquidity.test.ts

PASS lib/liquidity.test.ts
  ✓ 流動性の高いマーケットで高スコアを返すべき (3 ms)
  ✓ 流動性の低いマーケットで低スコアを返すべき (2 ms)
  ✓ エッジケース: ゼロボリュームを処理するべき (1 ms)

3 tests passed
```

✅ 全テストパス！

## ステップ6: リファクタリング（IMPROVE）

```typescript
// lib/liquidity.ts - 定数と可読性を改善してリファクタリング
const WEIGHTS = {
  VOLUME: 0.4,
  SPREAD: 0.3,
  TRADERS: 0.2,
  RECENCY: 0.1,
} as const

const SCALE_FACTORS = {
  VOLUME: 1000,
  SPREAD: 1000,
  TRADERS: 10,
  RECENCY_PENALTY: 10,
} as const

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

export function calculateLiquidityScore(market: MarketData): number {
  if (market.totalVolume === 0) return 0

  const volumeScore = Math.min(market.totalVolume / SCALE_FACTORS.VOLUME, 100)
  const spreadScore = clamp(100 - (market.bidAskSpread * SCALE_FACTORS.SPREAD), 0, 100)
  const traderScore = Math.min(market.activeTraders / SCALE_FACTORS.TRADERS, 100)

  const hoursSinceLastTrade = (Date.now() - market.lastTradeTime.getTime()) / (1000 * 60 * 60)
  const recencyScore = clamp(100 - (hoursSinceLastTrade * SCALE_FACTORS.RECENCY_PENALTY), 0, 100)

  const weightedScore =
    volumeScore * WEIGHTS.VOLUME +
    spreadScore * WEIGHTS.SPREAD +
    traderScore * WEIGHTS.TRADERS +
    recencyScore * WEIGHTS.RECENCY

  return clamp(weightedScore, 0, 100)
}
```

## ステップ7: テストが引き続きパスすることを確認

```bash
npm test lib/liquidity.test.ts

PASS lib/liquidity.test.ts
  ✓ 流動性の高いマーケットで高スコアを返すべき (3 ms)
  ✓ 流動性の低いマーケットで低スコアを返すべき (2 ms)
  ✓ エッジケース: ゼロボリュームを処理するべき (1 ms)

3 tests passed
```

✅ リファクタリング完了、テストは引き続きパス！

## ステップ8: カバレッジの確認

```bash
npm test -- --coverage lib/liquidity.test.ts

File           | % Stmts | % Branch | % Funcs | % Lines
---------------|---------|----------|---------|--------
liquidity.ts   |   100   |   100    |   100   |   100

カバレッジ: 100% ✅（目標: 80%）
```

✅ TDDセッション完了！
```

## TDDベストプラクティス

**推奨:**
- ✅ 実装の前にまずテストを書く
- ✅ 実装前にテストを実行し失敗を確認
- ✅ テストをパスさせるための最小限のコードを書く
- ✅ テストがグリーンになってからリファクタリング
- ✅ エッジケースとエラーシナリオを追加
- ✅ 80%以上のカバレッジを目標（クリティカルなコードは100%）

**非推奨:**
- ❌ テスト前に実装を書く
- ❌ 各変更後のテスト実行をスキップ
- ❌ 一度に大量のコードを書く
- ❌ 失敗するテストを無視
- ❌ 実装の詳細をテスト（振る舞いをテストすること）
- ❌ 全てをモック（統合テストを優先）

## 含めるべきテストタイプ

**ユニットテスト**（関数レベル）:
- 正常系シナリオ
- エッジケース（空、null、最大値）
- エラー条件
- 境界値

**統合テスト**（コンポーネントレベル）:
- APIエンドポイント
- データベース操作
- 外部サービス呼び出し
- フック付きReactコンポーネント

**E2Eテスト**（`/e2e` コマンドを使用）:
- クリティカルなユーザーフロー
- マルチステッププロセス
- フルスタック統合

## カバレッジ要件

- **最低80%** 全コードに対して
- **100%必須** 以下のコードに対して:
  - 金融計算
  - 認証ロジック
  - セキュリティクリティカルなコード
  - コアビジネスロジック

## 重要な注意事項

**MANDATORY**: テストは実装の前に書くこと。TDDサイクルは:

1. **RED** - 失敗するテストを書く
2. **GREEN** - パスするよう実装
3. **REFACTOR** - コードを改善

REDフェーズを絶対にスキップしないこと。テスト前にコードを書かないこと。

## 他のコマンドとの連携

- `/plan` で最初に何を構築するか理解
- `/tdd` でテスト付きで実装
- `/build-and-fix` でビルドエラーが発生した場合の対応
- `/code-review` で実装のレビュー
- `/test-coverage` でカバレッジの検証

## 関連エージェント

このコマンドは以下にある `tdd-guide` エージェントを呼び出す:
`~/.claude/agents/tdd-guide.md`

参照可能な `tdd-workflow` スキル:
`~/.claude/skills/tdd-workflow/`
