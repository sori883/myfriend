---
name: tdd-workflow
description: 新機能の作成、バグ修正、コードのリファクタリング時にこのスキルを使用する。ユニット、統合、E2Eテストを含む80%以上のカバレッジでテスト駆動開発を強制する。
---

# テスト駆動開発ワークフロー

このスキルは全てのコード開発がTDDの原則に従い、包括的なテストカバレッジを確保する。

## 発動タイミング

- 新しい機能やfunctionalityの作成時
- バグや問題の修正時
- 既存コードのリファクタリング時
- APIエンドポイントの追加時
- 新しいコンポーネントの作成時

## 基本原則

### 1. テストをコードの前に
常にテストを先に書き、テストをパスするコードを実装する。

### 2. カバレッジ要件
- 最低80%カバレッジ（ユニット + 統合 + E2E）
- 全エッジケースをカバー
- エラーシナリオをテスト
- 境界条件を検証

### 3. テストの種類

#### ユニットテスト
- 個別の関数とユーティリティ
- コンポーネントロジック
- 純粋関数
- ヘルパーとユーティリティ

#### 統合テスト
- APIエンドポイント
- データベース操作
- サービス間連携
- 外部APIコール

#### E2Eテスト（Playwright）
- クリティカルなユーザーフロー
- 完全なワークフロー
- ブラウザ自動化
- UIインタラクション

## TDDワークフローステップ

### ステップ1: ユーザージャーニーを書く
```
[役割]として、[アクション]をしたい。なぜなら[利益]だから。

例:
ユーザーとして、マーケットをセマンティック検索したい。
なぜなら正確なキーワードがなくても関連するマーケットを見つけられるから。
```

### ステップ2: テストケースを生成
各ユーザージャーニーに対して包括的なテストケースを作成:

```typescript
describe('Semantic Search', () => {
  it('クエリに対して関連するマーケットを返す', async () => {
    // テスト実装
  })

  it('空のクエリを適切に処理する', async () => {
    // エッジケースのテスト
  })

  it('Redis利用不可時にサブストリング検索にフォールバックする', async () => {
    // フォールバック動作のテスト
  })

  it('類似度スコアで結果をソートする', async () => {
    // ソートロジックのテスト
  })
})
```

### ステップ3: テストを実行（失敗するはず）
```bash
npm test
# テストは失敗するはず - まだ実装していないため
```

### ステップ4: コードを実装
テストをパスさせるための最小限のコードを書く:

```typescript
// テストに導かれた実装
export async function searchMarkets(query: string) {
  // ここに実装
}
```

### ステップ5: テストを再実行
```bash
npm test
# テストがパスするはず
```

### ステップ6: リファクタリング
テストをグリーンに保ちながらコード品質を改善:
- 重複を除去
- 命名を改善
- パフォーマンスを最適化
- 可読性を向上

### ステップ7: カバレッジを検証
```bash
npm run test:coverage
# 80%以上のカバレッジを確認
```

## テストパターン

### ユニットテストパターン（Jest/Vitest）
```typescript
import { render, screen, fireEvent } from '@testing-library/react'
import { Button } from './Button'

describe('Buttonコンポーネント', () => {
  it('正しいテキストでレンダリングする', () => {
    render(<Button>Click me</Button>)
    expect(screen.getByText('Click me')).toBeInTheDocument()
  })

  it('クリック時にonClickを呼び出す', () => {
    const handleClick = jest.fn()
    render(<Button onClick={handleClick}>Click</Button>)

    fireEvent.click(screen.getByRole('button'))

    expect(handleClick).toHaveBeenCalledTimes(1)
  })

  it('disabledプロップがtrueの時に無効になる', () => {
    render(<Button disabled>Click</Button>)
    expect(screen.getByRole('button')).toBeDisabled()
  })
})
```

### API統合テストパターン
```typescript
import { NextRequest } from 'next/server'
import { GET } from './route'

describe('GET /api/markets', () => {
  it('マーケットを正常に返す', async () => {
    const request = new NextRequest('http://localhost/api/markets')
    const response = await GET(request)
    const data = await response.json()

    expect(response.status).toBe(200)
    expect(data.success).toBe(true)
    expect(Array.isArray(data.data)).toBe(true)
  })

  it('クエリパラメータをバリデーションする', async () => {
    const request = new NextRequest('http://localhost/api/markets?limit=invalid')
    const response = await GET(request)

    expect(response.status).toBe(400)
  })

  it('データベースエラーを適切に処理する', async () => {
    // データベース失敗をモック
    const request = new NextRequest('http://localhost/api/markets')
    // エラーハンドリングのテスト
  })
})
```

### E2Eテストパターン（Playwright）
```typescript
import { test, expect } from '@playwright/test'

test('ユーザーがマーケットを検索してフィルタリングできる', async ({ page }) => {
  // マーケットページに移動
  await page.goto('/')
  await page.click('a[href="/markets"]')

  // ページ読み込みを確認
  await expect(page.locator('h1')).toContainText('Markets')

  // マーケットを検索
  await page.fill('input[placeholder="Search markets"]', 'election')

  // デバウンスと結果を待機
  await page.waitForTimeout(600)

  // 検索結果の表示を確認
  const results = page.locator('[data-testid="market-card"]')
  await expect(results).toHaveCount(5, { timeout: 5000 })

  // 結果に検索語が含まれることを確認
  const firstResult = results.first()
  await expect(firstResult).toContainText('election', { ignoreCase: true })

  // ステータスでフィルタ
  await page.click('button:has-text("Active")')

  // フィルタ結果を確認
  await expect(results).toHaveCount(3)
})

test('ユーザーが新しいマーケットを作成できる', async ({ page }) => {
  // まずログイン
  await page.goto('/creator-dashboard')

  // マーケット作成フォームに入力
  await page.fill('input[name="name"]', 'Test Market')
  await page.fill('textarea[name="description"]', 'Test description')
  await page.fill('input[name="endDate"]', '2025-12-31')

  // フォームを送信
  await page.click('button[type="submit"]')

  // 成功メッセージを確認
  await expect(page.locator('text=Market created successfully')).toBeVisible()

  // マーケットページへのリダイレクトを確認
  await expect(page).toHaveURL(/\/markets\/test-market/)
})
```

## テストファイルの構成

```
src/
├── components/
│   ├── Button/
│   │   ├── Button.tsx
│   │   ├── Button.test.tsx          # ユニットテスト
│   │   └── Button.stories.tsx       # Storybook
│   └── MarketCard/
│       ├── MarketCard.tsx
│       └── MarketCard.test.tsx
├── app/
│   └── api/
│       └── markets/
│           ├── route.ts
│           └── route.test.ts         # 統合テスト
└── e2e/
    ├── markets.spec.ts               # E2Eテスト
    ├── trading.spec.ts
    └── auth.spec.ts
```

## 外部サービスのモック

### Supabaseモック
```typescript
jest.mock('@/lib/supabase', () => ({
  supabase: {
    from: jest.fn(() => ({
      select: jest.fn(() => ({
        eq: jest.fn(() => Promise.resolve({
          data: [{ id: 1, name: 'Test Market' }],
          error: null
        }))
      }))
    }))
  }
}))
```

### Redisモック
```typescript
jest.mock('@/lib/redis', () => ({
  searchMarketsByVector: jest.fn(() => Promise.resolve([
    { slug: 'test-market', similarity_score: 0.95 }
  ])),
  checkRedisHealth: jest.fn(() => Promise.resolve({ connected: true }))
}))
```

### OpenAIモック
```typescript
jest.mock('@/lib/openai', () => ({
  generateEmbedding: jest.fn(() => Promise.resolve(
    new Array(1536).fill(0.1) // 1536次元のモック埋め込み
  ))
}))
```

## テストカバレッジの検証

### カバレッジレポートの実行
```bash
npm run test:coverage
```

### カバレッジ閾値
```json
{
  "jest": {
    "coverageThresholds": {
      "global": {
        "branches": 80,
        "functions": 80,
        "lines": 80,
        "statements": 80
      }
    }
  }
}
```

## 避けるべきよくあるテストの間違い

### ❌ 間違い: 実装の詳細をテスト
```typescript
// 内部状態をテストしない
expect(component.state.count).toBe(5)
```

### ✅ 正しい: ユーザーに見える動作をテスト
```typescript
// ユーザーが見るものをテスト
expect(screen.getByText('Count: 5')).toBeInTheDocument()
```

### ❌ 間違い: 脆いセレクタ
```typescript
// 壊れやすい
await page.click('.css-class-xyz')
```

### ✅ 正しい: セマンティックセレクタ
```typescript
// 変更に強い
await page.click('button:has-text("Submit")')
await page.click('[data-testid="submit-button"]')
```

### ❌ 間違い: テストの独立性がない
```typescript
// テストが相互依存している
test('ユーザーを作成する', () => { /* ... */ })
test('同じユーザーを更新する', () => { /* 前のテストに依存 */ })
```

### ✅ 正しい: 独立したテスト
```typescript
// 各テストが独自のデータを設定
test('ユーザーを作成する', () => {
  const user = createTestUser()
  // テストロジック
})

test('ユーザーを更新する', () => {
  const user = createTestUser()
  // 更新ロジック
})
```

## 継続的テスト

### 開発中のウォッチモード
```bash
npm test -- --watch
# ファイル変更時にテストが自動実行
```

### プリコミットフック
```bash
# 毎回のコミット前に実行
npm test && npm run lint
```

### CI/CD統合
```yaml
# GitHub Actions
- name: Run Tests
  run: npm test -- --coverage
- name: Upload Coverage
  uses: codecov/codecov-action@v3
```

## ベストプラクティス

1. **テストを先に書く** - 常にTDD
2. **1テスト1アサーション** - 単一の動作に焦点
3. **説明的なテスト名** - テスト内容を説明
4. **Arrange-Act-Assert** - 明確なテスト構造
5. **外部依存をモック** - ユニットテストを分離
6. **エッジケースをテスト** - null、undefined、空、大きな値
7. **エラーパスをテスト** - ハッピーパスだけでない
8. **テストを高速に保つ** - ユニットテストは各50ms未満
9. **テスト後にクリーンアップ** - 副作用なし
10. **カバレッジレポートをレビュー** - ギャップを特定

## 成功メトリクス

- 80%以上のコードカバレッジを達成
- 全テストがパス（グリーン）
- スキップまたは無効化されたテストなし
- 高速なテスト実行（ユニットテストは30秒未満）
- E2Eテストがクリティカルなユーザーフローをカバー
- テストが本番前にバグをキャッチ

---

**注意**: テストはオプションではない。テストは自信を持ったリファクタリング、迅速な開発、プロダクションの信頼性を可能にするセーフティネットである。
