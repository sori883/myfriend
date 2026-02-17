---
description: Playwrightを使用してE2Eテストを生成・実行する。テストジャーニーの作成、テスト実行、スクリーンショット/動画/トレースのキャプチャ、アーティファクトのアップロード。
---

# E2Eコマンド

このコマンドは**e2e-runner**エージェントを呼び出し、Playwrightを使用したエンドツーエンドテストの生成、保守、実行を行う。

## このコマンドの機能

1. **テストジャーニーの生成** - ユーザーフロー用のPlaywrightテストを作成
2. **E2Eテストの実行** - 複数ブラウザでテストを実行
3. **アーティファクトのキャプチャ** - 失敗時のスクリーンショット、動画、トレース
4. **結果のアップロード** - HTMLレポートとJUnit XML
5. **不安定なテストの特定** - 不安定なテストを隔離

## 使用タイミング

以下の場合に `/e2e` を使用:
- クリティカルなユーザージャーニーのテスト（ログイン、取引、決済）
- マルチステップフローのエンドツーエンド検証
- UIインタラクションとナビゲーションのテスト
- フロントエンドとバックエンドの統合検証
- 本番デプロイメントの準備

## 動作の仕組み

e2e-runnerエージェントが以下を実行:

1. **ユーザーフローを分析** しテストシナリオを特定
2. **Playwrightテストを生成** Page Object Modelパターンを使用
3. **テストを実行** 複数ブラウザで（Chrome、Firefox、Safari）
4. **失敗をキャプチャ** スクリーンショット、動画、トレース付き
5. **レポートを生成** 結果とアーティファクト付き
6. **不安定なテストを特定** し修正を推奨

## 使用例

```
ユーザー: /e2e マーケット検索と閲覧フローをテスト

エージェント（e2e-runner）:
# E2Eテスト生成: マーケット検索と閲覧フロー

## テストシナリオの特定

**ユーザージャーニー:** マーケット検索 → 結果表示 → マーケットクリック → 詳細表示

**テストカバレッジ:**
1. マーケットページに移動
2. セマンティック検索を実行
3. 検索結果を検証
4. 最初の結果をクリック
5. マーケット詳細ページの読み込みを検証
6. チャートのレンダリングを検証

## 生成されたテストコード

```typescript
// tests/e2e/markets/search-and-view.spec.ts
import { test, expect } from '@playwright/test'
import { MarketsPage } from '../../pages/MarketsPage'
import { MarketDetailsPage } from '../../pages/MarketDetailsPage'

test.describe('マーケット検索と閲覧フロー', () => {
  test('ユーザーがマーケットを検索して詳細を表示できる', async ({ page }) => {
    // 1. マーケットページに移動
    const marketsPage = new MarketsPage(page)
    await marketsPage.goto()

    // ページ読み込みを検証
    await expect(page).toHaveTitle(/Markets/)
    await expect(page.locator('h1')).toContainText('Markets')

    // 2. セマンティック検索を実行
    await marketsPage.searchMarkets('election')

    // APIレスポンスを待機
    await page.waitForResponse(resp =>
      resp.url().includes('/api/markets/search') && resp.status() === 200
    )

    // 3. 検索結果を検証
    const marketCards = marketsPage.marketCards
    await expect(marketCards.first()).toBeVisible()
    const resultCount = await marketCards.count()
    expect(resultCount).toBeGreaterThan(0)

    // 検索結果のスクリーンショット
    await page.screenshot({ path: 'artifacts/search-results.png' })

    // 4. 最初の結果をクリック
    const firstMarketTitle = await marketCards.first().textContent()
    await marketCards.first().click()

    // 5. マーケット詳細ページの読み込みを検証
    await expect(page).toHaveURL(/\/markets\/[a-z0-9-]+/)

    const detailsPage = new MarketDetailsPage(page)
    await expect(detailsPage.marketName).toBeVisible()
    await expect(detailsPage.marketDescription).toBeVisible()

    // 6. チャートのレンダリングを検証
    await expect(detailsPage.priceChart).toBeVisible()

    // マーケット名が一致することを検証
    const detailsTitle = await detailsPage.marketName.textContent()
    expect(detailsTitle?.toLowerCase()).toContain(
      firstMarketTitle?.toLowerCase().substring(0, 20) || ''
    )

    // マーケット詳細のスクリーンショット
    await page.screenshot({ path: 'artifacts/market-details.png' })
  })

  test('結果なしの検索で空の状態を表示する', async ({ page }) => {
    const marketsPage = new MarketsPage(page)
    await marketsPage.goto()

    // 存在しないマーケットを検索
    await marketsPage.searchMarkets('xyznonexistentmarket123456')

    // 空の状態を検証
    await expect(page.locator('[data-testid="no-results"]')).toBeVisible()
    await expect(page.locator('[data-testid="no-results"]')).toContainText(
      /no.*results|no.*markets/i
    )

    const marketCount = await marketsPage.marketCards.count()
    expect(marketCount).toBe(0)
  })

  test('検索をクリアして全マーケットを再表示できる', async ({ page }) => {
    const marketsPage = new MarketsPage(page)
    await marketsPage.goto()

    // 初期マーケット数
    const initialCount = await marketsPage.marketCards.count()

    // 検索を実行
    await marketsPage.searchMarkets('trump')
    await page.waitForLoadState('networkidle')

    // フィルタされた結果を検証
    const filteredCount = await marketsPage.marketCards.count()
    expect(filteredCount).toBeLessThan(initialCount)

    // 検索をクリア
    await marketsPage.searchInput.clear()
    await page.waitForLoadState('networkidle')

    // 全マーケットが再表示されることを検証
    const finalCount = await marketsPage.marketCards.count()
    expect(finalCount).toBe(initialCount)
  })
})
```

## テストの実行

```bash
# 生成されたテストを実行
npx playwright test tests/e2e/markets/search-and-view.spec.ts

Running 3 tests using 3 workers

  ✓  [chromium] › search-and-view.spec.ts:5:3 › ユーザーがマーケットを検索して詳細を表示できる (4.2s)
  ✓  [chromium] › search-and-view.spec.ts:52:3 › 結果なしの検索で空の状態を表示する (1.8s)
  ✓  [chromium] › search-and-view.spec.ts:67:3 › 検索をクリアして全マーケットを再表示できる (2.9s)

  3 passed (9.1s)

生成されたアーティファクト:
- artifacts/search-results.png
- artifacts/market-details.png
- playwright-report/index.html
```

## テストレポート

```
╔══════════════════════════════════════════════════════════════╗
║                    E2Eテスト結果                              ║
╠══════════════════════════════════════════════════════════════╣
║ ステータス:     ✅ 全テストパス                                ║
║ 合計:      3テスト                                            ║
║ パス:     3 (100%)                                           ║
║ フェイル:     0                                               ║
║ 不安定:      0                                               ║
║ 所要時間:   9.1s                                             ║
╚══════════════════════════════════════════════════════════════╝

アーティファクト:
📸 スクリーンショット: 2ファイル
📹 動画: 0ファイル（失敗時のみ）
🔍 トレース: 0ファイル（失敗時のみ）
📊 HTMLレポート: playwright-report/index.html

レポートを表示: npx playwright show-report
```

✅ E2Eテストスイートがci/CD統合の準備完了！
```

## テストアーティファクト

テスト実行時に以下のアーティファクトがキャプチャされる:

**全テスト共通:**
- タイムラインと結果を含むHTMLレポート
- CI統合用のJUnit XML

**失敗時のみ:**
- 失敗状態のスクリーンショット
- テストの動画記録
- デバッグ用トレースファイル（ステップごとのリプレイ）
- ネットワークログ
- コンソールログ

## アーティファクトの表示

```bash
# ブラウザでHTMLレポートを表示
npx playwright show-report

# 特定のトレースファイルを表示
npx playwright show-trace artifacts/trace-abc123.zip

# スクリーンショットはartifacts/ディレクトリに保存
open artifacts/search-results.png
```

## 不安定なテストの検出

テストが断続的に失敗する場合:

```
⚠️  不安定なテストを検出: tests/e2e/markets/trade.spec.ts

テストは10回中7回パス（パス率70%）

よくある失敗:
"'[data-testid="confirm-btn"]'要素のタイムアウト待ち"

推奨される修正:
1. 明示的な待機を追加: await page.waitForSelector('[data-testid="confirm-btn"]')
2. タイムアウトを延長: { timeout: 10000 }
3. コンポーネントの競合状態を確認
4. アニメーションで要素が隠れていないか確認

隔離の推奨: 修正まで test.fixme() としてマーク
```

## ブラウザ設定

デフォルトで複数ブラウザでテストを実行:
- ✅ Chromium（デスクトップChrome）
- ✅ Firefox（デスクトップ）
- ✅ WebKit（デスクトップSafari）
- ✅ モバイルChrome（オプション）

ブラウザを調整するには `playwright.config.ts` で設定。

## CI/CD統合

CIパイプラインに追加:

```yaml
# .github/workflows/e2e.yml
- name: Playwrightをインストール
  run: npx playwright install --with-deps

- name: E2Eテストを実行
  run: npx playwright test

- name: アーティファクトをアップロード
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: playwright-report
    path: playwright-report/
```

## PMX固有のクリティカルフロー

PMXでは、以下のE2Eテストを優先:

**🔴 CRITICAL（常にパス必須）:**
1. ユーザーがウォレットを接続できる
2. ユーザーがマーケットを閲覧できる
3. ユーザーがマーケットを検索できる（セマンティック検索）
4. ユーザーがマーケット詳細を表示できる
5. ユーザーが取引を行える（テスト資金で）
6. マーケットが正しく解決される
7. ユーザーが資金を引き出せる

**🟡 IMPORTANT:**
1. マーケット作成フロー
2. ユーザープロフィール更新
3. リアルタイム価格更新
4. チャートレンダリング
5. マーケットのフィルタとソート
6. モバイルレスポンシブレイアウト

## ベストプラクティス

**推奨:**
- ✅ メンテナンス性のためPage Object Modelを使用
- ✅ セレクタにdata-testid属性を使用
- ✅ 任意のタイムアウトではなくAPIレスポンスを待機
- ✅ クリティカルなユーザージャーニーをエンドツーエンドでテスト
- ✅ mainへのマージ前にテストを実行
- ✅ テスト失敗時にアーティファクトを確認

**非推奨:**
- ❌ 脆いセレクタの使用（CSSクラスは変更される可能性あり）
- ❌ 実装の詳細をテスト
- ❌ 本番環境に対してテストを実行
- ❌ 不安定なテストを無視
- ❌ 失敗時のアーティファクト確認をスキップ
- ❌ 全てのエッジケースをE2Eでテスト（ユニットテストを使用）

## 重要な注意事項

**PMX用のCRITICAL:**
- 実際のお金に関わるE2Eテストはテストネット/ステージング環境でのみ実行すること
- 本番環境に対して取引テストを実行しないこと
- 金融テストには `test.skip(process.env.NODE_ENV === 'production')` を設定
- 少額のテスト資金のみのテストウォレットを使用

## 他のコマンドとの連携

- `/plan` でテストすべきクリティカルなジャーニーを特定
- `/tdd` でユニットテスト（より高速、より細粒度）
- `/e2e` で統合テストとユーザージャーニーテスト
- `/code-review` でテスト品質を検証

## 関連エージェント

このコマンドは以下にある `e2e-runner` エージェントを呼び出す:
`~/.claude/agents/e2e-runner.md`

## クイックコマンド

```bash
# 全E2Eテストを実行
npx playwright test

# 特定のテストファイルを実行
npx playwright test tests/e2e/markets/search.spec.ts

# ヘッドモードで実行（ブラウザを表示）
npx playwright test --headed

# テストをデバッグ
npx playwright test --debug

# テストコードを生成
npx playwright codegen http://localhost:3000

# レポートを表示
npx playwright show-report
```
