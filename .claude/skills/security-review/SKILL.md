---
name: security-review
description: 認証の追加、ユーザー入力の処理、シークレットの扱い、APIエンドポイントの作成、決済/機密機能の実装時にこのスキルを使用する。包括的なセキュリティチェックリストとパターンを提供。
---

# セキュリティレビュースキル

このスキルは全てのコードがセキュリティのベストプラクティスに従い、潜在的な脆弱性を特定することを保証する。

## 発動タイミング

- 認証または認可の実装時
- ユーザー入力やファイルアップロードの処理時
- 新しいAPIエンドポイントの作成時
- シークレットや認証情報の扱い時
- 決済機能の実装時
- 機密データの保存または送信時
- サードパーティAPIの統合時

## セキュリティチェックリスト

### 1. シークレット管理

#### ❌ 絶対にやってはいけないこと
```typescript
const apiKey = "sk-proj-xxxxx"  // ハードコードされたシークレット
const dbPassword = "password123" // ソースコード内
```

#### ✅ 常にやるべきこと
```typescript
const apiKey = process.env.OPENAI_API_KEY
const dbUrl = process.env.DATABASE_URL

// シークレットの存在を確認
if (!apiKey) {
  throw new Error('OPENAI_API_KEY not configured')
}
```

#### 検証ステップ
- [ ] ハードコードされたAPIキー、トークン、パスワードがない
- [ ] 全てのシークレットが環境変数内にある
- [ ] `.env.local` が.gitignoreに含まれている
- [ ] git履歴にシークレットがない
- [ ] 本番シークレットがホスティングプラットフォーム（Vercel、Railway）にある

### 2. 入力バリデーション

#### ユーザー入力は常にバリデーション
```typescript
import { z } from 'zod'

// バリデーションスキーマの定義
const CreateUserSchema = z.object({
  email: z.string().email(),
  name: z.string().min(1).max(100),
  age: z.number().int().min(0).max(150)
})

// 処理前にバリデーション
export async function createUser(input: unknown) {
  try {
    const validated = CreateUserSchema.parse(input)
    return await db.users.create(validated)
  } catch (error) {
    if (error instanceof z.ZodError) {
      return { success: false, errors: error.errors }
    }
    throw error
  }
}
```

#### ファイルアップロードのバリデーション
```typescript
function validateFileUpload(file: File) {
  // サイズチェック（最大5MB）
  const maxSize = 5 * 1024 * 1024
  if (file.size > maxSize) {
    throw new Error('File too large (max 5MB)')
  }

  // タイプチェック
  const allowedTypes = ['image/jpeg', 'image/png', 'image/gif']
  if (!allowedTypes.includes(file.type)) {
    throw new Error('Invalid file type')
  }

  // 拡張子チェック
  const allowedExtensions = ['.jpg', '.jpeg', '.png', '.gif']
  const extension = file.name.toLowerCase().match(/\.[^.]+$/)?.[0]
  if (!extension || !allowedExtensions.includes(extension)) {
    throw new Error('Invalid file extension')
  }

  return true
}
```

#### 検証ステップ
- [ ] 全てのユーザー入力がスキーマでバリデーションされている
- [ ] ファイルアップロードが制限されている（サイズ、タイプ、拡張子）
- [ ] ユーザー入力がクエリ内で直接使用されていない
- [ ] ホワイトリストバリデーション（ブラックリストではない）
- [ ] エラーメッセージが機密情報を漏洩しない

### 3. SQLインジェクション対策

#### ❌ 絶対にSQLを結合しない
```typescript
// 危険 - SQLインジェクション脆弱性
const query = `SELECT * FROM users WHERE email = '${userEmail}'`
await db.query(query)
```

#### ✅ 常にパラメータ化クエリを使用
```typescript
// 安全 - パラメータ化クエリ
const { data } = await supabase
  .from('users')
  .select('*')
  .eq('email', userEmail)

// または生SQLの場合
await db.query(
  'SELECT * FROM users WHERE email = $1',
  [userEmail]
)
```

#### 検証ステップ
- [ ] 全てのデータベースクエリがパラメータ化されている
- [ ] SQL内で文字列結合がない
- [ ] ORM/クエリビルダーが正しく使用されている
- [ ] Supabaseクエリが適切にサニタイズされている

### 4. 認証と認可

#### JWTトークンの取り扱い
```typescript
// ❌ 間違い: localStorage（XSSに脆弱）
localStorage.setItem('token', token)

// ✅ 正しい: httpOnlyクッキー
res.setHeader('Set-Cookie',
  `token=${token}; HttpOnly; Secure; SameSite=Strict; Max-Age=3600`)
```

#### 認可チェック
```typescript
export async function deleteUser(userId: string, requesterId: string) {
  // 常に最初に認可を確認
  const requester = await db.users.findUnique({
    where: { id: requesterId }
  })

  if (requester.role !== 'admin') {
    return NextResponse.json(
      { error: 'Unauthorized' },
      { status: 403 }
    )
  }

  // 削除を実行
  await db.users.delete({ where: { id: userId } })
}
```

#### Row Level Security（Supabase）
```sql
-- 全テーブルでRLSを有効化
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

-- ユーザーは自分のデータのみ閲覧可能
CREATE POLICY "Users view own data"
  ON users FOR SELECT
  USING (auth.uid() = id);

-- ユーザーは自分のデータのみ更新可能
CREATE POLICY "Users update own data"
  ON users FOR UPDATE
  USING (auth.uid() = id);
```

#### 検証ステップ
- [ ] トークンがhttpOnlyクッキーに保存されている（localStorageではない）
- [ ] 機密操作前に認可チェックがある
- [ ] SupabaseでRow Level Securityが有効
- [ ] ロールベースアクセス制御が実装されている
- [ ] セッション管理が安全

### 5. XSS対策

#### HTMLのサニタイズ
```typescript
import DOMPurify from 'isomorphic-dompurify'

// ユーザー提供のHTMLは常にサニタイズ
function renderUserContent(html: string) {
  const clean = DOMPurify.sanitize(html, {
    ALLOWED_TAGS: ['b', 'i', 'em', 'strong', 'p'],
    ALLOWED_ATTR: []
  })
  return <div dangerouslySetInnerHTML={{ __html: clean }} />
}
```

#### Content Security Policy
```typescript
// next.config.js
const securityHeaders = [
  {
    key: 'Content-Security-Policy',
    value: `
      default-src 'self';
      script-src 'self' 'unsafe-eval' 'unsafe-inline';
      style-src 'self' 'unsafe-inline';
      img-src 'self' data: https:;
      font-src 'self';
      connect-src 'self' https://api.example.com;
    `.replace(/\s{2,}/g, ' ').trim()
  }
]
```

#### 検証ステップ
- [ ] ユーザー提供のHTMLがサニタイズされている
- [ ] CSPヘッダーが設定されている
- [ ] 未バリデーションの動的コンテンツレンダリングがない
- [ ] Reactの組み込みXSS保護が使用されている

### 6. CSRF対策

#### CSRFトークン
```typescript
import { csrf } from '@/lib/csrf'

export async function POST(request: Request) {
  const token = request.headers.get('X-CSRF-Token')

  if (!csrf.verify(token)) {
    return NextResponse.json(
      { error: 'Invalid CSRF token' },
      { status: 403 }
    )
  }

  // リクエストを処理
}
```

#### SameSiteクッキー
```typescript
res.setHeader('Set-Cookie',
  `session=${sessionId}; HttpOnly; Secure; SameSite=Strict`)
```

#### 検証ステップ
- [ ] 状態変更操作にCSRFトークンがある
- [ ] 全クッキーにSameSite=Strictが設定されている
- [ ] ダブルサブミットクッキーパターンが実装されている

### 7. レート制限

#### APIレート制限
```typescript
import rateLimit from 'express-rate-limit'

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15分
  max: 100, // ウィンドウあたり100リクエスト
  message: 'Too many requests'
})

// ルートに適用
app.use('/api/', limiter)
```

#### 高コスト操作
```typescript
// 検索に対する厳格なレート制限
const searchLimiter = rateLimit({
  windowMs: 60 * 1000, // 1分
  max: 10, // 1分あたり10リクエスト
  message: 'Too many search requests'
})

app.use('/api/search', searchLimiter)
```

#### 検証ステップ
- [ ] 全APIエンドポイントにレート制限がある
- [ ] 高コスト操作により厳しい制限がある
- [ ] IPベースのレート制限
- [ ] ユーザーベースのレート制限（認証済み）

### 8. 機密データの露出

#### ログ記録
```typescript
// ❌ 間違い: 機密データのログ記録
console.log('User login:', { email, password })
console.log('Payment:', { cardNumber, cvv })

// ✅ 正しい: 機密データのマスキング
console.log('User login:', { email, userId })
console.log('Payment:', { last4: card.last4, userId })
```

#### エラーメッセージ
```typescript
// ❌ 間違い: 内部詳細の露出
catch (error) {
  return NextResponse.json(
    { error: error.message, stack: error.stack },
    { status: 500 }
  )
}

// ✅ 正しい: 一般的なエラーメッセージ
catch (error) {
  console.error('Internal error:', error)
  return NextResponse.json(
    { error: 'An error occurred. Please try again.' },
    { status: 500 }
  )
}
```

#### 検証ステップ
- [ ] ログにパスワード、トークン、シークレットがない
- [ ] ユーザー向けエラーメッセージが一般的
- [ ] 詳細エラーはサーバーログのみ
- [ ] スタックトレースがユーザーに露出していない

### 9. ブロックチェーンセキュリティ（Solana）

#### ウォレット検証
```typescript
import { verify } from '@solana/web3.js'

async function verifyWalletOwnership(
  publicKey: string,
  signature: string,
  message: string
) {
  try {
    const isValid = verify(
      Buffer.from(message),
      Buffer.from(signature, 'base64'),
      Buffer.from(publicKey, 'base64')
    )
    return isValid
  } catch (error) {
    return false
  }
}
```

#### トランザクション検証
```typescript
async function verifyTransaction(transaction: Transaction) {
  // 受取人の検証
  if (transaction.to !== expectedRecipient) {
    throw new Error('Invalid recipient')
  }

  // 金額の検証
  if (transaction.amount > maxAmount) {
    throw new Error('Amount exceeds limit')
  }

  // ユーザーの残高確認
  const balance = await getBalance(transaction.from)
  if (balance < transaction.amount) {
    throw new Error('Insufficient balance')
  }

  return true
}
```

#### 検証ステップ
- [ ] ウォレット署名が検証されている
- [ ] トランザクション詳細がバリデーションされている
- [ ] トランザクション前に残高チェックがある
- [ ] ブラインドトランザクション署名がない

### 10. 依存関係のセキュリティ

#### 定期的な更新
```bash
# 脆弱性のチェック
npm audit

# 自動修正可能な問題の修正
npm audit fix

# 依存関係の更新
npm update

# 古いパッケージの確認
npm outdated
```

#### ロックファイル
```bash
# 常にロックファイルをコミット
git add package-lock.json

# CI/CDで再現可能なビルドのために使用
npm ci  # npm installの代わりに
```

#### 検証ステップ
- [ ] 依存関係が最新
- [ ] 既知の脆弱性がない（npm audit クリーン）
- [ ] ロックファイルがコミットされている
- [ ] GitHubでDependabotが有効
- [ ] 定期的なセキュリティ更新

## セキュリティテスト

### 自動セキュリティテスト
```typescript
// 認証のテスト
test('requires authentication', async () => {
  const response = await fetch('/api/protected')
  expect(response.status).toBe(401)
})

// 認可のテスト
test('requires admin role', async () => {
  const response = await fetch('/api/admin', {
    headers: { Authorization: `Bearer ${userToken}` }
  })
  expect(response.status).toBe(403)
})

// 入力バリデーションのテスト
test('rejects invalid input', async () => {
  const response = await fetch('/api/users', {
    method: 'POST',
    body: JSON.stringify({ email: 'not-an-email' })
  })
  expect(response.status).toBe(400)
})

// レート制限のテスト
test('enforces rate limits', async () => {
  const requests = Array(101).fill(null).map(() =>
    fetch('/api/endpoint')
  )

  const responses = await Promise.all(requests)
  const tooManyRequests = responses.filter(r => r.status === 429)

  expect(tooManyRequests.length).toBeGreaterThan(0)
})
```

## デプロイ前セキュリティチェックリスト

本番デプロイの前に必ず確認:

- [ ] **シークレット**: ハードコードされたシークレットなし、全て環境変数内
- [ ] **入力バリデーション**: 全ユーザー入力がバリデーション済み
- [ ] **SQLインジェクション**: 全クエリがパラメータ化済み
- [ ] **XSS**: ユーザーコンテンツがサニタイズ済み
- [ ] **CSRF**: 保護が有効
- [ ] **認証**: 適切なトークン処理
- [ ] **認可**: ロールチェックが設置済み
- [ ] **レート制限**: 全エンドポイントで有効
- [ ] **HTTPS**: 本番環境で強制
- [ ] **セキュリティヘッダー**: CSP、X-Frame-Optionsが設定済み
- [ ] **エラーハンドリング**: エラーに機密データなし
- [ ] **ログ記録**: ログに機密データなし
- [ ] **依存関係**: 最新、脆弱性なし
- [ ] **Row Level Security**: Supabaseで有効
- [ ] **CORS**: 適切に設定
- [ ] **ファイルアップロード**: バリデーション済み（サイズ、タイプ）
- [ ] **ウォレット署名**: 検証済み（ブロックチェーンの場合）

## リソース

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Next.js Security](https://nextjs.org/docs/security)
- [Supabase Security](https://supabase.com/docs/guides/auth)
- [Web Security Academy](https://portswigger.net/web-security)

---

**注意**: セキュリティはオプションではない。1つの脆弱性がプラットフォーム全体を危険にさらす可能性がある。迷った場合は、安全側に倒すこと。
