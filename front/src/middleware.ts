import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

const COOKIE_NAME = 'site_auth'
const COOKIE_MAX_AGE = 60 * 60 * 24 * 30 // 30日

/** タイミング攻撃を防ぐ定時間比較 */
function constantTimeEquals(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  let result = 0
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i)
  }
  return result === 0
}

/** パスワードハッシュから HMAC ベースのセッショントークンを生成 */
function deriveSessionToken(passwordHash: string): string {
  // Edge Runtime では crypto.createHmac が使えないため、
  // パスワードハッシュ + 固定ソルトの簡易派生トークンを使用
  const salt = 'site_auth_session_v1'
  let hash = 0
  const combined = salt + passwordHash
  for (let i = 0; i < combined.length; i++) {
    const char = combined.charCodeAt(i)
    hash = ((hash << 5) - hash + char) | 0
  }
  return `s_${Math.abs(hash).toString(36)}_${passwordHash.slice(0, 8)}`
}

export function middleware(request: NextRequest) {
  const passwordHash = process.env.SITE_PASSWORD_HASH
  const isVercel = process.env.VERCEL

  // Vercel以外 or パスワード未設定 → スキップ
  if (!isVercel || !passwordHash) {
    return NextResponse.next()
  }

  const sessionToken = deriveSessionToken(passwordHash)

  // cookie 認証済み → スキップ
  const cookieValue = request.cookies.get(COOKIE_NAME)?.value ?? ''
  if (cookieValue.length > 0 && constantTimeEquals(cookieValue, sessionToken)) {
    return NextResponse.next()
  }

  // URLパラメータのトークンを検証
  const token = request.nextUrl.searchParams.get('token') ?? ''
  if (token.length > 0 && constantTimeEquals(token, passwordHash)) {
    // クリーンURLにリダイレクト + cookie セット
    const url = request.nextUrl.clone()
    url.searchParams.delete('token')
    const response = NextResponse.redirect(url)
    response.cookies.set(COOKIE_NAME, sessionToken, {
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
      path: '/',
      maxAge: COOKIE_MAX_AGE,
    })
    return response
  }

  // 未認証 → 401
  return new NextResponse('Unauthorized', { status: 401 })
}

export const config = {
  matcher: [
    // 静的ファイル・_next・favicon を除外
    '/((?!_next/static|_next/image|favicon.ico|scripts/).*)',
  ],
}
