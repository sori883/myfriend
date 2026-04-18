import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

const COOKIE_NAME = 'site_auth'
const COOKIE_MAX_AGE = 60 * 60 * 24 * 30 // 30日
const SESSION_CONTEXT = 'site_auth_session_v1'
const TOKEN_QUERY_KEY = 'token'

/** タイミング攻撃を防ぐ定時間比較 */
function constantTimeEquals(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  let result = 0
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i)
  }
  return result === 0
}

/** Web Crypto API (Edge Runtime 対応) で HMAC-SHA256 を計算して hex で返す */
async function hmacSha256Hex(secret: string, message: string): Promise<string> {
  const encoder = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    encoder.encode(message)
  )
  const bytes = new Uint8Array(signature)
  let hex = ''
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0')
  }
  return hex
}

export async function middleware(request: NextRequest) {
  const secret = process.env.SITE_ACCESS_SECRET
  const isVercel = process.env.VERCEL === '1'

  // Vercel 以外 or シークレット未設定 → 認証スキップ
  if (!isVercel || !secret) {
    return NextResponse.next()
  }

  const sessionToken = await hmacSha256Hex(secret, SESSION_CONTEXT)

  // cookie 認証済み → スキップ
  const cookieValue = request.cookies.get(COOKIE_NAME)?.value ?? ''
  if (cookieValue.length > 0 && constantTimeEquals(cookieValue, sessionToken)) {
    return NextResponse.next()
  }

  // URLパラメータのトークンを検証
  const token = request.nextUrl.searchParams.get(TOKEN_QUERY_KEY) ?? ''
  if (token.length > 0 && constantTimeEquals(token, secret)) {
    // クリーンURLにリダイレクト + cookie セット
    const url = request.nextUrl.clone()
    url.searchParams.delete(TOKEN_QUERY_KEY)
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
