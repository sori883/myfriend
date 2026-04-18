import type { NextRequest } from 'next/server'
import { deriveKeyAndIv, isValidPrivateBlobUrl } from '@/utils/blobEncryption'
import { arrayBufferToBase64 } from '@/utils/encoding'

export const config = { runtime: 'edge' }

/**
 * 暗号化Blobの復号キーを返す Edge Function
 *
 * ミドルウェア認証（middleware.ts）で保護済み。
 * Cache-Control: no-store で、キーがCDNやブラウザにキャッシュされることを防ぐ。
 *
 * blob-url.ts と同じ URL + BLOB_ENCRYPTION_SECRET からキーを導出する。
 */
export default async function handler(req: NextRequest) {
  if (req.method !== 'GET') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  const url = req.nextUrl.searchParams.get('url')
  if (!url) {
    return new Response(
      JSON.stringify({ error: 'url parameter is required' }),
      { status: 400, headers: { 'Content-Type': 'application/json' } }
    )
  }

  if (!isValidPrivateBlobUrl(url)) {
    return new Response(JSON.stringify({ error: 'Invalid blob URL' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  const encryptionSecret = process.env.BLOB_ENCRYPTION_SECRET
  if (!encryptionSecret) {
    return new Response(
      JSON.stringify({ error: 'Encryption not configured' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    )
  }

  try {
    const { rawKey, iv } = await deriveKeyAndIv(encryptionSecret, url)

    return new Response(
      JSON.stringify({
        key: arrayBufferToBase64(rawKey),
        iv: arrayBufferToBase64(iv.buffer as ArrayBuffer),
      }),
      {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'private, no-store',
        },
      }
    )
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error'
    return new Response(
      JSON.stringify({ error: `Failed to derive key: ${message}` }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    )
  }
}
