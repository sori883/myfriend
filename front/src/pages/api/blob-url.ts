import type { NextRequest } from 'next/server'

export const config = { runtime: 'edge' }

/**
 * Private Blob のコンテンツをプロキシで配信する Edge Function
 * ブラウザから private blob に直接アクセスできないため、
 * サーバーサイドで認証付きフェッチを行いストリーミングで返す
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
    return new Response(JSON.stringify({ error: 'url parameter is required' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Vercel Blob の private URL のみ許可
  if (!url.includes('.private.blob.vercel-storage.com')) {
    return new Response(JSON.stringify({ error: 'Invalid blob URL' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  const token = process.env.BLOB_READ_WRITE_TOKEN
  if (!token) {
    return new Response(
      JSON.stringify({ error: 'Blob token not configured' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    )
  }

  try {
    const blobResponse = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    })

    if (!blobResponse.ok) {
      return new Response(
        JSON.stringify({ error: `Blob fetch failed: ${blobResponse.status}` }),
        {
          status: blobResponse.status,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }

    return new Response(blobResponse.body, {
      headers: {
        'Content-Type':
          blobResponse.headers.get('Content-Type') ||
          'application/octet-stream',
        'Cache-Control': 'public, max-age=31536000, immutable',
      },
    })
  } catch (error) {
    console.error('Failed to proxy blob:', error)
    return new Response(
      JSON.stringify({ error: 'Failed to proxy blob' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    )
  }
}
