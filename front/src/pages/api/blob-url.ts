import type { NextApiRequest, NextApiResponse } from 'next'
import {
  encryptData,
  isValidPrivateBlobUrl,
  MAX_BLOB_SIZE,
} from '@/utils/blobEncryption'

export const config = {
  api: {
    responseLimit: false,
  },
}

/**
 * Private Blob のコンテンツを暗号化して配信する Serverless Function
 *
 * Node.js Runtime を使用（Edge Runtimeではメモリ不足になる大容量VRMに対応）
 *
 * BLOB_ENCRYPTION_SECRET が設定されている場合:
 *   - VRMデータをAES-GCMで暗号化して返す
 *   - 復号キーは /api/blob-key から別途取得する必要がある
 *   - 暗号化済みデータはCDNキャッシュ可能（生データは露出しない）
 *
 * BLOB_ENCRYPTION_SECRET が未設定の場合:
 *   - 従来通り生データをプロキシ配信（キャッシュなし）
 */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  const url = typeof req.query.url === 'string' ? req.query.url : ''
  if (!url) {
    return res.status(400).json({ error: 'url parameter is required' })
  }

  if (!isValidPrivateBlobUrl(url)) {
    return res.status(400).json({ error: 'Invalid blob URL' })
  }

  const token = process.env.BLOB_READ_WRITE_TOKEN
  if (!token) {
    return res.status(500).json({ error: 'Blob token not configured' })
  }

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), 30000)

  try {
    const blobResponse = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
      signal: controller.signal,
    })

    if (!blobResponse.ok) {
      return res
        .status(blobResponse.status)
        .json({ error: `Blob fetch failed: ${blobResponse.status}` })
    }

    const contentLength = parseInt(
      blobResponse.headers.get('content-length') ?? '0',
      10
    )
    if (contentLength > MAX_BLOB_SIZE) {
      return res
        .status(413)
        .json({ error: 'Blob too large for encrypted delivery' })
    }

    const encryptionSecret = process.env.BLOB_ENCRYPTION_SECRET

    if (encryptionSecret) {
      const etag = blobResponse.headers.get('etag') ?? ''
      const rawData = await blobResponse.arrayBuffer()
      const { encrypted } = await encryptData(
        rawData,
        encryptionSecret,
        url,
        etag
      )

      res.setHeader('Content-Type', 'application/octet-stream')
      res.setHeader('X-Blob-Encrypted', 'aes-256-gcm')
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable')
      return res.send(Buffer.from(encrypted))
    }

    const contentType =
      blobResponse.headers.get('Content-Type') || 'application/octet-stream'
    const rawData = await blobResponse.arrayBuffer()

    res.setHeader('Content-Type', contentType)
    res.setHeader('Cache-Control', 'private, no-store')
    return res.send(Buffer.from(rawData))
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Unknown error'
    return res
      .status(500)
      .json({ error: `Failed to proxy blob: ${message}` })
  } finally {
    clearTimeout(timeoutId)
  }
}
