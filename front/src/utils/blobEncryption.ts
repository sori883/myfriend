/**
 * Blob暗号化ユーティリティ（サーバーサイド / Edge Runtime対応）
 *
 * BLOB_ENCRYPTION_SECRET + blob URLパスから決定的にAES-256キーとIVを導出し、
 * VRMデータをAES-GCMで暗号化する。
 * 同じURL + 同じシークレットからは常に同じキー/IVが生成されるため、
 * 暗号化済みレスポンスのCDNキャッシュが有効。
 *
 * SECURITY INVARIANT: 決定的な(key, IV)ペアを生成するため、
 * Vercel BlobのURLが不変（content-addressed）であることが前提。
 * 同一URLで内容が変わるとAES-GCM nonce再利用が発生し暗号が破られる。
 * ETagをキー導出に含めることで、内容変更時の安全性を確保する。
 */

import { arrayBufferToBase64 } from '@/utils/encoding'

const ALGORITHM = 'AES-GCM'
const IV_LENGTH = 12
const MAX_BLOB_SIZE = 100 * 1024 * 1024 // 100MB

async function hmacSha256(
  secret: string,
  message: string
): Promise<ArrayBuffer> {
  const encoder = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )
  return crypto.subtle.sign('HMAC', key, encoder.encode(message))
}

export async function deriveKeyAndIv(
  secret: string,
  blobUrl: string,
  etag: string = ''
): Promise<{ rawKey: ArrayBuffer; iv: Uint8Array }> {
  const material = `${blobUrl}:${etag}`
  const rawKey = await hmacSha256(secret, `blob-key:${material}`)

  const ivSource = await hmacSha256(secret, `blob-iv:${material}`)
  const iv = new Uint8Array(ivSource).slice(0, IV_LENGTH)

  return { rawKey, iv }
}

export async function encryptData(
  data: ArrayBuffer,
  secret: string,
  blobUrl: string,
  etag: string = ''
): Promise<{ encrypted: ArrayBuffer; iv: Uint8Array }> {
  const { rawKey, iv } = await deriveKeyAndIv(secret, blobUrl, etag)

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    rawKey,
    { name: ALGORITHM },
    false,
    ['encrypt']
  )

  const encrypted = await crypto.subtle.encrypt(
    { name: ALGORITHM, iv },
    cryptoKey,
    data
  )

  return { encrypted, iv }
}

export function ivToBase64(iv: Uint8Array): string {
  return arrayBufferToBase64(iv.buffer as ArrayBuffer)
}

/**
 * Vercel Blob の private URL のみ許可する。
 * <store-id>.private.blob.vercel-storage.com の形式のみ受け付ける。
 */
export function isValidPrivateBlobUrl(url: string): boolean {
  try {
    const parsed = new URL(url)
    const validPattern = /^[a-z0-9]+\.private\.blob\.vercel-storage\.com$/
    return parsed.protocol === 'https:' && validPattern.test(parsed.hostname)
  } catch {
    return false
  }
}

export { MAX_BLOB_SIZE }
