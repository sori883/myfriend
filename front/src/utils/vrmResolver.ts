/**
 * VRM URL resolver
 *
 * - Vercel Blob private URL の場合: /api/blob-url から暗号化データ、
 *   /api/blob-key から復号キーを取得し、Web Crypto で復号して Blob URL を生成
 * - それ以外のパス（例: /vrm/xxx.vrm、http URL など）はそのまま返す
 *
 * ローカル開発時は通常 /vrm/*.vrm を指定するため、復号パスは使われない。
 * Vercel デプロイ時に selectedVrmPath を Vercel Blob の URL に差し替えることで
 * 自動的に暗号化配信＆クライアント復号が有効になる。
 */

import { decryptVrmData } from '@/utils/blobDecryption'
import { isValidPrivateBlobUrl } from '@/utils/blobEncryption'

export async function resolveVrmSource(path: string): Promise<string> {
  if (!isValidPrivateBlobUrl(path)) {
    return path
  }

  const encodedUrl = encodeURIComponent(path)
  const [encResp, keyResp] = await Promise.all([
    fetch(`/api/blob-url?url=${encodedUrl}`),
    fetch(`/api/blob-key?url=${encodedUrl}`),
  ])

  if (!encResp.ok) {
    throw new Error(`Failed to fetch encrypted VRM (${encResp.status})`)
  }
  if (!keyResp.ok) {
    throw new Error(`Failed to fetch decryption key (${keyResp.status})`)
  }

  const encryptedData = await encResp.arrayBuffer()
  const { key, iv } = (await keyResp.json()) as { key: string; iv: string }

  const decrypted = await decryptVrmData(encryptedData, key, iv)

  const blob = new Blob([decrypted], { type: 'application/octet-stream' })
  return URL.createObjectURL(blob)
}
