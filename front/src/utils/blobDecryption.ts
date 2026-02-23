/**
 * Blob復号ユーティリティ（クライアントサイド / Web Crypto API）
 *
 * サーバーから取得した暗号化済みVRMデータを、
 * 別途取得した復号キーでメモリ上で復号する。
 */

import { base64ToUint8Array } from '@/utils/encoding'

const ALGORITHM = 'AES-GCM'

export async function decryptVrmData(
  encryptedData: ArrayBuffer,
  keyBase64: string,
  ivBase64: string
): Promise<ArrayBuffer> {
  const rawKey = base64ToUint8Array(keyBase64)
  const iv = base64ToUint8Array(ivBase64)

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    rawKey,
    { name: ALGORITHM },
    false,
    ['decrypt']
  )

  return crypto.subtle.decrypt(
    { name: ALGORITHM, iv },
    cryptoKey,
    encryptedData
  )
}
