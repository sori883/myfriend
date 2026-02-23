import type { NextApiRequest, NextApiResponse } from 'next'

/**
 * サーバーサイド環境変数から設定値を返す API
 * NEXT_PUBLIC_ を使わずにクライアントへ初期値を渡す
 */
export default function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  res.status(200).json({
    selectedVrmPath: process.env.SELECTED_VRM_PATH || '',
  })
}
