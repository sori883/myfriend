import type { NextApiRequest, NextApiResponse } from 'next'
import { getDownloadUrl } from '@vercel/blob'

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  const { url } = req.query
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url parameter is required' })
  }

  // Vercel Blob の private URL のみ許可
  if (!url.includes('.private.blob.vercel-storage.com')) {
    return res.status(400).json({ error: 'Invalid blob URL' })
  }

  try {
    const downloadUrl = await getDownloadUrl(url)
    return res.status(200).json({ downloadUrl })
  } catch (error) {
    console.error('Failed to get blob download URL:', error)
    return res.status(500).json({ error: 'Failed to generate download URL' })
  }
}
