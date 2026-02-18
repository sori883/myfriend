import { createRoute } from 'honox/factory'
import { stream } from 'hono/streaming'

const AGENTCORE_URL = 'http://localhost:8080/invoke'

export const POST = createRoute(async (c) => {
  const { bank_id, prompt } = await c.req.json<{ bank_id: string; prompt: string }>()

  const res = await fetch(AGENTCORE_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ bank_id, prompt }),
  })

  if (!res.ok) {
    return c.json({ error: `AgentCore error: ${res.status}` }, 502)
  }

  c.header('Content-Encoding', 'Identity')
  return stream(c, async (s) => {
    await s.pipe(res.body!)
  })
})
