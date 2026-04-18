import { Message } from '@/features/messages/messages'
import { NextRequest } from 'next/server'

export const config = {
  runtime: 'edge',
}

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export default async function handler(req: NextRequest) {
  if (req.method !== 'POST') {
    return new Response(
      JSON.stringify({
        error: 'Method Not Allowed',
        errorCode: 'METHOD_NOT_ALLOWED',
      }),
      {
        status: 405,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  // URL はサーバーサイド環境変数から取得（SSRF 防止）
  const agentcoreUrl = process.env.AGENTCORE_URL || 'http://localhost:8080'

  // bank_id はサーバーサイド環境変数から取得（クライアントに露出しない）
  const agentcoreBankId = process.env.AGENTCORE_BANK_ID
  if (!agentcoreBankId || !UUID_REGEX.test(agentcoreBankId)) {
    return new Response(
      JSON.stringify({
        error: 'AGENTCORE_BANK_ID is not configured',
        errorCode: 'AgentCoreInvalidBankId',
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  const { messages, characterState } = (await req.json()) as {
    messages: Message[]
    characterState?: { expression?: string; pose?: string | null }
  }

  // メッセージからテキストを抽出するヘルパー
  type TextContent = { type: 'text'; text: string }
  const extractText = (msg: Message): string => {
    const raw =
      typeof msg.content === 'string'
        ? msg.content
        : Array.isArray(msg.content)
          ? msg.content
              .filter((c): c is TextContent => c.type === 'text')
              .map((c) => c.text)
              .join('\n')
          : ''
    // アシスタントの感情タグを除去
    return msg.role === 'assistant'
      ? raw.replace(/\[([a-zA-Z]*?)\]\s*/g, '')
      : raw
  }

  // 最新の user メッセージを抽出
  const lastUserMessage = [...messages].reverse().find((m) => m.role === 'user')

  if (!lastUserMessage) {
    return new Response(
      JSON.stringify({
        error: 'No user message found',
        errorCode: 'AgentCoreNoUserMessage',
      }),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  const prompt = extractText(lastUserMessage)

  if (!prompt.trim()) {
    return new Response(
      JSON.stringify({
        error: 'Empty prompt',
        errorCode: 'AgentCoreEmptyPrompt',
      }),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  // 会話履歴を構築（最新 user メッセージを除く直近20件）
  const userAssistantMessages = messages.filter(
    (m) => m.role === 'user' || m.role === 'assistant'
  )
  const history = userAssistantMessages
    .slice(0, -1)
    .slice(-20)
    .map((m) => ({ role: m.role, content: extractText(m) }))
    .filter((m) => m.content.trim())
  console.log(
    `[agentcore] total messages: ${messages.length}, user/assistant: ${userAssistantMessages.length}, history sent: ${history.length}`
  )
  console.log(
    '[agentcore] all roles:',
    messages.map(
      (m) =>
        `${m.role}:${typeof m.content === 'string' ? m.content.slice(0, 30) : 'array'}`
    )
  )

  const invokeUrl = `${agentcoreUrl}/invoke`

  try {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }
    const apiKey = process.env.AGENTCORE_API_KEY
    if (apiKey) {
      headers['x-api-key'] = apiKey
    }

    const characterStatePayload =
      characterState && typeof characterState === 'object'
        ? {
            expression:
              typeof characterState.expression === 'string'
                ? characterState.expression
                : 'neutral',
            pose:
              typeof characterState.pose === 'string'
                ? characterState.pose
                : null,
          }
        : null

    const agentResponse = await fetch(invokeUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        bank_id: agentcoreBankId,
        prompt: prompt.trim(),
        messages: history,
        ...(characterStatePayload && {
          character_state: characterStatePayload,
        }),
      }),
      signal: AbortSignal.timeout(180000),
    })

    if (!agentResponse.ok) {
      return new Response(
        JSON.stringify({
          error: `AgentCore request failed (${agentResponse.status})`,
          errorCode: 'AgentCoreAPIError',
        }),
        {
          status: agentResponse.status >= 500 ? 502 : agentResponse.status,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }

    // プレーンテキストストリームを自前の ReadableStream でラップし、
    // クライアント切断時の ECONNRESET 等を握りつぶして uncaughtException を防ぐ。
    const upstream = agentResponse.body
    if (!upstream) {
      return new Response(
        JSON.stringify({
          error: 'AgentCore returned empty body',
          errorCode: 'AgentCoreEmptyBody',
        }),
        {
          status: 502,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }

    const safeStream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const reader = upstream.getReader()
        try {
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            try {
              controller.enqueue(value)
            } catch {
              // コントローラーが既に閉じている（クライアント切断）
              break
            }
          }
        } catch (err) {
          // upstream 読み取りエラー（接続切断など）。ログのみで握りつぶす。
          console.warn('[agentcore] upstream read error:', err)
        } finally {
          try {
            controller.close()
          } catch {
            // 既に close 済み
          }
          try {
            reader.releaseLock()
          } catch {
            // ignore
          }
        }
      },
      async cancel() {
        // クライアント切断時に upstream をキャンセル
        try {
          await upstream.cancel()
        } catch {
          // ignore
        }
      },
    })

    return new Response(safeStream, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      },
    })
  } catch (error) {
    return new Response(
      JSON.stringify({
        error: 'AgentCore connection failed',
        errorCode: 'AgentCoreConnectionError',
      }),
      {
        status: 502,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }
}
