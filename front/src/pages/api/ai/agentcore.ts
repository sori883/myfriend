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

  const { messages } = (await req.json()) as {
    messages: Message[]
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
  const history = messages
    .filter((m) => m.role === 'user' || m.role === 'assistant')
    .slice(-21, -1)
    .map((m) => ({ role: m.role, content: extractText(m) }))
    .filter((m) => m.content.trim())

  const invokeUrl = `${agentcoreUrl}/invoke`

  try {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }
    const apiKey = process.env.AGENTCORE_API_KEY
    if (apiKey) {
      headers['x-api-key'] = apiKey
    }

    const agentResponse = await fetch(invokeUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        bank_id: agentcoreBankId,
        prompt: prompt.trim(),
        messages: history,
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

    // プレーンテキストストリームをそのままパイプスルー
    return new Response(agentResponse.body, {
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
