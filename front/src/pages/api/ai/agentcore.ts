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

  const { messages, agentcoreBankId } = (await req.json()) as {
    messages: Message[]
    agentcoreBankId: string
  }

  // bank_id の UUID バリデーション
  if (!agentcoreBankId || !UUID_REGEX.test(agentcoreBankId)) {
    return new Response(
      JSON.stringify({
        error: 'Invalid bank ID format',
        errorCode: 'AgentCoreInvalidBankId',
      }),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      }
    )
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

  // content が配列の場合はテキスト部分のみ抽出
  type TextContent = { type: 'text'; text: string }
  const prompt =
    typeof lastUserMessage.content === 'string'
      ? lastUserMessage.content
      : Array.isArray(lastUserMessage.content)
        ? lastUserMessage.content
            .filter((c): c is TextContent => c.type === 'text')
            .map((c) => c.text)
            .join('\n')
        : ''

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

  const invokeUrl = `${agentcoreUrl}/invoke`

  try {
    const agentResponse = await fetch(invokeUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        bank_id: agentcoreBankId,
        prompt: prompt.trim(),
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

    // AgentCore の plain text stream をそのままパイプスルー
    // vercelAIChat.ts が Content-Type: text/plain を検出して直接 enqueue する
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
