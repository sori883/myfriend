import { Message } from '@/features/messages/messages'
import settingsStore from '@/features/stores/settings'
import i18next from 'i18next'
import toastStore from '@/features/stores/toast'

function handleApiError(errorCode: string): string {
  const languageCode = settingsStore.getState().selectLanguage
  i18next.changeLanguage(languageCode)
  return i18next.t(`Errors.${errorCode || 'AIAPIError'}`)
}

export async function getAgentCoreChatResponseStream(
  messages: Message[]
): Promise<ReadableStream<string>> {
  const ss = settingsStore.getState()

  const response = await fetch('/api/ai/agentcore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      messages,
      agentcoreBankId: ss.agentcoreBankId,
    }),
  })

  if (!response.ok) {
    const responseBody = await response.json().catch(() => ({
      error: 'Unknown error',
      errorCode: 'AgentCoreAPIError',
    }))
    const errorMessage = handleApiError(responseBody.errorCode)
    toastStore.getState().addToast({
      message: errorMessage,
      type: 'error',
      tag: 'agentcore-api-error',
    })
    throw new Error(
      `AgentCore API request failed with status ${response.status}: ${responseBody.error}`,
      { cause: { errorCode: responseBody.errorCode } }
    )
  }

  // AgentCore はプレーンテキストストリームを返す
  return new ReadableStream({
    async start(controller) {
      if (!response.body) {
        throw new Error('AgentCore response body is empty', {
          cause: { errorCode: 'AgentCoreAPIError' },
        })
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder('utf-8')

      try {
        while (true) {
          const { done, value } = await reader.read()
          if (done) break

          const chunk = decoder.decode(value, { stream: true })
          if (chunk) {
            controller.enqueue(chunk)
          }
        }
      } catch (error) {
        const errorMessage = handleApiError('AIAPIError')
        toastStore.getState().addToast({
          message: errorMessage,
          type: 'error',
          tag: 'agentcore-api-error',
        })
      } finally {
        controller.close()
        reader.releaseLock()
      }
    },
  })
}
