import i18next from 'i18next'
import homeStore from '@/features/stores/home'
import toastStore from '@/features/stores/toast'
import settingsStore from '@/features/stores/settings'
import { Message } from '../messages/messages'

function handleApiError(errorCode: string): string {
  const languageCode = settingsStore.getState().selectLanguage
  i18next.changeLanguage(languageCode)
  return i18next.t(`Errors.${errorCode || 'AIAPIError'}`)
}

type CharacterState = {
  expression: string
  pose: string | null
}

function getCurrentCharacterState(): CharacterState {
  const viewer = homeStore.getState().viewer
  const model = viewer?.model
  const expression = model?.emoteController?.getCurrentEmotion?.() ?? 'neutral'
  const pose = model?.poseManager?.getCurrentPoseName?.() ?? null
  return { expression, pose }
}

/**
 * AgentCore（proxy Lambda 経由）からプレーンテキストストリームを受け取り、
 * ReadableStream<string> として返す。
 */
export async function getAgentCoreChatResponseStream(
  messages: Message[]
): Promise<ReadableStream<string>> {
  const characterState = getCurrentCharacterState()
  const response = await fetch('/api/ai/agentcore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages, characterState }),
  })

  if (!response.ok) {
    let errorCode = 'AIAPIError'
    try {
      const body = await response.json()
      errorCode = body.errorCode || errorCode
    } catch {
      // ignore
    }
    const errorMessage = handleApiError(errorCode)
    toastStore.getState().addToast({
      message: errorMessage,
      type: 'error',
      tag: 'agentcore-api-error',
    })
    throw new Error(`AgentCore request failed (${response.status})`, {
      cause: { errorCode },
    })
  }

  return new ReadableStream<string>({
    async start(controller) {
      let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
      try {
        if (!response.body) {
          throw new Error('AgentCore response body is empty', {
            cause: { errorCode: 'AIAPIError' },
          })
        }

        reader = response.body.getReader()
        const decoder = new TextDecoder('utf-8')

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          const chunk = decoder.decode(value, { stream: true })
          if (chunk) controller.enqueue(chunk)
        }
        // 末尾の残りを flush
        const tail = decoder.decode()
        if (tail) controller.enqueue(tail)
      } catch (error) {
        console.error('Error reading AgentCore stream:', error)
        toastStore.getState().addToast({
          message: i18next.t('Errors.AIAPIError'),
          type: 'error',
          tag: 'agentcore-api-error',
        })
      } finally {
        controller.close()
        if (reader) reader.releaseLock()
      }
    },
  })
}
