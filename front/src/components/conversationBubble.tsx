import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import { Message, EMOTIONS } from '@/features/messages/messages'

const escapeRegExp = (value: string) =>
  value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const emotionPattern = new RegExp(
  `\\[(${EMOTIONS.map(escapeRegExp).join('|')})\\]`,
  'gi'
)

const extractText = (msg: Message | undefined): string => {
  if (!msg) return ''
  if (typeof msg.content === 'string') return msg.content
  if (Array.isArray(msg.content)) {
    const text = msg.content.find((c) => c.type === 'text')
    return text && 'text' in text ? text.text : ''
  }
  return ''
}

const stripTags = (text: string) =>
  text
    .replace(emotionPattern, '')
    .replace(/\[motion:[^\]]*\]/gi, '')
    .trim()

/**
 * 直近1対話（ユーザー発話＋アシスタント応答）を吹き出しで表示
 */
export const ConversationBubble = () => {
  const chatLog = homeStore((s) => s.chatLog)
  const characterName = settingsStore((s) => s.characterName)
  const showCharacterName = settingsStore((s) => s.showCharacterName)

  if (!chatLog || chatLog.length === 0) return null

  let lastAssistant: Message | undefined
  let lastUser: Message | undefined
  for (let i = chatLog.length - 1; i >= 0; i--) {
    const m = chatLog[i]
    if (!lastAssistant && m.role === 'assistant') lastAssistant = m
    if (lastAssistant && m.role === 'user') {
      lastUser = m
      break
    }
  }

  const assistantText = stripTags(extractText(lastAssistant))
  const userText = extractText(lastUser)

  if (!assistantText && !userText) return null

  return (
    <div className="pointer-events-none absolute bottom-0 left-0 right-0 z-10 mb-[80px] px-4 sm:px-6">
      <div className="mx-auto flex max-w-4xl flex-col gap-3">
        {userText && (
          <div className="flex justify-end">
            <div className="pointer-events-auto relative max-w-[85%] rounded-[28px] rounded-br-md bg-white/95 px-5 py-3 text-sm text-gray-800 shadow-[0_12px_40px_-12px_rgba(17,24,39,0.18)] backdrop-blur-md sm:text-base">
              {userText}
            </div>
          </div>
        )}
        {assistantText && (
          <div className="flex w-full justify-start">
            <div className="pointer-events-auto relative w-full rounded-[28px] bg-white/95 px-6 py-4 text-gray-800 shadow-[0_16px_48px_-16px_rgba(17,24,39,0.22)] backdrop-blur-md">
              {showCharacterName && (
                <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-500 sm:text-xs">
                  {characterName}
                </div>
              )}
              <div className="max-h-44 overflow-y-auto overscroll-contain text-sm leading-relaxed text-gray-900 sm:text-base">
                {assistantText}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
