import { useEffect, useRef } from 'react'

import { generateIdleAIPhrase } from '@/features/idle/generateIdleAIPhrase'
import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import { speakCharacter } from '@/features/messages/speakCharacter'

type TimePeriod = 'morning' | 'afternoon' | 'evening'

function getTimePeriod(): TimePeriod {
  const hour = new Date().getHours()
  if (hour >= 5 && hour < 11) return 'morning'
  if (hour >= 11 && hour < 17) return 'afternoon'
  return 'evening'
}

/**
 * ページマウント直後に AI 生成の時間帯別挨拶を1回だけ発話する。
 *
 * - settingsStore.initialGreetingEnabled が true のときのみ有効
 * - idleTimePeriod{Morning,Afternoon,Evening} の文面を AI 生成のヒントとして流用
 * - 発話は speakCharacter 経由で音声＋吹き出し表示、chatLog に assistant として追加
 * - React Strict Mode の二重実行を useRef で防止
 */
export function useInitialGreeting() {
  const triggeredRef = useRef(false)
  const initialGreetingEnabled = settingsStore(
    (s) => s.initialGreetingEnabled
  )

  useEffect(() => {
    if (!initialGreetingEnabled) return
    if (triggeredRef.current) return
    triggeredRef.current = true

    void (async () => {
      const ss = settingsStore.getState()
      const period = getTimePeriod()
      const hintByPeriod: Record<TimePeriod, string> = {
        morning: ss.idleTimePeriodMorning,
        afternoon: ss.idleTimePeriodAfternoon,
        evening: ss.idleTimePeriodEvening,
      }
      const hint = hintByPeriod[period]

      const promptTemplate = `現在の時間帯は「${period}」です。
ユーザーが画面を開いたばかりなので、あなたから会話を切り出してください。
参考例: 「${hint}」
自然な会話調で、時間帯に合った挨拶と、相手の状況や気持ちを引き出す短い問いかけを1つ返してください。`

      try {
        const phrase = await generateIdleAIPhrase(promptTemplate)
        if (!phrase) return

        const sessionId = `initial-greeting-${Date.now()}`
        speakCharacter(
          sessionId,
          { message: phrase.text, emotion: phrase.emotion },
          () => {},
          () => {}
        )

        homeStore.getState().upsertMessage({
          role: 'assistant',
          content: phrase.text,
        })
      } catch (error) {
        console.error('[useInitialGreeting] failed:', error)
      }
    })()
  }, [initialGreetingEnabled])
}
