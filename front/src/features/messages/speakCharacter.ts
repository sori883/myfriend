import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import type { AIVoice } from '@/features/constants/settings'
import { Talk } from './messages'
import { SpeakQueue } from './speakQueue'
import { Live2DHandler } from './live2dHandler'
import { PNGTuberHandler } from '@/features/pngTuber/pngTuberHandler'

const speakQueue = SpeakQueue.getInstance()

/**
 * メッセージの軽い前処理。絵文字・余白・記号のみのテキストを除外する。
 * 既存テストとの互換維持のために export されているが、現行実装では
 * speakCharacter 自体が発話を行わないため実質未使用。
 */
export function preprocessMessage(
  message: string,
  _settings: ReturnType<typeof settingsStore.getState>
): string | null {
  let processed: string | null = message.trim()
  if (!processed) return null

  processed = processed.replace(
    /[\u{1F300}-\u{1F9FF}]|[\u{1F600}-\u{1F64F}]|[\u{1F680}-\u{1F6FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]|[\u{1F900}-\u{1F9FF}]|[\u{1F1E0}-\u{1F1FF}]/gu,
    ''
  )

  const isOnlySymbols =
    /^[!?.,。、．，'"(){}[\]<>+=\-*\/\\|;:@#$%^&*_~！？（）「」『』【】〔〕［］｛｝〈〉《》｢｣。、．，：；＋－＊／＝＜＞％＆＾｜～＠＃＄＿"　]+$/.test(
      processed
    )

  if (processed === '' || isOnlySymbols) return null
  return processed
}

/**
 * TTS が廃止されているため、モデル種別に応じて表情・ポーズだけを反映する。
 */
async function applyVisualOnly(talk: Talk): Promise<void> {
  const ss = settingsStore.getState()
  const hs = homeStore.getState()
  try {
    if (ss.modelType === 'live2d') {
      await Live2DHandler.speak(new ArrayBuffer(0), talk, false)
      return
    }
    if (ss.modelType === 'pngtuber') {
      await PNGTuberHandler.speak(new ArrayBuffer(0), talk, false)
      return
    }
    const model = hs.viewer?.model
    if (!model) return
    model.emoteController?.playEmotion(talk.emotion)
    // motion タグが指定された場合のみポーズを切替。
    // 未指定の場合は現在のポーズを維持する（LLM が明示的に [motion:xxx] で
    // 切り替えない限り保持）。
    if (talk.motion) {
      const poseConfig = ss.poseConfigs.find((p) => p.id === talk.motion)
      if (poseConfig) {
        await model.poseManager
          .applyPose(model, talk.motion, poseConfig)
          .catch((e: unknown) => console.error('Failed to apply pose:', e))
      }
    }
  } catch (e) {
    console.error('applyVisualOnly failed:', e)
  }
}

const createSpeakCharacter = () => {
  return (
    sessionId: string,
    talk: Talk,
    onStart?: () => void,
    onComplete?: () => void
  ) => {
    let called = false
    const complete = () => {
      if (onComplete && !called) {
        called = true
        onComplete()
      }
    }

    onStart?.()

    const initialToken = SpeakQueue.currentStopToken
    speakQueue.checkSessionId(sessionId)

    if (SpeakQueue.currentStopToken !== initialToken) {
      complete()
      return
    }

    void applyVisualOnly(talk).finally(complete)
  }
}

export const speakCharacter = createSpeakCharacter()

/**
 * 後方互換のために残す no-op の TTS 関連エクスポート。
 * UI / テストから参照されているが、現行では音声は一切再生しない。
 */
export function handleTTSError(_error: unknown, _serviceName: string): void {
  /* TTS 廃止のため no-op */
}

export const testVoiceVox = async (_customText?: string) => {
  /* TTS 廃止のため no-op */
}

export const testAivisSpeech = async (_customText?: string) => {
  /* TTS 廃止のため no-op */
}

export const testVoice = async (_voiceType: AIVoice, _customText?: string) => {
  /* TTS 廃止のため no-op */
}
