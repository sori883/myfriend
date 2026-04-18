import { useState } from 'react'
import { MessageInput } from '@/components/messageInput'
import homeStore from '@/features/stores/home'
import { SpeakQueue } from '@/features/messages/speakQueue'

type Props = {
  onChatProcessStart: (text: string) => void
}

/**
 * 音声入力を一切使わないテキスト専用の入力コンテナ。
 * useVoiceRecognition を呼び出さないため、ブラウザのマイクアクセス許可を
 * 求める処理は発生しない。
 */
export const MessageInputContainer = ({ onChatProcessStart }: Props) => {
  const isSpeaking = homeStore((s) => s.isSpeaking)
  const [userMessage, setUserMessage] = useState('')

  const handleInputChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    setUserMessage(e.target.value)
  }

  const handleSendMessage = () => {
    const text = userMessage.trim()
    if (!text) return
    homeStore.setState({ chatProcessing: true })
    setUserMessage('')
    onChatProcessStart(text)
  }

  const handleStopSpeaking = () => {
    homeStore.setState({ isSpeaking: false })
    SpeakQueue.stopAll()
  }

  const noop = () => {}

  return (
    <MessageInput
      userMessage={userMessage}
      isMicRecording={false}
      onChangeUserMessage={handleInputChange}
      onClickMicButton={noop}
      onClickSendButton={handleSendMessage}
      onClickStopButton={handleStopSpeaking}
      isSpeaking={isSpeaking}
      silenceTimeoutRemaining={null}
      continuousMicListeningMode={false}
      onToggleContinuousMode={noop}
    />
  )
}
