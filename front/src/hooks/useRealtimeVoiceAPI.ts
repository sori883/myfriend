import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useIsomorphicLayoutEffect } from './useIsomorphicLayoutEffect'
import { useTranslation } from 'react-i18next'
import settingsStore from '@/features/stores/settings'
import webSocketStore from '@/features/stores/websocketStore'
import toastStore from '@/features/stores/toast'
import homeStore from '@/features/stores/home'
import { useSilenceDetection } from './useSilenceDetection'
import { processAudio, base64EncodeAudio } from '@/utils/audioProcessing'
import { useAudioProcessing } from './useAudioProcessing'
import { SpeakQueue } from '@/features/messages/speakQueue'
import { getVoiceLanguageCode } from '@/utils/voiceLanguage'

/**
 * リアルタイムAPIを使用した音声認識のカスタムフック
 */
export function useRealtimeVoiceAPI(
  onChatProcessStart: (text: string) => void
) {
  const { t } = useTranslation()
  const selectLanguage = settingsStore((s) => s.selectLanguage)

  // ----- 状態管理 -----
  const [userMessage, setUserMessage] = useState('')
  const [isListening, setIsListening] = useState(false)
  const isListeningRef = useRef(false)

  // ----- 音声認識関連 -----
  const [recognition, setRecognition] = useState<SpeechRecognition | null>(null)
  const transcriptRef = useRef('')
  const speechDetectedRef = useRef<boolean>(false)
  const initialSpeechCheckTimerRef = useRef<NodeJS.Timeout | null>(null)
  // ----- stopListening関数の参照を保持（stale closure防止） -----
  const stopListeningRef = useRef<() => Promise<void>>(() => Promise.resolve())

  // ----- オーディオ処理フックを使用 -----
  const {
    audioContext,
    checkMicrophonePermission,
    startRecording,
    stopRecording,
  } = useAudioProcessing()

  // ----- オーディオバッファ用 -----
  const audioBufferRef = useRef<Float32Array | null>(null)

  // ----- キーボードトリガー関連 -----
  const keyPressStartTime = useRef<number | null>(null)
  const isKeyboardTriggered = useRef(false)

  // ----- テキストをWebSocketで送信する関数 -----
  const sendTextToWebSocket = useCallback((text: string) => {
    const wsManager = webSocketStore.getState().wsManager
    const ss = settingsStore.getState()

    if (wsManager?.websocket?.readyState !== WebSocket.OPEN) {
      console.error('WebSocket is not open')
      return
    }

    let sendContent: { type: string; text?: string; audio?: string }[] = []

    if (ss.realtimeAPIModeContentType === 'input_text' || text.trim()) {
      console.log(
        'Sending text through WebSocket from silence detection:',
        text
      )
      sendContent = [{ type: 'input_text', text: text.trim() }]
    }

    if (sendContent.length > 0) {
      wsManager.websocket.send(
        JSON.stringify({
          type: 'conversation.item.create',
          item: {
            type: 'message',
            role: 'user',
            content: sendContent,
          },
        })
      )
      wsManager.websocket.send(
        JSON.stringify({
          type: 'response.create',
        })
      )
    }
  }, [])

  // ----- 無音検出時のテキスト処理コールバック（メモ化して無限ループ防止） -----
  const handleTextDetected = useCallback(
    (text: string) => {
      // 検出されたテキストを元の onChatProcessStart に渡す前に、WebSocketで送信する処理を追加
      sendTextToWebSocket(text)
      // 元のコールバックも呼び出す
      onChatProcessStart(text)
    },
    [sendTextToWebSocket, onChatProcessStart]
  )

  // ----- 無音検出フックを使用 -----
  const {
    silenceTimeoutRemaining,
    clearSilenceDetection,
    startSilenceDetection,
    updateSpeechTimestamp,
    isSpeechEnded,
  } = useSilenceDetection({
    onTextDetected: handleTextDetected,
    transcriptRef,
    setUserMessage,
    speechDetectedRef,
  })

  // ----- 初期音声検出タイマーをクリアする関数 -----
  const clearInitialSpeechCheckTimer = useCallback(() => {
    if (initialSpeechCheckTimerRef.current) {
      clearTimeout(initialSpeechCheckTimerRef.current)
      initialSpeechCheckTimerRef.current = null
    }
  }, [])

  // ----- リアルタイムAPI用の音声データ送信 -----
  const sendAudioBuffer = useCallback(() => {
    console.log('sendAudioBuffer')
    if (!audioBufferRef.current || audioBufferRef.current.length === 0) {
      console.error('音声バッファが空です')
      return
    }

    const base64Chunk = base64EncodeAudio(audioBufferRef.current)
    const ss = settingsStore.getState()
    const wsManager = webSocketStore.getState().wsManager

    if (wsManager?.websocket?.readyState !== WebSocket.OPEN) {
      return
    }

    let sendContent: { type: string; text?: string; audio?: string }[] = []

    if (ss.realtimeAPIModeContentType === 'input_audio') {
      console.log('Sending buffer. Length:', audioBufferRef.current.length)
      sendContent = [{ type: 'input_audio', audio: base64Chunk }]
    } else {
      const currentText = transcriptRef.current.trim()
      if (currentText) {
        console.log('Sending text. userMessage:', currentText)
        sendContent = [{ type: 'input_text', text: currentText }]
      }
    }

    if (sendContent.length > 0) {
      wsManager.websocket.send(
        JSON.stringify({
          type: 'conversation.item.create',
          item: {
            type: 'message',
            role: 'user',
            content: sendContent,
          },
        })
      )
      wsManager.websocket.send(
        JSON.stringify({
          type: 'response.create',
        })
      )
    }

    audioBufferRef.current = null // 送信後にバッファをクリア
  }, [])

  // ----- 音声認識停止処理 -----
  const stopListening = useCallback(async () => {
    console.log('🛑 useRealtimeVoiceAPI: stopListening 呼び出し開始')

    // リスニング状態を先に更新して、新しいデータ収集を防止
    isListeningRef.current = false
    setIsListening(false)

    // 各種タイマーをクリア
    clearSilenceDetection()
    clearInitialSpeechCheckTimer()

    // 音声認識を停止 - まずrecognitionから
    if (recognition) {
      try {
        console.log('🎙️ 音声認識を停止します')
        recognition.stop()
        console.log('🎙️ 音声認識停止成功')
      } catch (error) {
        console.error('🔴 音声認識停止エラー:', error)
      }
    }

    // 録音を停止して音声データを取得
    console.log('🎤 MediaRecorderを停止します')
    const audioBlob = await stopRecording()
    console.log(
      '🎤 MediaRecorder停止完了',
      audioBlob ? `サイズ: ${audioBlob.size}` : 'データなし'
    )

    // 音声データが存在する場合のみ処理
    if (audioBlob && audioContext) {
      try {
        console.log('🔊 音声データを処理します')
        const arrayBuffer = await audioBlob.arrayBuffer()
        const audioBuffer = await audioContext.decodeAudioData(arrayBuffer)
        const processedData = processAudio(audioBuffer)
        audioBufferRef.current = processedData

        // 音声データ送信
        sendAudioBuffer()
      } catch (error) {
        console.error('🔴 音声データ処理エラー:', error)
      }
    } else {
      console.log('⚠️ 有効な音声データが取得できませんでした')
    }

    // キーボードトリガーの場合の処理
    const trimmedTranscriptRef = transcriptRef.current.trim()
    if (isKeyboardTriggered.current) {
      const pressDuration = Date.now() - (keyPressStartTime.current || 0)
      // 押してから1秒以上 かつ 文字が存在する場合のみ送信
      // 無音検出による自動送信が既に行われていない場合のみ送信する
      if (pressDuration >= 1000 && trimmedTranscriptRef && !isSpeechEnded()) {
        console.log('⌨️ キーボードトリガーによる送信:', trimmedTranscriptRef)
        onChatProcessStart(trimmedTranscriptRef)
        setUserMessage('')
      }
      isKeyboardTriggered.current = false
    }

    console.log('🏁 useRealtimeVoiceAPI: stopListening 処理完了')
  }, [
    clearSilenceDetection,
    clearInitialSpeechCheckTimer,
    recognition,
    audioContext,
    stopRecording,
    sendAudioBuffer,
    isSpeechEnded,
    onChatProcessStart,
  ])

  // stopListeningRef更新はeffectで（render中refアクセス禁止lint対策）
  useIsomorphicLayoutEffect(() => {
    stopListeningRef.current = stopListening
  }, [stopListening])

  // ----- 音声認識開始処理 -----
  const startListening = useCallback(async () => {
    const hasPermission = await checkMicrophonePermission()
    if (!hasPermission) return

    if (!recognition || !audioContext) return

    // 既に認識が開始されている場合は、一度停止してから再開する
    if (isListeningRef.current) {
      try {
        recognition.stop()
        // 停止完了を待つための短い遅延
        await new Promise((resolve) => setTimeout(resolve, 100))
      } catch (err) {
        console.log('Recognition was not running, proceeding to start', err)
      }
    }

    // トランスクリプトをリセット
    transcriptRef.current = ''
    setUserMessage('')

    try {
      recognition.start()
      console.log('Recognition started successfully')
      // リスニング状態を更新
      isListeningRef.current = true
      setIsListening(true)
    } catch (error) {
      console.error('Error starting recognition:', error)

      // InvalidStateErrorの場合は、既に開始されているとみなす
      if (error instanceof DOMException && error.name === 'InvalidStateError') {
        console.log('Recognition is already running, skipping retry')
        // 既に実行中なので、リスニング状態を更新する
        isListeningRef.current = true
        setIsListening(true)
      } else {
        // その他のエラーの場合のみ再試行
        setTimeout(() => {
          try {
            if (recognition) {
              recognition.start()
              console.log('Recognition started on retry')
              isListeningRef.current = true
              setIsListening(true)
            }
          } catch (retryError) {
            console.error('Failed to start recognition on retry:', retryError)
            isListeningRef.current = false
            setIsListening(false)
            return
          }
        }, 300)
      }
    }

    // 録音を開始
    const success = await startRecording({ mimeType: 'audio/webm' })
    if (!success) {
      console.error('Failed to start recording')
      toastStore.getState().addToast({
        message: t('Toasts.SpeechRecognitionError'),
        type: 'error',
        tag: 'speech-recognition-error',
      })
    }
  }, [recognition, audioContext, checkMicrophonePermission, startRecording, t])

  // ----- 音声認識トグル処理 -----
  const toggleListening = useCallback(() => {
    if (isListeningRef.current) {
      stopListening()
    } else {
      keyPressStartTime.current = Date.now()
      isKeyboardTriggered.current = true
      startListening()
      // AIの発話を停止
      homeStore.setState({ isSpeaking: false })
      SpeakQueue.stopAll()
    }
  }, [startListening, stopListening])

  // ----- メッセージ送信 -----
  const handleSendMessage = useCallback(() => {
    if (userMessage.trim()) {
      // AIの発話を停止
      homeStore.setState({ isSpeaking: false })
      SpeakQueue.stopAll()
      onChatProcessStart(userMessage)
      setUserMessage('')
    }
  }, [userMessage, onChatProcessStart])

  // ----- メッセージ入力 -----
  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      setUserMessage(e.target.value)
    },
    []
  )

  // ----- 音声認識オブジェクトの初期化とイベントハンドラ設定 -----
  const speechRecognitionMode = settingsStore(
    (s) => s.speechRecognitionMode
  )
  const realtimeAPIMode = settingsStore((s) => s.realtimeAPIMode)

  useEffect(() => {
    // realtimeAPI + browser モードの場合のみ初期化
    if (speechRecognitionMode !== 'browser' || !realtimeAPIMode) {
      return
    }

    const SpeechRecognition =
      window.SpeechRecognition || window.webkitSpeechRecognition

    if (!SpeechRecognition) {
      // 統一されたエラーハンドリングパターン (Requirement 8)
      console.error('Speech Recognition API is not supported in this browser')
      toastStore.getState().addToast({
        message: t('Toasts.SpeechRecognitionNotSupported'),
        type: 'error',
        tag: 'speech-recognition-not-supported',
      })
      return
    }

    const newRecognition = new SpeechRecognition()
    newRecognition.lang = getVoiceLanguageCode(selectLanguage)
    newRecognition.continuous = true
    newRecognition.interimResults = true

    // ----- イベントハンドラの設定 -----

    // 音声認識開始時
    newRecognition.onstart = () => {
      console.log('Speech recognition started')

      // 無音検出開始（refを使用してstale closure防止）
      startSilenceDetection(() => stopListeningRef.current())
    }

    // 音声認識結果が得られたとき
    newRecognition.onresult = (event) => {
      if (!isListeningRef.current) return

      const transcript = Array.from(event.results)
        .map((result) => result[0].transcript)
        .join('')

      updateSpeechTimestamp()
      speechDetectedRef.current = true
      transcriptRef.current = transcript
      setUserMessage(transcript)
    }

    // 音声認識終了時
    newRecognition.onend = () => {
      console.log('Recognition ended')
      clearSilenceDetection()
    }

    setRecognition(newRecognition)

    // クリーンアップ関数
    return () => {
      try {
        if (newRecognition) {
          newRecognition.abort()
        }
      } catch (error) {
        console.error('Error cleaning up speech recognition:', error)
      }
      clearSilenceDetection()
    }
  }, [
    speechRecognitionMode,
    realtimeAPIMode,
    selectLanguage,
    clearSilenceDetection,
    startSilenceDetection,
    // stopListeningは依存配列から除去（無限ループ防止、stopListeningRefを使用）
    updateSpeechTimestamp,
    t,
  ])

  // WebSocketの準備ができているかを確認
  const isWebSocketReady = useCallback(() => {
    const wsManager = webSocketStore.getState().wsManager
    return wsManager?.websocket?.readyState === WebSocket.OPEN
  }, [])

  // 戻り値オブジェクトをメモ化（Requirement 1.3, 1.4）
  const returnValue = useMemo(
    () => ({
      userMessage,
      isListening,
      silenceTimeoutRemaining,
      handleInputChange,
      handleSendMessage,
      toggleListening,
      startListening,
      stopListening,
      isWebSocketReady,
    }),
    [
      userMessage,
      isListening,
      silenceTimeoutRemaining,
      handleInputChange,
      handleSendMessage,
      toggleListening,
      startListening,
      stopListening,
      isWebSocketReady,
    ]
  )

  return returnValue
}
