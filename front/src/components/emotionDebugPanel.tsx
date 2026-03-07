import { EMOTIONS, EmotionType } from '@/features/messages/messages'
import homeStore from '@/features/stores/home'

const EMOTION_LABELS: Record<EmotionType, string> = {
  neutral: '😐',
  happy: '😊',
  angry: '😠',
  sad: '😢',
  relaxed: '😌',
  surprised: '😲',
}

export const EmotionDebugPanel = () => {
  const handleEmotion = (emotion: EmotionType) => {
    const viewer = homeStore.getState().viewer
    viewer.model?.playEmotion(emotion)
  }

  return (
    <div className="absolute bottom-4 right-4 z-30 flex gap-1 rounded-lg bg-black/60 p-2">
      {EMOTIONS.map((emotion) => (
        <button
          key={emotion}
          onClick={() => handleEmotion(emotion)}
          className="rounded-md px-2 py-1 text-lg hover:bg-white/20 active:bg-white/30"
          title={emotion}
        >
          {EMOTION_LABELS[emotion]}
        </button>
      ))}
    </div>
  )
}
