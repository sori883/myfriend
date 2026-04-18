import { useCallback, useEffect, useRef, useState } from 'react'
import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import type { ExpressionConfigItem } from '@/features/stores/settings'

function applyExpression(config: ExpressionConfigItem) {
  const { viewer } = homeStore.getState()
  const model = viewer.model
  if (!model?.vrm) return

  if (typeof config.expression === 'string') {
    model.emoteController?.playEmotion(config.expression)
  } else {
    model.emoteController?.playEmotionMix(config.expression)
  }
}

function resetExpression() {
  const { viewer } = homeStore.getState()
  const model = viewer.model
  if (!model?.vrm) return
  model.emoteController?.playEmotion('neutral')
}

export default function ExpressionTestButton() {
  const [activeId, setActiveId] = useState<string | null>(null)
  const activeIdRef = useRef<string | null>(null)

  useEffect(() => {
    activeIdRef.current = activeId
  }, [activeId])

  const expressionConfigs = settingsStore((s) => s.expressionConfigs)

  const handleClick = useCallback((config: ExpressionConfigItem) => {
    if (activeIdRef.current === config.id) {
      resetExpression()
      setActiveId(null)
      return
    }
    applyExpression(config)
    setActiveId(config.id)
  }, [])

  useEffect(() => {
    return () => {
      resetExpression()
    }
  }, [])

  return (
    <div className="fixed top-0 left-0 bottom-0 z-50 flex items-center">
      <div className="flex flex-col gap-2 ml-4">
        {expressionConfigs.map((config) => (
          <button
            key={config.id}
            onClick={() => handleClick(config)}
            className={`rounded-xl px-4 py-2 font-bold text-white shadow-lg text-sm ${
              activeId === config.id
                ? 'bg-secondary hover:bg-secondary-hover'
                : 'bg-primary hover:bg-primary-hover active:bg-primary-press'
            }`}
          >
            {activeId === config.id ? 'Neutral' : config.id}
          </button>
        ))}
      </div>
    </div>
  )
}
