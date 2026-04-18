import { useCallback, useEffect, useState } from 'react'
import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import type {
  ExpressionConfigItem,
  PoseConfigItem,
} from '@/features/stores/settings'

/**
 * 表情とポーズを1つのパネルから独立に操作できるコントローラー。
 * ポーズ選択は表情を上書きしない（表情クリックでのみ表情を変更）。
 */

function applyExpression(config: ExpressionConfigItem) {
  const model = homeStore.getState().viewer?.model
  if (!model?.vrm) return
  if (typeof config.expression === 'string') {
    model.emoteController?.playEmotion(config.expression)
  } else {
    model.emoteController?.playEmotionMix(config.expression)
  }
}

function resetExpression() {
  homeStore.getState().viewer?.model?.emoteController?.playEmotion('neutral')
}

async function applyPose(poseName: string, config: PoseConfigItem) {
  const model = homeStore.getState().viewer?.model
  if (!model?.vrm || !model.mixer) return
  model.poseYRotationOffset = 0
  await model.poseManager.applyPose(model, poseName, config)
}

function resetPose() {
  const model = homeStore.getState().viewer?.model
  if (!model?.mixer) return
  if (model.poseManager.isActive) {
    model.poseManager.resetToIdle(model)
  }
}

export default function CharacterController() {
  const expressionConfigs = settingsStore((s) => s.expressionConfigs)
  const poseConfigs = settingsStore((s) => s.poseConfigs)
  const [activeExpr, setActiveExpr] = useState<string | null>(null)
  const [activePose, setActivePose] = useState<string | null>(null)

  const handleExpr = useCallback(
    (config: ExpressionConfigItem) => {
      if (activeExpr === config.id) {
        resetExpression()
        setActiveExpr(null)
        return
      }
      applyExpression(config)
      setActiveExpr(config.id)
    },
    [activeExpr]
  )

  const handlePose = useCallback(
    async (config: PoseConfigItem) => {
      if (activePose === config.id) {
        resetPose()
        setActivePose(null)
        return
      }
      try {
        await applyPose(config.id, config)
        setActivePose(config.id)
      } catch (e) {
        console.error('Failed to apply pose:', e)
      }
    },
    [activePose]
  )

  const handleResetAll = useCallback(() => {
    resetExpression()
    resetPose()
    setActiveExpr(null)
    setActivePose(null)
  }, [])

  useEffect(() => {
    return () => {
      resetExpression()
      resetPose()
    }
  }, [])

  const sectionClass =
    'flex flex-col gap-1.5 bg-white/90 backdrop-blur-md rounded-2xl p-2.5 shadow-xl pointer-events-auto'
  const labelClass =
    'px-1 text-[10px] font-bold uppercase tracking-[0.15em] text-gray-500'
  const chipBase =
    'rounded-lg px-3 py-1.5 text-xs font-semibold transition-colors text-left'
  const chipActive = 'bg-gray-900 text-white shadow-sm'
  const chipIdle = 'bg-gray-100 text-gray-800 hover:bg-gray-200'

  return (
    <div className="pointer-events-none fixed inset-y-0 right-3 z-40 flex items-center">
      <div className="flex max-h-[90vh] flex-col gap-2 overflow-y-auto py-4">
        <div className={sectionClass}>
          <div className={labelClass}>Expression</div>
          <div className="grid grid-cols-2 gap-1.5">
            {expressionConfigs.map((c) => (
              <button
                key={c.id}
                onClick={() => handleExpr(c)}
                className={`${chipBase} ${activeExpr === c.id ? chipActive : chipIdle}`}
              >
                {c.id}
              </button>
            ))}
          </div>
        </div>

        <div className={sectionClass}>
          <div className={labelClass}>Pose</div>
          <div className="grid grid-cols-2 gap-1.5">
            {poseConfigs.map((c) => (
              <button
                key={c.id}
                onClick={() => handlePose(c)}
                className={`${chipBase} ${activePose === c.id ? chipActive : chipIdle}`}
              >
                {c.id}
              </button>
            ))}
          </div>
        </div>

        <button
          onClick={handleResetAll}
          className="pointer-events-auto rounded-2xl bg-gray-900 px-3 py-2 text-xs font-bold text-white shadow-xl hover:bg-gray-700"
        >
          Reset All
        </button>
      </div>
    </div>
  )
}
