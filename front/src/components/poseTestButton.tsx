import { useCallback, useEffect, useRef, useState } from 'react'
import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import type { PoseConfigItem } from '@/features/stores/settings'

function usePoseToggle() {
  const [activePose, setActivePose] = useState<string | null>(null)
  const activePoseRef = useRef<string | null>(null)

  useEffect(() => {
    activePoseRef.current = activePose
  }, [activePose])

  const resetToIdle = useCallback(() => {
    const { viewer } = homeStore.getState()
    const model = viewer.model
    if (!model?.mixer) return

    model.poseManager.resetToIdle(model)
    model.emoteController?.playEmotion('neutral')
    setActivePose(null)
  }, [])

  const applyPose = useCallback(
    async (poseName: string, poseConfig: PoseConfigItem) => {
      const { viewer } = homeStore.getState()
      const model = viewer.model
      if (!model?.vrm || !model.mixer) return

      if (activePoseRef.current === poseName) {
        resetToIdle()
        return
      }

      await model.poseManager.applyPose(model, poseName, poseConfig)
      if (poseConfig.expression) {
        if (typeof poseConfig.expression === 'string') {
          model.emoteController?.playEmotion(poseConfig.expression)
        } else {
          model.emoteController?.playEmotionMix(poseConfig.expression)
        }
      }
      setActivePose(poseName)
    },
    [resetToIdle]
  )

  return { activePose, applyPose, resetToIdle }
}

export default function PoseTestButton() {
  const { activePose, applyPose, resetToIdle } = usePoseToggle()
  const poseConfigs = settingsStore((s) => s.poseConfigs)

  const handlePoseClick = useCallback(
    async (poseConfig: PoseConfigItem) => {
      const { viewer } = homeStore.getState()
      if (viewer.model) {
        viewer.model.poseYRotationOffset = 0
      }
      try {
        await applyPose(poseConfig.id, poseConfig)
      } catch (e) {
        console.error('Failed to apply pose:', e)
      }
    },
    [applyPose]
  )

  const resetToIdleRef = useRef(resetToIdle)
  resetToIdleRef.current = resetToIdle
  useEffect(() => {
    return () => resetToIdleRef.current()
  }, [])

  return (
    <div className="fixed top-0 right-0 bottom-0 z-50 flex items-center">
      <div className="flex flex-col gap-2 mr-4">
        {poseConfigs.map((pose) => (
          <button
            key={pose.id}
            onClick={() => handlePoseClick(pose)}
            className={`rounded-xl px-4 py-2 font-bold text-white shadow-lg text-sm ${
              activePose === pose.id
                ? 'bg-secondary hover:bg-secondary-hover'
                : 'bg-primary hover:bg-primary-hover active:bg-primary-press'
            }`}
          >
            {activePose === pose.id ? 'Idle' : pose.id}
          </button>
        ))}
      </div>
    </div>
  )
}
