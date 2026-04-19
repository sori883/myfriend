import { useCallback } from 'react'

import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'
import { loadVRMAnimation } from '@/lib/VRMAnimation/loadVRMAnimation'
import { resolveVrmSource } from '@/utils/vrmResolver'
import CharacterController from '@/components/characterController'

export default function VrmViewer() {
  const isVrmLoading = homeStore((s) => s.isVrmLoading)

  const canvasRef = useCallback((canvas: HTMLCanvasElement) => {
    if (canvas) {
      const { viewer, setIsVrmLoading } = homeStore.getState()
      const { selectedVrmPath } = settingsStore.getState()
      viewer.setup(canvas)

      setIsVrmLoading(true)
      resolveVrmSource(selectedVrmPath)
        .then((src) => viewer.loadVrm(src))
        .catch((err) => {
          console.error('Failed to resolve/load VRM:', err)
          return viewer.loadVrm(selectedVrmPath)
        })
        .finally(() => setIsVrmLoading(false))

      // Drag and DropでVRMを差し替え
      canvas.addEventListener('dragover', function (event) {
        event.preventDefault()
      })

      canvas.addEventListener('drop', function (event) {
        event.preventDefault()

        const files = event.dataTransfer?.files
        if (!files) {
          return
        }

        const file = files[0]
        if (!file) {
          return
        }
        const file_type = file.name.split('.').pop()
        if (file_type === 'vrm') {
          const blob = new Blob([file], { type: 'application/octet-stream' })
          const url = window.URL.createObjectURL(blob)
          setIsVrmLoading(true)
          viewer.loadVrm(url).finally(() => setIsVrmLoading(false))
        } else if (file_type === 'vrma') {
          const blob = new Blob([file], { type: 'application/octet-stream' })
          const url = window.URL.createObjectURL(blob)
          loadVRMAnimation(url)
            .then((vrma) => {
              if (vrma) viewer.model?.loadAnimation(vrma)
            })
            .catch((error) => {
              console.error('Failed to load VRMA:', error)
            })
            .finally(() => URL.revokeObjectURL(url))
        } else if (file.type.startsWith('image/')) {
          const reader = new FileReader()
          reader.readAsDataURL(file)
          reader.onload = function () {
            const image = reader.result as string
            image !== '' && homeStore.setState({ modalImage: image })
          }
        }
      })
    }
  }, [])

  const poseAdjustMode = settingsStore((s) => s.poseAdjustMode)

  return (
    <>
      <div className={'absolute top-0 left-0 w-screen h-[100svh] z-5'}>
        <canvas ref={canvasRef} className={'h-full w-full'}></canvas>
      </div>
      {isVrmLoading && (
        <div className="absolute inset-0 z-30 flex flex-col items-center justify-center gap-4 bg-black/40 backdrop-blur-sm pointer-events-none">
          <div className="h-14 w-14 animate-spin rounded-full border-4 border-white/30 border-t-white" />
          <p className="text-sm font-semibold text-white drop-shadow">
            モデルを読み込み中...
          </p>
        </div>
      )}
      {poseAdjustMode && <CharacterController />}
    </>
  )
}
