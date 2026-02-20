import { useCallback, useEffect } from 'react'

import homeStore from '@/features/stores/home'
import settingsStore from '@/features/stores/settings'

export default function VrmViewer() {
  const chatLogWidth = settingsStore((s) => s.chatLogWidth)

  const canvasRef = useCallback((canvas: HTMLCanvasElement) => {
    if (canvas) {
      const { viewer } = homeStore.getState()
      const { selectedVrmPath } = settingsStore.getState()
      viewer.setup(canvas)
      viewer.loadVrm(selectedVrmPath)

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
          viewer.loadVrm(url)
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

  // chatLogWidth 変更時に Three.js レンダラーをリサイズ
  useEffect(() => {
    const { viewer } = homeStore.getState()
    // レイアウト反映を待ってからリサイズ
    const id = requestAnimationFrame(() => viewer.resize())
    return () => cancelAnimationFrame(id)
  }, [chatLogWidth])

  return (
    <div
      className={'absolute top-0 h-[100svh] z-5'}
      style={{
        left: `${chatLogWidth}px`,
        width: `calc(100vw - ${chatLogWidth}px)`,
      }}
    >
      <canvas ref={canvasRef} className={'h-full w-full'}></canvas>
    </div>
  )
}
