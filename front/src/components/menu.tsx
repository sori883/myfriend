import React, { useCallback, useState, useEffect } from 'react'

import homeStore from '@/features/stores/home'
import menuStore from '@/features/stores/menu'
import settingsStore from '@/features/stores/settings'
import { ConversationBubble } from './conversationBubble'
import Settings from './settings'
import { useKioskMode } from '@/hooks/useKioskMode'

const useIsMobile = () => {
  const [isMobile, setIsMobile] = useState<boolean | null>(null)

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(
        window.innerWidth <= 768 ||
          /Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
      )
    }

    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])

  return isMobile
}

export const Menu = () => {
  const showAssistantText = settingsStore((s) => s.showAssistantText)

  const { canAccessSettings } = useKioskMode()

  const [showSettings, setShowSettings] = useState(false)

  useEffect(() => {
    if (!canAccessSettings) setShowSettings(false)
  }, [canAccessSettings])

  const [touchStartTime, setTouchStartTime] = useState<number | null>(null)
  const isMobile = useIsMobile()

  const handleTouchStart = () => {
    if (!canAccessSettings) return
    setTouchStartTime(Date.now())
  }

  const handleTouchEnd = () => {
    if (!canAccessSettings) return
    if (touchStartTime && Date.now() - touchStartTime >= 800) {
      setShowSettings(true)
    }
    setTouchStartTime(null)
  }

  const handleTouchCancel = () => setTouchStartTime(null)

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === '.') {
        if (!canAccessSettings) return
        setShowSettings((prev) => !prev)
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [canAccessSettings])

  const handleChangeVrmFile = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const files = event.target.files
      if (!files) return

      const file = files[0]
      if (!file) return

      const file_type = file.name.split('.').pop()
      if (file_type === 'vrm') {
        const blob = new Blob([file], { type: 'application/octet-stream' })
        const url = window.URL.createObjectURL(blob)
        homeStore.getState().viewer.loadVrm(url)
      }

      event.target.value = ''
    },
    []
  )

  return (
    <>
      {isMobile === true && (
        <div
          className="absolute top-0 left-0 z-30 w-20 h-20"
          onTouchStart={handleTouchStart}
          onTouchEnd={handleTouchEnd}
          onTouchCancel={handleTouchCancel}
        >
          <div className="w-full h-full opacity-0"></div>
        </div>
      )}

      {showSettings && canAccessSettings && (
        <Settings onClickClose={() => setShowSettings(false)} />
      )}
      {showAssistantText && <ConversationBubble />}
      <input
        type="file"
        className="hidden"
        accept=".vrm"
        ref={(fileInput) => {
          if (!fileInput) {
            menuStore.setState({ fileInput: null })
            return
          }
          menuStore.setState({ fileInput })
        }}
        onChange={handleChangeVrmFile}
      />
      <input
        type="file"
        className="hidden"
        accept="image/*"
        ref={(bgFileInput) => {
          if (!bgFileInput) {
            menuStore.setState({ bgFileInput: null })
            return
          }
          menuStore.setState({ bgFileInput })
        }}
        onChange={(e) => {
          const file = e.target.files?.[0]
          if (file) {
            const imageUrl = URL.createObjectURL(file)
            homeStore.setState({ backgroundImageUrl: imageUrl })
          }
        }}
      />
    </>
  )
}
