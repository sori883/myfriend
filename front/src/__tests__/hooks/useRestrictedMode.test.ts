import { renderHook } from '@testing-library/react'
import { useRestrictedMode } from '@/hooks/useRestrictedMode'

describe('useRestrictedMode', () => {
  const originalEnv = process.env

  beforeEach(() => {
    jest.resetModules()
    process.env = { ...originalEnv }
  })

  afterAll(() => {
    process.env = originalEnv
  })

  it('returns isRestrictedMode as false (stubbed after Vercel migration)', () => {
    const { result } = renderHook(() => useRestrictedMode())
    expect(result.current.isRestrictedMode).toBe(false)
  })

  it('ignores NEXT_PUBLIC_RESTRICTED_MODE environment variable', () => {
    process.env.NEXT_PUBLIC_RESTRICTED_MODE = 'true'
    const { result } = renderHook(() => useRestrictedMode())
    expect(result.current.isRestrictedMode).toBe(false)
  })

  it('memoizes the result across re-renders', () => {
    const { result, rerender } = renderHook(() => useRestrictedMode())
    const firstResult = result.current

    rerender()
    expect(result.current).toBe(firstResult)
  })
})
