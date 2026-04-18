import {
  isRestrictedMode,
  createRestrictedModeErrorResponse,
  RestrictedModeErrorResponse,
} from '@/utils/restrictedMode'

describe('restrictedMode', () => {
  describe('isRestrictedMode', () => {
    it('returns false (stubbed after Vercel migration)', () => {
      expect(isRestrictedMode()).toBe(false)
    })

    it('ignores NEXT_PUBLIC_RESTRICTED_MODE environment variable', () => {
      const original = process.env.NEXT_PUBLIC_RESTRICTED_MODE
      process.env.NEXT_PUBLIC_RESTRICTED_MODE = 'true'
      expect(isRestrictedMode()).toBe(false)
      if (original === undefined) {
        delete process.env.NEXT_PUBLIC_RESTRICTED_MODE
      } else {
        process.env.NEXT_PUBLIC_RESTRICTED_MODE = original
      }
    })
  })

  describe('createRestrictedModeErrorResponse', () => {
    it('returns correct error response structure', () => {
      const response = createRestrictedModeErrorResponse('upload-image')

      expect(response).toEqual({
        error: 'feature_disabled_in_restricted_mode',
        message: expect.any(String),
      })
    })

    it('includes feature name in message', () => {
      const response = createRestrictedModeErrorResponse('upload-image')

      expect(response.message).toContain('upload-image')
    })

    it('satisfies RestrictedModeErrorResponse type', () => {
      const response: RestrictedModeErrorResponse =
        createRestrictedModeErrorResponse('test')

      expect(response.error).toBe('feature_disabled_in_restricted_mode')
      expect(typeof response.message).toBe('string')
    })
  })
})
