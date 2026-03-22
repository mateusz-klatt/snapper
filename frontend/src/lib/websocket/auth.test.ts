import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { isAuthControlMessage, getWsToken } from './auth'
import * as wsTicketCache from '../wsTicketCache'
import { apiClient } from '../apiClient'

vi.mock('../wsTicketCache', () => ({
  consumeWsTicket: vi.fn(),
}))
vi.mock('../apiClient', () => ({
  apiClient: {
    postJSON: vi.fn(),
  },
}))
describe('auth', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })
  describe('isAuthControlMessage', () => {
    it('returns true for auth_required message', () => {
      expect(isAuthControlMessage({ type: 'auth_required' })).toBe(true)
    })
    it('returns true for auth_ok message', () => {
      expect(isAuthControlMessage({ type: 'auth_ok' })).toBe(true)
    })
    it('returns true for auth_complete message', () => {
      expect(isAuthControlMessage({ type: 'auth_complete' })).toBe(true)
    })
    it('returns true for auth_failed message', () => {
      expect(isAuthControlMessage({ type: 'auth_failed' })).toBe(true)
    })
    it('returns true for auth_expired message', () => {
      expect(isAuthControlMessage({ type: 'auth_expired' })).toBe(true)
    })
    it('returns true for reauth_required message', () => {
      expect(isAuthControlMessage({ type: 'reauth_required' })).toBe(true)
    })
    it('returns false for candle message', () => {
      expect(isAuthControlMessage({ type: 'candle' })).toBe(false)
    })
    it('returns false for heartbeat message', () => {
      expect(isAuthControlMessage({ type: 'heartbeat' })).toBe(false)
    })
    it('returns false for unknown message type', () => {
      expect(isAuthControlMessage({ type: 'unknown_type' })).toBe(false)
    })
  })
  describe('getWsToken', () => {
    it('returns token from cache when available', async () => {
      const cachedTicket = { token: 'cached-token', exp: 1234567890 }

      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(cachedTicket)
      const result = await getWsToken()

      expect(result).toEqual({ token: 'cached-token', exp: 1234567890 })
      expect(apiClient.postJSON).not.toHaveBeenCalled()
    })
    it('fetches token from API when cache is empty', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      const expDate = new Date('2026-01-07T12:00:00Z')

      vi.mocked(apiClient.postJSON).mockResolvedValue({
        payload: {
          ws_token: 'fetched-token',
          ws_token_exp: expDate.toISOString(),
        },
      })
      const result = await getWsToken()

      expect(result).toEqual({ token: 'fetched-token', exp: Math.floor(expDate.getTime() / 1000) })
      expect(apiClient.postJSON).toHaveBeenCalledWith('/api/auth/refresh', undefined, {
        skipRetry: true,
      })
    })
    it('throws error for invalid API response', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      vi.mocked(apiClient.postJSON).mockResolvedValue({ payload: { invalid: 'response' } })
      await expect(getWsToken()).rejects.toThrow('Invalid ws_token response from refresh endpoint')
    })
    it('throws error when API returns null', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      vi.mocked(apiClient.postJSON).mockResolvedValue(null)
      await expect(getWsToken()).rejects.toThrow('Invalid ws_token response from refresh endpoint')
    })
    it('throws error when ws_token is not a string', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      vi.mocked(apiClient.postJSON).mockResolvedValue({
        payload: {
          ws_token: 12345,
          ws_token_exp: new Date().toISOString(),
        },
      })
      await expect(getWsToken()).rejects.toThrow('Invalid ws_token response from refresh endpoint')
    })
    it('throws error when ws_token_exp is not a string', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      vi.mocked(apiClient.postJSON).mockResolvedValue({
        payload: {
          ws_token: 'valid-token',
          ws_token_exp: 12345,
        },
      })
      await expect(getWsToken()).rejects.toThrow('Invalid ws_token response from refresh endpoint')
    })
    it('deduplicates concurrent requests', async () => {
      vi.mocked(wsTicketCache.consumeWsTicket).mockReturnValue(null)
      const expDate = new Date('2026-01-07T12:00:00Z')

      vi.mocked(apiClient.postJSON).mockImplementation(
        () =>
          new Promise(resolve =>
            setTimeout(
              () =>
                resolve({
                  payload: {
                    ws_token: 'dedup-token',
                    ws_token_exp: expDate.toISOString(),
                  },
                }),
              50
            )
          )
      )
      const [result1, result2] = await Promise.all([getWsToken(), getWsToken()])
      const expectedExp = Math.floor(expDate.getTime() / 1000)

      expect(result1).toEqual({ token: 'dedup-token', exp: expectedExp })
      expect(result2).toEqual({ token: 'dedup-token', exp: expectedExp })
      expect(apiClient.postJSON).toHaveBeenCalledTimes(1)
    })
  })
})
