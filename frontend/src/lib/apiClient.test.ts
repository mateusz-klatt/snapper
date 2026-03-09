import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { getCookie } from './utils'

vi.mock('./utils', () => ({
  getCookie: vi.fn(() => 'test-csrf-token'),
}))
vi.mock('./wsTicketCache', () => ({
  storeWsTicket: vi.fn(),
}))
describe('APIClient', () => {
  let apiClient: typeof import('./apiClient').apiClient
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(async () => {
    vi.clearAllMocks()
    mockFetch = vi.fn()
    ;(globalThis as any).fetch = mockFetch
    Object.defineProperty(document, 'cookie', {
      writable: true,
      value: 'csrf_token=test-token; access_token=test-access',
    })
    const mod = await import('./apiClient')

    apiClient = mod.apiClient
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })
  describe('singleton pattern', () => {
    it('exports APIClient singleton', () => {
      expect(apiClient).toBeDefined()
    })
    it('returns same instance', async () => {
      const mod = await import('./apiClient')
      const instance1 = mod.apiClient
      const instance2 = mod.apiClient

      expect(instance1).toBe(instance2)
    })
    it('getInstance returns existing singleton', () => {
      const APIClient = (apiClient as any).constructor
      const instance = APIClient.getInstance()

      expect(instance).toBe(apiClient)
    })
  })
  describe('request method', () => {
    it('makes GET request without CSRF token', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'test' }),
      })
      await apiClient.request('/test', { method: 'GET' })
      expect(mockFetch).toHaveBeenCalledWith(
        '/test',
        expect.objectContaining({
          method: 'GET',
          credentials: 'include',
        })
      )
    })
    it('makes POST request with CSRF token', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'test' }),
      })
      await apiClient.request('/test', {
        method: 'POST',
        body: JSON.stringify({ test: 'data' }),
      })
      expect(mockFetch).toHaveBeenCalledWith(
        '/test',
        expect.objectContaining({
          method: 'POST',
          credentials: 'include',
        })
      )
    })
    it('adds Content-Type header for JSON requests', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.request('/test', {
        method: 'POST',
        body: JSON.stringify({ test: 'data' }),
      })
      const callArgs = mockFetch.mock.calls[0][1]
      const headers = callArgs.headers as Headers

      expect(headers.get('Content-Type')).toBe('application/json')
    })
    it('skips CSRF header when skipCSRF is true', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.request('/test', {
        method: 'POST',
        skipCSRF: true,
        body: JSON.stringify({ test: 'data' }),
      })
      const callArgs = mockFetch.mock.calls[0][1]
      const headers = callArgs.headers as Headers

      expect(headers.get('X-CSRF-Token')).toBeNull()
    })
    it('does not set CSRF header when cookie is missing', async () => {
      vi.mocked(getCookie).mockReturnValueOnce('')
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.request('/test', {
        method: 'POST',
        body: JSON.stringify({ test: 'data' }),
      })
      const callArgs = mockFetch.mock.calls[0][1]
      const headers = callArgs.headers as Headers

      expect(headers.get('X-CSRF-Token')).toBeNull()
    })
    it('defaults to GET when method is omitted', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.request('/test')
      const callArgs = mockFetch.mock.calls[0][1]
      const headers = callArgs.headers as Headers

      expect(callArgs.method).toBeUndefined()
      expect(headers.get('X-CSRF-Token')).toBeNull()
    })
    it('handles 401 with token refresh and retry', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
      })
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({
          ws_token: 'new-token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
        }),
        clone: function () {
          return {
            json: async () => ({
              ws_token: 'new-token',
              ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
            }),
          }
        },
      })
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'success' }),
      })
      const response = await apiClient.request('/test', { method: 'GET' })

      expect(response.ok).toBe(true)
      expect(mockFetch).toHaveBeenCalledTimes(3)
    })
    it('handles 401 when refresh fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
      })
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
      })
      await expect(apiClient.request('/test', { method: 'GET' })).rejects.toThrow(
        'Authentication required'
      )
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
    it('triggers auth logout callback with cooldown', async () => {
      vi.useFakeTimers()
      vi.resetModules()
      const freshApiClient = (await import('./apiClient')).apiClient
      const originalAuthLogout = (window as { authLogoutCallback?: () => void }).authLogoutCallback
      const authLogout = vi.fn()

      ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = authLogout

      const queueAuthFailure = () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 401,
        })
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 401,
        })
      }

      try {
        queueAuthFailure()
        await expect(freshApiClient.request('/test', { method: 'GET' })).rejects.toThrow(
          'Authentication required'
        )
        expect(authLogout).toHaveBeenCalledTimes(1)
        queueAuthFailure()
        await expect(freshApiClient.request('/test', { method: 'GET' })).rejects.toThrow(
          'Authentication required'
        )
        expect(authLogout).toHaveBeenCalledTimes(1)
        vi.advanceTimersByTime(1000)
        await vi.runAllTimersAsync()
        queueAuthFailure()
        await expect(freshApiClient.request('/test', { method: 'GET' })).rejects.toThrow(
          'Authentication required'
        )
        expect(authLogout).toHaveBeenCalledTimes(2)
      } finally {
        if (originalAuthLogout) {
          ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = originalAuthLogout
        } else {
          delete (window as { authLogoutCallback?: () => void }).authLogoutCallback
        }

        vi.useRealTimers()
      }
    })
    it('handles authentication failure without window', () => {
      vi.useFakeTimers()
      const originalWindow = globalThis.window

      vi.stubGlobal('window', undefined)

      try {
        const APIClientClass = (apiClient as any).constructor
        const instance = APIClientClass.getInstance()

        instance.isLoggingOut = false
        expect(() => instance.handleAuthenticationFailure()).not.toThrow()
        vi.runAllTimers()
      } finally {
        vi.stubGlobal('window', originalWindow)
        vi.useRealTimers()
      }
    })
    it('skips retry when skipRetry is true', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
      })
      const response = await apiClient.request('/test', {
        method: 'GET',
        skipRetry: true,
      })

      expect(response.status).toBe(401)
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
    it('handles 403 CSRF error with retry', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        clone: function () {
          return {
            json: async () => ({ detail: 'CSRF token invalid' }),
          }
        },
      })
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      const response = await apiClient.request('/test', { method: 'POST' })

      expect(response.ok).toBe(true)
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
    it('handles non-CSRF 403 error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        clone: function () {
          return {
            json: async () => ({ detail: 'Access denied' }),
          }
        },
      })
      await expect(apiClient.request('/test', { method: 'GET' })).rejects.toThrow('Access denied')
    })
  })
  describe('convenience methods', () => {
    it('provides GET convenience method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.get('/test')
      expect(mockFetch).toHaveBeenCalledWith('/test', expect.objectContaining({ method: 'GET' }))
    })
    it('provides POST convenience method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.post('/test', { data: 'test' })
      expect(mockFetch).toHaveBeenCalledWith('/test', expect.objectContaining({ method: 'POST' }))
    })
    it('provides POST convenience method without body', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.post('/test')
      const callArgs = mockFetch.mock.calls[0][1]

      expect(callArgs.body).toBeUndefined()
    })
    it('provides PUT convenience method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.put('/test', { data: 'test' })
      expect(mockFetch).toHaveBeenCalledWith('/test', expect.objectContaining({ method: 'PUT' }))
    })
    it('provides PUT convenience method without body', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.put('/test')
      const callArgs = mockFetch.mock.calls[0][1]

      expect(callArgs.body).toBeUndefined()
    })
    it('provides DELETE convenience method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
      })
      await apiClient.delete('/test')
      expect(mockFetch).toHaveBeenCalledWith('/test', expect.objectContaining({ method: 'DELETE' }))
    })
  })
  describe('JSON convenience methods', () => {
    it('provides getJSON method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'test' }),
      })
      const data = await apiClient.getJSON('/test')

      expect(data).toEqual({ data: 'test' })
    })
    it('throws on non-ok response in getJSON with detail message', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockResolvedValue({ detail: 'Database connection failed' }),
      })
      await expect(apiClient.getJSON('/test')).rejects.toThrow('Database connection failed')
    })
    it('throws on non-ok response in getJSON with message field', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockResolvedValue({ message: 'Server message without detail' }),
      })
      await expect(apiClient.getJSON('/test')).rejects.toThrow('Server message without detail')
    })
    it('throws status text when response has object without detail or message', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockResolvedValue({ other: 'field' }),
      })
      await expect(apiClient.getJSON('/test')).rejects.toThrow('HTTP 500: Internal Server Error')
    })
    it('throws status text when response json is not an object', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockResolvedValue('string response'),
      })
      await expect(apiClient.getJSON('/test')).rejects.toThrow('HTTP 500: Internal Server Error')
    })
    it('throws on non-ok response in getJSON without json body', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockRejectedValue(new Error('No JSON')),
      })
      await expect(apiClient.getJSON('/test')).rejects.toThrow('HTTP 500: Internal Server Error')
    })
    it('provides postJSON method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'test' }),
      })
      const data = await apiClient.postJSON('/test', { input: 'data' })

      expect(data).toEqual({ data: 'test' })
    })
    it('provides putJSON method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ data: 'test' }),
      })
      const data = await apiClient.putJSON('/test', { input: 'data' })

      expect(data).toEqual({ data: 'test' })
    })
    it('provides deleteJSON method', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ success: true }),
      })
      const data = await apiClient.deleteJSON('/test')

      expect(data).toEqual({ success: true })
    })
  })
  describe('cookie management', () => {
    it('checks for auth cookies', () => {
      Object.defineProperty(document, 'cookie', {
        writable: true,
        value: 'csrf_token=test; access_token=test',
      })
      expect(apiClient.hasAuthCookies()).toBe(true)
    })
    it('returns false when no auth cookies', () => {
      Object.defineProperty(document, 'cookie', {
        writable: true,
        value: 'other=value',
      })
      expect(apiClient.hasAuthCookies()).toBe(false)
    })
    it('provides clearCSRFToken method', () => {
      expect(typeof apiClient.clearCSRFToken).toBe('function')
      apiClient.clearCSRFToken()
    })
    it('provides setCsrfToken method', () => {
      expect(typeof apiClient.setCsrfToken).toBe('function')
      apiClient.setCsrfToken('test-token')
    })
  })
})
describe('api function', () => {
  let api: typeof import('./apiClient').api
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(async () => {
    vi.clearAllMocks()
    vi.mocked(getCookie).mockReturnValue('test-csrf')
    mockFetch = vi.fn()
    ;(globalThis as any).fetch = mockFetch
    const mod = await import('./apiClient')

    api = mod.api
  })
  it('makes request with CSRF token', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
    })
    await api('/test', { method: 'POST' })
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/test',
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
      })
    )
    const callArgs = mockFetch.mock.calls[0][1]
    const headers = callArgs.headers as Headers

    expect(headers.get('X-CSRF-Token')).toBe('test-csrf')
  })
  it('does not set CSRF header when cookie is missing', async () => {
    vi.mocked(getCookie).mockReturnValueOnce('')
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
    })
    await api('/test', { method: 'POST' })
    const callArgs = mockFetch.mock.calls[0][1]
    const headers = callArgs.headers as Headers

    expect(headers.get('X-CSRF-Token')).toBeNull()
  })
  it('handles 401 with retry', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
    })
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        ws_token: 'new-token',
        ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
      }),
      clone: function () {
        return {
          json: async () => ({
            ws_token: 'new-token',
            ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          }),
        }
      },
    })
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
    })
    const response = await api('/test')

    expect(response.ok).toBe(true)
  })
  it('returns 401 when refresh fails', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
    })
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
    })
    const response = await api('/test')

    expect(response.status).toBe(401)
  })
  it('adds Content-Type header for JSON body', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
    })
    await api('/test', {
      method: 'POST',
      body: JSON.stringify({ data: 'test' }),
    })
    const callArgs = mockFetch.mock.calls[0][1]
    const headers = callArgs.headers as Headers

    expect(headers.get('Content-Type')).toBe('application/json')
  })
  it('handles refresh network error', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
    })
    mockFetch.mockRejectedValueOnce(new Error('Network error'))
    const response = await api('/test')

    expect(response.status).toBe(401)
  })
})
describe('domain API methods', () => {
  let apiClient: typeof import('./apiClient').apiClient
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(async () => {
    vi.clearAllMocks()
    vi.mocked(getCookie).mockReturnValue('test-csrf')
    mockFetch = vi.fn()
    ;(globalThis as any).fetch = mockFetch
    const mod = await import('./apiClient')

    apiClient = mod.apiClient
  })
  it('getHealth returns health data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        status: 'healthy',
        timestamp: '2024-01-01T00:00:00Z',
        version: '1.0.0',
        connections: {
          active_connections: 5,
          zmq_subscribers: 2,
          subscriber_tasks: 1,
          active_topics: 3,
          active_clients: 4,
        },
        topics: { available: 10, active: 3 },
      }),
    })
    const result = await apiClient.getHealth()

    expect(result.status).toBe('healthy')
    expect(result.timestamp).toBe('2024-01-01T00:00:00Z')
  })
  it('getSystemStatus returns system status', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        trader: { status: 'running', pid: 1234 },
        backtests: {},
      }),
    })
    const result = await apiClient.getSystemStatus()

    expect(result.trader.status).toBe('running')
    expect(result.backtests).toEqual({})
  })
  it('getCandles returns candle data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          instrument: 'BTC/USD',
          exchange: 'kraken',
          timeframe: '1h',
          open_at: '2024-01-01T00:00:00Z',
          open: 1,
          high: 1.1,
          low: 0.9,
          close: 1.05,
          volume: 1000,
        },
      ],
    })
    const result = await apiClient.getCandles('BTC/USD', 'kraken', '1h', 50)

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('instrument=BTC%2FUSD'),
      expect.any(Object)
    )
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('exchange=kraken'),
      expect.any(Object)
    )
  })
  it('getCandles returns empty array on 204', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 204,
    })
    const result = await apiClient.getCandles('BTC/USD', 'kraken')

    expect(result).toEqual([])
  })
  it('getCandles throws on non-ok response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Server Error',
    })
    await expect(apiClient.getCandles('BTC/USD', 'kraken')).rejects.toThrow(
      'HTTP 500: Server Error'
    )
  })
  it('getOrders returns orders with optional symbol filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          id: 1,
          instrument: 'BTC/USD',
          exchange: 'kraken',
          client_order_id: 'client-1',
          exchange_order_id: 'ex-1',
          created_at: '2024-01-01T00:00:00Z',
          updated_at: '2024-01-01T00:00:00Z',
          side: 'buy',
          type: 'limit',
          price: 50000,
          size: 1,
          filled_size: 1,
          status: 'filled',
        },
      ],
    })
    const result = await apiClient.getOrders('BTC/USD', 50, 10)

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('symbol=BTC%2FUSD'),
      expect.any(Object)
    )
  })
  it('getOrders works without symbol filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [],
    })
    await apiClient.getOrders()
    expect(mockFetch).toHaveBeenCalledWith(
      expect.not.stringContaining('symbol='),
      expect.any(Object)
    )
  })
  it('getExecutions returns executions', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          id: 1,
          order_id: 1,
          timestamp: '2024-01-01T00:00:00Z',
          price: 100,
          size: 1,
          fee: 0.1,
          fee_asset: 'USD',
          instrument: 'BTC/USD',
          side: 'buy',
          exchange: 'kraken',
        },
      ],
    })
    const result = await apiClient.getExecutions(50)

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=50'), expect.any(Object))
  })
  it('getExecutions uses default limit', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          id: 1,
          order_id: 1,
          timestamp: '2024-01-01T00:00:00Z',
          price: 100,
          size: 1,
          fee: 0.1,
          fee_asset: 'USD',
          instrument: 'BTC/USD',
          side: 'buy',
          exchange: 'kraken',
        },
      ],
    })
    const result = await apiClient.getExecutions()

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=100'), expect.any(Object))
  })
  it('getPositions returns positions', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          id: 1,
          instrument: 'BTC/USD',
          exchange: 'kraken',
          quantity: 1,
          average_price: 50000,
          unrealized_pnl: 100,
          realized_pnl: 50,
          updated_at: '2024-01-01T00:00:00Z',
        },
      ],
    })
    const result = await apiClient.getPositions()

    expect(result).toHaveLength(1)
  })
  it('getSignals returns signals with optional filters', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          id: 1,
          instrument: 'BTC/USD',
          exchange: 'kraken',
          timestamp: '2024-01-01T00:00:00Z',
          side: 'buy',
          strength: 0.8,
          reason: 'momentum signal',
          strategy_name: 'momentum',
          price: 50000,
        },
      ],
    })
    const result = await apiClient.getSignals('momentum', 50, 'BTC/USD', 48)

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('strategy=momentum'),
      expect.any(Object)
    )
  })
  it('getSignals works without optional filters', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [],
    })
    await apiClient.getSignals()
    const url = mockFetch.mock.calls[0][0] as string

    expect(url).toContain('limit=100')
    expect(url).toContain('hours=24')
    expect(url).not.toContain('strategy=')
    expect(url).not.toContain('instrument=')
  })
  it('getSignals passes exchange filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [],
    })
    await apiClient.getSignals('momentum', 50, 'BTC/USD', 48, 'kraken')
    const url = mockFetch.mock.calls[0][0] as string

    expect(url).toContain('exchange=kraken')
  })
  it('getOrders passes exchange filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [],
    })
    await apiClient.getOrders('BTC/USD', 50, 10, 'kraken')
    const url = mockFetch.mock.calls[0][0] as string

    expect(url).toContain('exchange=kraken')
  })
  it('getExchanges returns exchange list', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ['kraken', 'binance'],
    })
    const result = await apiClient.getExchanges()

    expect(result).toEqual(['kraken', 'binance'])
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/exchanges'),
      expect.any(Object)
    )
  })
  it('getExchangeInstruments returns instruments for exchange', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ['BTC/USD', 'ETH/USD'],
    })
    const result = await apiClient.getExchangeInstruments('kraken')

    expect(result).toEqual(['BTC/USD', 'ETH/USD'])
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/exchanges/kraken/instruments'),
      expect.any(Object)
    )
  })
  it('getSettings returns settings with optional category', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [
        {
          key: 'setting1',
          value: 'value1',
          category: 'trading',
          updated_at: '2024-01-01T00:00:00Z',
        },
      ],
    })
    const result = await apiClient.getSettings('trading')

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('category=trading'),
      expect.any(Object)
    )
  })
  it('getSettings works without category', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [],
    })
    await apiClient.getSettings()
    expect(mockFetch).toHaveBeenCalledWith(
      expect.not.stringContaining('category='),
      expect.any(Object)
    )
  })
  it('getSettingCategories returns categories', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ categories: ['trading', 'system'] }),
    })
    const result = await apiClient.getSettingCategories()

    expect(result).toEqual(['trading', 'system'])
  })
  it('updateSetting updates a setting', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        key: 'setting1',
        value: 'new-value',
        category: 'general',
        updated_at: '2024-01-01T00:00:00Z',
      }),
    })
    const result = await apiClient.updateSetting('setting1', {
      value: 'new-value',
      category: 'general',
    })

    expect(result.key).toBe('setting1')
    expect(result.value).toBe('new-value')
  })
  it('deleteSetting deletes a setting', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ message: 'Setting deleted successfully' }),
    })
    const result = await apiClient.deleteSetting('setting1')

    expect(result).toEqual({ message: 'Setting deleted successfully' })
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/settings/setting1'),
      expect.objectContaining({ method: 'DELETE' })
    )
  })
  it('getProcessSchema returns process schema', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        name: 'test',
        description: 'Test process',
        class_path: 'snapper.processes.test',
        method: 'run',
        default_enabled: true,
        default_mode: 'thread',
        lifecycle: 'long_running',
      }),
    })
    const result = await apiClient.getProcessSchema('test-process')

    expect(result.name).toBe('test')
    expect(result.lifecycle).toBe('long_running')
  })
  it('createProcessConfig creates process config', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        status: 'created',
        process: { name: 'new-process', template: 'test-template' },
      }),
    })
    const result = await apiClient.createProcessConfig({
      name: 'new-process',
      template: 'test-template',
    })

    expect(result.status).toBe('created')
    expect(result.process.name).toBe('new-process')
  })
  it('getConfiguredProcesses returns configured processes', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ processes: [], count: 0 }),
    })
    const result = await apiClient.getConfiguredProcesses()

    expect(result).toEqual({ processes: [], count: 0 })
  })
  it('getProcessSummary returns process category counts', async () => {
    const summary = {
      feeds: { running: 1, total: 2 },
      strategies: { running: 0, total: 1 },
      executors: { running: 0, total: 0 },
      brokers: { running: 1, total: 1 },
    }

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => summary,
    })
    const result = await apiClient.getProcessSummary()

    expect(result).toEqual(summary)
  })
  it('getStrategies returns strategy list', async () => {
    const strategiesResponse = {
      strategies: [{ name: 'strategy_test', running: true, enabled: true, mode: 'thread' }],
      count: 1,
    }

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => strategiesResponse,
    })
    const result = await apiClient.getStrategies()

    expect(result).toEqual(strategiesResponse)
  })
  it('getAvailableProcesses returns available processes', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ processes: [], count: 0 }),
    })
    const result = await apiClient.getAvailableProcesses()

    expect(result).toEqual({ processes: [], count: 0 })
  })
  it('getProcessRuns returns process runs', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ runs: [], count: 0 }),
    })
    const result = await apiClient.getProcessRuns({ limit: 10, name: 'test' })

    expect(result).toEqual({ runs: [], count: 0 })
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=10'), expect.any(Object))
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('name=test'), expect.any(Object))
  })
  it('getProcessRuns works without options', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ runs: [], count: 0 }),
    })
    await apiClient.getProcessRuns()
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/processes/runs'),
      expect.any(Object)
    )
  })
  it('startProcessByName starts a process', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ status: 'success', name: 'test-process', message: 'Process started' }),
    })
    const result = await apiClient.startProcessByName('test-process', {
      mode: 'live',
      args: [1, 2],
      kwargs: { param: 'value' },
      autostart: true,
    })

    expect(result).toEqual({ status: 'success', name: 'test-process', message: 'Process started' })
  })
  it('startProcessByName works without options', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ status: 'success', name: 'test-process', message: 'Started' }),
    })
    await apiClient.startProcessByName('test-process')
    expect(mockFetch).toHaveBeenCalled()
  })
  it('stopProcessByName stops a process', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ status: 'success', name: 'test-process', message: 'Process stopped' }),
    })
    const result = await apiClient.stopProcessByName('test-process')

    expect(result).toEqual({ status: 'success', name: 'test-process', message: 'Process stopped' })
  })
  it('changePassword changes user password', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ message: 'Password changed successfully' }),
    })
    const result = await apiClient.changePassword('testuser', 'oldPassword', 'newPassword')

    expect(result).toEqual({ message: 'Password changed successfully' })
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users/testuser/change-password',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({
          current_password: 'oldPassword',
          new_password: 'newPassword',
        }),
      })
    )
  })
})
describe('cacheWsTicketFromResponse', () => {
  let apiClient: typeof import('./apiClient').apiClient
  let mockFetch: ReturnType<typeof vi.fn>
  let storeWsTicket: ReturnType<typeof vi.fn>

  beforeEach(async () => {
    vi.clearAllMocks()
    vi.mocked(getCookie).mockReturnValue('test-csrf')
    mockFetch = vi.fn()
    ;(globalThis as any).fetch = mockFetch
    const wsTicketCacheMod = await import('./wsTicketCache')

    storeWsTicket = wsTicketCacheMod.storeWsTicket as unknown as ReturnType<typeof vi.fn>
    const mod = await import('./apiClient')

    apiClient = mod.apiClient
  })
  it('stores ws ticket from valid refresh response', async () => {
    const expDate = new Date('2026-01-07T12:00:00Z')

    mockFetch
      .mockResolvedValueOnce({ ok: false, status: 401 })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        clone: function () {
          return {
            json: async () => ({
              ws_token: 'test-token',
              ws_token_exp: expDate.toISOString(),
            }),
          }
        },
      })
      .mockResolvedValueOnce({ ok: true, status: 200 })
    await apiClient.request('/test', { method: 'GET' })
    expect(storeWsTicket).toHaveBeenCalledWith({
      token: 'test-token',
      exp: Math.floor(expDate.getTime() / 1000),
    })
  })
  it('stores null when ws_token is missing', async () => {
    mockFetch
      .mockResolvedValueOnce({ ok: false, status: 401 })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        clone: function () {
          return {
            json: async () => ({ other: 'data' }),
          }
        },
      })
      .mockResolvedValueOnce({ ok: true, status: 200 })
    await apiClient.request('/test', { method: 'GET' })
    expect(storeWsTicket).toHaveBeenCalledWith(null)
  })
  it('stores null when json parsing fails', async () => {
    mockFetch
      .mockResolvedValueOnce({ ok: false, status: 401 })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        clone: function () {
          return {
            json: async () => {
              throw new Error('JSON parse error')
            },
          }
        },
      })
      .mockResolvedValueOnce({ ok: true, status: 200 })
    await apiClient.request('/test', { method: 'GET' })
    expect(storeWsTicket).toHaveBeenCalledWith(null)
  })
  it('throws error when postJSON receives non-ok response with detail message', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      json: vi.fn().mockResolvedValue({ detail: 'Invalid current password or user not found' }),
    })
    await expect(apiClient.postJSON('/test', { data: 'test' })).rejects.toThrow(
      'Invalid current password or user not found'
    )
  })
  it('throws error when postJSON receives non-ok response with message', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      json: vi.fn().mockResolvedValue({ message: 'Custom error message' }),
    })
    await expect(apiClient.postJSON('/test', { data: 'test' })).rejects.toThrow(
      'Custom error message'
    )
  })
  it('throws error when postJSON receives non-ok response without json body', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      json: vi.fn().mockRejectedValue(new Error('No JSON')),
    })
    await expect(apiClient.postJSON('/test', { data: 'test' })).rejects.toThrow(
      'HTTP 400: Bad Request'
    )
  })
  it('throws error when putJSON receives non-ok response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
      json: vi.fn().mockResolvedValue({ detail: 'Server error' }),
    })
    await expect(apiClient.putJSON('/test', { data: 'test' })).rejects.toThrow('Server error')
  })
  it('throws error when deleteJSON receives non-ok response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
      json: vi.fn().mockResolvedValue({ detail: 'Resource not found' }),
    })
    await expect(apiClient.deleteJSON('/test')).rejects.toThrow('Resource not found')
  })
  it('handles JSON parse error in CSRF retry logic', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 403,
      clone: () => ({
        json: vi.fn().mockRejectedValue(new Error('Invalid JSON')),
      }),
    })
    await expect(apiClient.request('/test', { method: 'POST' })).rejects.toThrow()
  })
})
