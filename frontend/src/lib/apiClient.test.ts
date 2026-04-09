import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { getCookie } from './utils'

vi.mock('./utils', () => ({
  getCookie: vi.fn(() => 'test-csrf-token'),
}))
vi.mock('./wsTicketCache', () => ({
  storeWsTicket: vi.fn(),
}))

let mockSeqCounter = 0

vi.mock('uuid', () => ({
  v7: vi.fn(() => `00000000-0000-7000-8000-${String(++mockSeqCounter).padStart(12, '0')}`),
}))
vi.mock('./sequenceTracker', () => ({
  getTracker: vi.fn(() => ({
    sessionId: 'test-session-id',
    nextSequence: vi.fn(() => ++mockSeqCounter),
  })),
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
          payload: {
            ws_token: 'new-token',
            ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          },
        }),
        clone: function () {
          return {
            json: async () => ({
              payload: {
                ws_token: 'new-token',
                ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
              },
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
        type: 'health_check_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'health_check',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          status: 'healthy',
          version: '1.0.0',
          connections: {
            active_connections: 5,
            zmq_subscribers: 2,
            subscriber_tasks: 1,
            active_topics: 3,
            active_clients: 4,
          },
          topics: {
            active: 3,
          },
          gap_detection: {
            bridge: {
              gaps_detected: 0,
              session_resets: 0,
              duplicates: 0,
              mid_stream_joins: 0,
              rejected_unstamped: 0,
            },
            rest_clients: {},
          },
        },
      }),
    })
    const result = await apiClient.getHealth()

    expect(result.payload.status).toBe('healthy')
    expect(result.timestamp).toBe('2024-01-01T00:00:00Z')
  })
  it('getSystemStatus returns system status', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'system_status_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'system_status',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          trader: {
            status: 'running',
            pid: 1234,
          },
          backtests: {},
          strategies: [],
        },
      }),
    })
    const result = await apiClient.getSystemStatus()

    expect(result.payload.trader.status).toBe('running')
    expect(result.payload.backtests).toEqual({})
  })
  it('getCandles returns candle data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'candle_list',
        session_id: '',
        sequence_id: 0,
        payload: [
          {
            type: 'candle',
            session_id: '',
            sequence_id: 0,
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
        count: 1,
      }),
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
  it('getCandles returns empty array when payload is empty', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'candle_list',
        session_id: '',
        sequence_id: 0,
        payload: [],
        count: 0,
      }),
    })
    const result = await apiClient.getCandles('BTC/USD', 'kraken')

    expect(result).toEqual([])
  })
  it('getCandles returns empty array when items is missing', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ type: 'candle_list', session_id: '', sequence_id: 0, count: 0 }),
    })
    const result = await apiClient.getCandles('BTC/USD', 'kraken', '1h', 50)

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
      json: async () => ({
        type: 'order_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'order',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            instrument: 'BTC/USD',
            exchange: 'kraken',
            mode: 'live',
            client_order_id: 'client-1',
            exchange_order_id: 'ex-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: '2024-01-01T00:00:00Z',
            side: 'buy',
            order_type: 'limit',
            price: 50000,
            size: 1,
            filled_size: 1,
            status: 'filled',
            reduce_only: false,
            wallet_public_id: 'wallet-1',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getOrders('BTC/USD', 50, 10)

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('symbol=BTC%2FUSD'),
      expect.any(Object)
    )
  })
  it('getOrders works without symbol filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'order_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
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
      json: async () => ({
        type: 'execution_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'execution',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            client_order_id: 'client-1',
            executed_at: '2024-01-01T00:00:00Z',
            price: 100,
            size: 1,
            last_size: 1,
            last_price: 100,
            fee: 0.1,
            fee_asset: 'USD',
            instrument: 'BTC/USD',
            side: 'buy',
            exchange: 'kraken',
            status: 'filled',
            wallet_public_id: 'wallet-1',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getExecutions(50)

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=50'), expect.any(Object))
  })
  it('getExecutions uses default limit', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'execution_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'execution',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            client_order_id: 'client-1',
            executed_at: '2024-01-01T00:00:00Z',
            price: 100,
            size: 1,
            last_size: 1,
            last_price: 100,
            fee: 0.1,
            fee_asset: 'USD',
            instrument: 'BTC/USD',
            side: 'buy',
            exchange: 'kraken',
            status: 'filled',
            wallet_public_id: 'wallet-1',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getExecutions()

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=100'), expect.any(Object))
  })
  it('getPositions returns positions', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'position_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'position',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            instrument: 'BTC/USD',
            exchange: 'kraken',
            mode: 'live',
            quantity: 1,
            average_price: 50000,
            unrealized_pnl: 100,
            realized_pnl: 50,
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getPositions()

    expect(result.payload).toHaveLength(1)
  })
  it('getSignals returns signals with optional filters', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'signal_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'signal',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            instrument: 'BTC/USD',
            exchange: 'kraken',
            fired_at: '2024-01-01T00:00:00Z',
            side: 'buy',
            strength: 0.8,
            reason: 'momentum signal',
            strategy_name: 'momentum',
            price: 50000,
            wallet_public_id: 'wallet-1',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getSignals('momentum', 50, 'BTC/USD', 48)

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('strategy=momentum'),
      expect.any(Object)
    )
  })
  it('getSignals works without optional filters', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'signal_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
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
      json: async () => ({
        type: 'signal_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    await apiClient.getSignals('momentum', 50, 'BTC/USD', 48, 'kraken')
    const url = mockFetch.mock.calls[0][0] as string

    expect(url).toContain('exchange=kraken')
  })
  it('getOrders passes exchange filter', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'order_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    await apiClient.getOrders('BTC/USD', 50, 10, 'kraken')
    const url = mockFetch.mock.calls[0][0] as string

    expect(url).toContain('exchange=kraken')
  })
  it('getExchanges returns exchange list', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'exchange_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: ['kraken', 'binance'],
        count: 2,
      }),
    })
    const result = await apiClient.getExchanges()

    expect(result.payload).toEqual(['kraken', 'binance'])
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/exchanges'),
      expect.any(Object)
    )
  })
  it('getExchangeInstruments returns instruments for exchange', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'instrument_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: ['BTC/USD', 'ETH/USD'],
        count: 2,
      }),
    })
    const result = await apiClient.getExchangeInstruments('kraken')

    expect(result.payload).toEqual(['BTC/USD', 'ETH/USD'])
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/exchanges/kraken/instruments'),
      expect.any(Object)
    )
  })
  it('getSettings returns settings with optional category', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'setting_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'setting_read',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            key: 'setting1',
            value: 'value1',
            category: 'trading',
            updated_at: '2024-01-01T00:00:00Z',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.getSettings('trading')

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('category=trading'),
      expect.any(Object)
    )
  })
  it('getSettings works without category', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'setting_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
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
      json: async () => ({
        type: 'setting_categories',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: ['trading', 'system'],
        count: 2,
      }),
    })
    const result = await apiClient.getSettingCategories()

    expect(result).toEqual(['trading', 'system'])
  })
  it('updateSetting updates a setting', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'setting_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'setting_read',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          key: 'setting1',
          value: 'new-value',
          category: 'general',
          updated_at: '2024-01-01T00:00:00Z',
        },
      }),
    })
    const result = await apiClient.updateSetting('setting1', {
      value: 'new-value',
      category: 'general',
    })

    expect(result.payload.key).toBe('setting1')
    expect(result.payload.value).toBe('new-value')
  })
  it('removeSetting removes a setting', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'message',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: 'Setting deleted successfully',
      }),
    })
    const result = await apiClient.removeSetting('setting1')

    expect(result).toEqual({
      type: 'message',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: 'Setting deleted successfully',
    })
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/settings/setting1/remove'),
      expect.objectContaining({ method: 'POST' })
    )
  })
  it('getProcessSchema returns process schema', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'process_schema_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'process_schema',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          name: 'test',
          description: 'Test process',
          class_path: 'snapper.processes.test',
          method: 'run',
          default_enabled: true,
          default_mode: 'thread',
          default_parameters: {},
          lifecycle: 'long_running',
        },
      }),
    })
    const result = await apiClient.getProcessSchema('test-process')

    expect(result.payload.name).toBe('test')
    expect(result.payload.lifecycle).toBe('long_running')
  })
  it('createProcessConfig creates process config', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'process_create_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'process_create',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          status: 'created',
          process: {
            name: 'new-process',
            template: 'test-template',
          },
        },
      }),
    })
    const result = await apiClient.createProcessConfig({
      name: 'new-process',
      template: 'test-template',
    })

    expect(result.payload.status).toBe('created')
    expect(result.payload.process.name).toBe('new-process')
  })
  it('getConfiguredProcesses returns configured processes', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'configured_processes',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    const result = await apiClient.getConfiguredProcesses()

    expect(result).toEqual({
      type: 'configured_processes',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: [],
      count: 0,
    })
  })
  it('getProcessSummary returns process category counts', async () => {
    const responseData = {
      type: 'process_summary_response' as const,
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: {
        type: 'process_summary' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        feeds: {
          running: 1,
          total: 2,
        },
        strategies: {
          running: 0,
          total: 1,
        },
        executors: {
          running: 0,
          total: 0,
        },
        brokers: {
          running: 1,
          total: 1,
        },
      },
    }

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => responseData,
    })
    const result = await apiClient.getProcessSummary()

    expect(result).toEqual(responseData)
  })
  it('getStrategies returns strategy list', async () => {
    const strategiesResponse = {
      type: 'strategy_list' as const,
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: [
        {
          type: 'strategy_process' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          name: 'strategy_test',
          running: true,
          enabled: true,
          mode: 'thread' as const,
        },
      ],
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
      json: async () => ({
        type: 'available_processes',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    const result = await apiClient.getAvailableProcesses()

    expect(result).toEqual({
      type: 'available_processes',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: [],
      count: 0,
    })
  })
  it('getProcessRuns returns process runs', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'process_runs',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    const result = await apiClient.getProcessRuns({ limit: 10, name: 'test' })

    expect(result).toEqual({
      type: 'process_runs',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: [],
      count: 0,
    })
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('limit=10'), expect.any(Object))
    expect(mockFetch).toHaveBeenCalledWith(expect.stringContaining('name=test'), expect.any(Object))
  })
  it('getProcessRuns works without options', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'process_runs',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    await apiClient.getProcessRuns()
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/processes/runs'),
      expect.any(Object)
    )
  })
  it('startProcessByName starts a process', async () => {
    const responseData = {
      type: 'process_start_response' as const,
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: {
        type: 'process_start' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        status: 'success' as const,
        name: 'test-process',
        message: 'Process started',
      },
    }

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => responseData,
    })
    const result = await apiClient.startProcessByName('test-process', {
      mode: 'live' as 'thread',
      parameters: { param: 'value' },
    })

    expect(result).toEqual(responseData)
  })
  it('startProcessByName works without options', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'process_start_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'process_start',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          status: 'success',
          name: 'test-process',
          message: 'Started',
        },
      }),
    })
    await apiClient.startProcessByName('test-process')
    expect(mockFetch).toHaveBeenCalled()
  })
  it('stopProcessByName stops a process', async () => {
    const responseData = {
      type: 'process_stop_response' as const,
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: {
        type: 'process_stop' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        status: 'success' as const,
        name: 'test-process',
        message: 'Process stopped',
      },
    }

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => responseData,
    })
    const result = await apiClient.stopProcessByName('test-process')

    expect(result).toEqual(responseData)
  })
  it('changePassword changes user password', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        type: 'message',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: 'Password changed successfully',
      }),
    })
    const result = await apiClient.changePassword('testuser', 'oldPassword', 'newPassword')

    expect(result).toEqual({
      type: 'message',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      payload: 'Password changed successfully',
    })
    const call = mockFetch.mock.calls[0]

    expect(call[0]).toBe('/api/auth/users/testuser/change-password')
    expect(call[1].method).toBe('POST')
    const body = JSON.parse(call[1].body)

    expect(body.payload.current_password).toBe('oldPassword')
    expect(body.payload.new_password).toBe('newPassword')
    expect(body.public_id).toBeDefined()
    expect(body.session_id).toBe('test-session-id')
    expect(body.timestamp).toBeDefined()
  })
})
describe('user management API methods', () => {
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
  it('listUsers fetches users', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'user_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [
          {
            type: 'user_profile',
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            username: 'admin',
            email: 'admin@test.com',
            role: 'admin',
            is_active: true,
            created_at: '2024-01-01T00:00:00Z',
          },
        ],
        count: 1,
      }),
    })
    const result = await apiClient.listUsers(true)

    expect(result.payload).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users?include_inactive=true',
      expect.objectContaining({ method: 'GET' })
    )
  })
  it('listUsers appends as_of via get() when time-traveling', async () => {
    apiClient.setTimeTravelAsOf('2024-06-01T12:00:00Z')
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'user_list',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: [],
        count: 0,
      }),
    })
    await apiClient.listUsers(false)

    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users?include_inactive=false&as_of=2024-06-01T12%3A00%3A00Z',
      expect.objectContaining({ method: 'GET' })
    )
    apiClient.setTimeTravelAsOf(null)
  })
  it('createUser posts new user', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'user_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'user_profile',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'new',
          email: 'e@e.com',
          role: 'viewer',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
      }),
    })
    const result = await apiClient.createUser({
      username: 'new',
      password: 'pass',
      email: 'e@e.com',
      role: 'viewer',
      is_active: true,
    })

    expect(result.payload.username).toBe('new')
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users',
      expect.objectContaining({ method: 'POST' })
    )
  })
  it('updateUser posts user data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'user_response',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: {
          type: 'user_profile',
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          email: 'new@e.com',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
      }),
    })
    const result = await apiClient.updateUser('admin', {
      email: 'new@e.com',
      role: 'admin',
      is_active: true,
    })

    expect(result.payload.username).toBe('admin')
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users/admin/update',
      expect.objectContaining({ method: 'POST' })
    )
  })
  it('deactivateUser deactivates user', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'message',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: 'User deactivated',
      }),
    })
    const result = await apiClient.deactivateUser('testuser')

    expect(result.payload).toBe('User deactivated')
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users/testuser/deactivate',
      expect.objectContaining({ method: 'POST' })
    )
  })
  it('adminResetPassword resets password', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        type: 'message',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        payload: 'Password reset',
      }),
    })
    const result = await apiClient.adminResetPassword('admin', { new_password: 'newpass123' })

    expect(result.payload).toBe('Password reset')
    expect(mockFetch).toHaveBeenCalledWith(
      '/api/auth/users/admin/admin-reset-password',
      expect.objectContaining({ method: 'POST' })
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
              payload: {
                ws_token: 'test-token',
                ws_token_exp: expDate.toISOString(),
              },
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
            json: async () => ({ payload: { other: 'data' } }),
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
  describe('provenance stamping', () => {
    beforeEach(() => {
      mockSeqCounter = 0
    })
    it('stamps POST body with provenance fields', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/test', { key: 'value' })
      const call = mockFetch.mock.calls[0]
      const body = JSON.parse(call[1].body)

      expect(body.payload.key).toBe('value')
      expect(body.public_id).toBeDefined()
      expect(body.session_id).toBe('test-session-id')
      expect(body.sequence_id).toEqual(expect.any(Number))
      expect(body.timestamp).toBeDefined()
    })
    it('does not stamp GET requests', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/test')
      const call = mockFetch.mock.calls[0]

      expect(call[1].body).toBeUndefined()
    })
    it('does not stamp POST without body', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/test')
      const call = mockFetch.mock.calls[0]

      expect(call[1].body).toBeUndefined()
    })
    it('does not stamp array bodies', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/test', [1, 2, 3])
      const call = mockFetch.mock.calls[0]
      const body = JSON.parse(call[1].body)

      expect(Array.isArray(body)).toBe(true)
      expect(body).toEqual([1, 2, 3])
    })
    it('does not stamp null bodies', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/test', null)
      const call = mockFetch.mock.calls[0]

      expect(call[1].body).toBeUndefined()
    })
  })
  describe('time-travel mode', () => {
    afterEach(() => {
      apiClient.setTimeTravelAsOf(null)
    })
    it('appends as_of query parameter to GET requests when time-traveling', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/positions')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('as_of=2026-03-15T10%3A00%3A00Z')
    })
    it('appends as_of with & when URL already has query params', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/candles?instrument=BTC-USD')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('&as_of=')
    })
    it('does not append as_of when not time-traveling', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/positions')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).not.toContain('as_of')
    })
    it('blocks POST requests when time-traveling', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')

      await expect(apiClient.post('/api/settings/foo/set', { value: 'bar' })).rejects.toThrow(
        'Write operations are disabled in time-travel mode'
      )
      expect(mockFetch).not.toHaveBeenCalled()
    })
    it('allows POST requests when not time-traveling', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/settings/foo/set', { value: 'bar' })

      expect(mockFetch).toHaveBeenCalled()
    })
    it('allows auth POST requests when time-traveling', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/auth/refresh', {})

      expect(mockFetch).toHaveBeenCalled()
    })
    it('allows login POST when time-traveling', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.post('/api/auth/login', { username: 'u', password: 'p' })

      expect(mockFetch).toHaveBeenCalled()
    })
    it('exposes current asOf via getter', () => {
      expect(apiClient.getTimeTravelAsOf()).toBeNull()
      apiClient.setTimeTravelAsOf('2026-01-01T00:00:00Z')
      expect(apiClient.getTimeTravelAsOf()).toBe('2026-01-01T00:00:00Z')
    })
    it('blocks DELETE requests when time-traveling via request()', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')

      await expect(apiClient.request('/api/settings/foo', { method: 'DELETE' })).rejects.toThrow(
        'Write operations are disabled in time-travel mode'
      )
      expect(mockFetch).not.toHaveBeenCalled()
    })
    it('blocks PUT requests when time-traveling via request()', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')

      await expect(
        apiClient.request('/api/settings/foo', { method: 'PUT', body: '{}' })
      ).rejects.toThrow('Write operations are disabled in time-travel mode')
      expect(mockFetch).not.toHaveBeenCalled()
    })
  })
  describe('getOperators', () => {
    it('fetches and validates operators list', async () => {
      const payload = {
        type: 'operator_list_response',
        session_id: 's',
        sequence_id: 1,
        public_id: 'p',
        timestamp: '2026-01-01T00:00:00Z',
        count: 1,
        payload: [
          {
            type: 'operator_info',
            session_id: 's',
            sequence_id: 1,
            public_id: 'op-1',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'alice',
          },
        ],
      }

      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => payload })
      const result = await apiClient.getOperators()

      expect(result.payload).toHaveLength(1)
      expect(result.payload[0].label).toBe('alice')
    })
  })
  describe('getWallets', () => {
    it('fetches and validates wallets list', async () => {
      const payload = {
        type: 'wallet_list_response',
        session_id: 's',
        sequence_id: 1,
        public_id: 'p',
        timestamp: '2026-01-01T00:00:00Z',
        count: 1,
        payload: [
          {
            type: 'wallet_info',
            session_id: 's',
            sequence_id: 1,
            public_id: 'w-1',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'default',
            is_paper: false,
          },
        ],
      }

      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => payload })
      const result = await apiClient.getWallets()

      expect(result.payload).toHaveLength(1)
      expect(result.payload[0].is_paper).toBe(false)
    })
  })
  describe('multi-tenant scope query params', () => {
    afterEach(() => {
      apiClient.setOperatorScope(null)
      apiClient.setWalletScope(null)
    })
    it('appends operator_public_id when operator scope is set', async () => {
      apiClient.setOperatorScope('op-123')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/orders')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('operator_public_id=op-123')
    })
    it('appends wallet_public_id when wallet scope is set', async () => {
      apiClient.setWalletScope('wallet-456')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/positions')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('wallet_public_id=wallet-456')
    })
    it('appends both scope params when both are set', async () => {
      apiClient.setOperatorScope('op-123')
      apiClient.setWalletScope('wallet-456')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/orders')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('operator_public_id=op-123')
      expect(url).toContain('wallet_public_id=wallet-456')
    })
    it('does not append scope params when both are null', async () => {
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/orders')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).not.toContain('operator_public_id')
      expect(url).not.toContain('wallet_public_id')
    })
    it('exposes current scope via getters', () => {
      expect(apiClient.getOperatorScope()).toBeNull()
      expect(apiClient.getWalletScope()).toBeNull()
      apiClient.setOperatorScope('op-1')
      apiClient.setWalletScope('w-1')
      expect(apiClient.getOperatorScope()).toBe('op-1')
      expect(apiClient.getWalletScope()).toBe('w-1')
    })
    it('combines with time-travel as_of when both are active', async () => {
      apiClient.setTimeTravelAsOf('2026-03-15T10:00:00Z')
      apiClient.setOperatorScope('op-99')
      mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      await apiClient.get('/api/orders')
      const url = mockFetch.mock.calls[0][0] as string

      expect(url).toContain('as_of=')
      expect(url).toContain('operator_public_id=op-99')
      apiClient.setTimeTravelAsOf(null)
    })
  })
})
