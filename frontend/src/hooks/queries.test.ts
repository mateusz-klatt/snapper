import { createElement, type ReactNode } from 'react'
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  useSystemStatus,
  useCandles,
  useOrders,
  useExecutions,
  useAvailableProcesses,
  useConfiguredProcesses,
  useProcessSchema,
  useProcessRuns,
  useLatestSignals,
  useOrdersGrouped,
  usePositionsSummary,
  useStartProcessByName,
  useStopProcessByName,
  useCreateProcessConfig,
} from './queries'
import { useAuth } from '../stores/auth'
import { apiClient } from '../lib/apiClient'

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    getSystemStatus: vi.fn(() => Promise.resolve({ trader: { status: 'running' } })),
    getCandles: vi.fn(() => Promise.resolve([])),
    getOrders: vi.fn(() => Promise.resolve([])),
    getExecutions: vi.fn(() => Promise.resolve([])),
    getPositions: vi.fn(() => Promise.resolve([])),
    getSignals: vi.fn(() => Promise.resolve([])),
    getAvailableProcesses: vi.fn(() => Promise.resolve({ available: [] })),
    getConfiguredProcesses: vi.fn(() => Promise.resolve({ processes: [] })),
    getProcessSchema: vi.fn(() => Promise.resolve({ schema: {} })),
    getProcessRuns: vi.fn(() => Promise.resolve({ runs: [] })),
    startProcessByName: vi.fn(() => Promise.resolve({ status: 'ok', message: 'started' })),
    stopProcessByName: vi.fn(() => Promise.resolve({ status: 'ok', message: 'stopped' })),
    createProcessConfig: vi.fn(() => Promise.resolve({ name: 'test', id: '123' })),
  },
}))
vi.mock('../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    isAuthenticated: true,
  })),
}))
vi.mock('../stores/trade', () => ({
  useTradeStore: vi.fn(selector => {
    const state = {
      updateOrders: vi.fn(),
      updateExecutions: vi.fn(),
      updatePositions: vi.fn(),
      updateSignals: vi.fn(),
    }

    return selector ? selector(state) : state
  }),
}))
vi.mock('../lib/transforms', () => ({
  safeOrderFromAPI: vi.fn(o => o),
  safeExecutionFromAPI: vi.fn(e => e),
  safeSignalFromAPI: vi.fn(s => s),
  positionFromAPI: vi.fn(p => ({
    ...p,
    averagePrice: p.average_price ?? 0,
    unrealized_pnl: p.unrealized_pnl ?? 0,
    realized_pnl: p.realized_pnl ?? 0,
    quantity: p.quantity ?? 0,
    instrument: p.instrument ?? '',
  })),
}))
const mockedApiClient = apiClient as unknown as {
  getSystemStatus: Mock
  getCandles: Mock
  getOrders: Mock
  getExecutions: Mock
  getPositions: Mock
  getSignals: Mock
  getAvailableProcesses: Mock
  getConfiguredProcesses: Mock
  getProcessSchema: Mock
  getProcessRuns: Mock
  startProcessByName: Mock
  stopProcessByName: Mock
  createProcessConfig: Mock
}
const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  })

const createWrapper = () => {
  const queryClient = createQueryClient()

  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client: queryClient }, children)
  }
}

const createWrapperWithClient = () => {
  const queryClient = createQueryClient()
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(QueryClientProvider, { client: queryClient }, children)

  return { queryClient, wrapper }
}

describe('queries', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  describe('useSystemStatus', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useSystemStatus(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useCandles', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useCandles('EUR-USD', '1h'), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('does not fetch when enabled is false', async () => {
      const { result } = renderHook(() => useCandles('EUR-USD', '1h', 100, false), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeUndefined()
    })
  })
  describe('useOrders', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useOrders(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useExecutions', () => {
    it('returns data when authenticated', async () => {
      mockedApiClient.getExecutions.mockResolvedValueOnce([null, { id: 'exec-1' }])
      const { result } = renderHook(() => useExecutions(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useLatestSignals', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useLatestSignals(10), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('uses default limit of 10', async () => {
      const { result } = renderHook(() => useLatestSignals(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('sorts signals by timestamp and applies limit', async () => {
      mockedApiClient.getSignals.mockResolvedValueOnce([
        {
          id: '1',
          exchange: 'kraken',
          instrument: 'BTC/USD',
          side: 'buy',
          strength: 0.5,
          reason: 'oldest',
          strategy_name: 'test',
          price: 50000,
          timestamp: new Date('2026-01-15T10:00:00Z'),
        },
        {
          id: '2',
          exchange: 'kraken',
          instrument: 'BTC/USD',
          side: 'buy',
          strength: 0.6,
          reason: 'newest',
          strategy_name: 'test',
          price: 50100,
          timestamp: new Date('2026-01-15T12:00:00Z'),
        },
        {
          id: '3',
          exchange: 'kraken',
          instrument: 'BTC/USD',
          side: 'sell',
          strength: 0.4,
          reason: 'middle',
          strategy_name: 'test',
          price: 49900,
          timestamp: new Date('2026-01-15T11:00:00Z'),
        },
      ] as never)
      const { result } = renderHook(() => useLatestSignals(2), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toHaveLength(2)
      expect(result.current.data?.[0].id).toBe('2')
      expect(result.current.data?.[1].id).toBe('3')
    })
    it('handles signals with undefined timestamp in sorting', async () => {
      mockedApiClient.getSignals.mockResolvedValueOnce([
        {
          id: '1',
          exchange: 'kraken',
          instrument: 'BTC/USD',
          side: 'buy',
          strength: 0.5,
          reason: 'with-timestamp',
          strategy_name: 'test',
          price: 50000,
          timestamp: new Date('2026-01-15T10:00:00Z'),
        },
        {
          id: '2',
          exchange: 'kraken',
          instrument: 'ETH/USD',
          side: 'sell',
          strength: 0.6,
          reason: 'no-timestamp',
          strategy_name: 'test',
          price: 3000,
          timestamp: undefined,
        },
        {
          id: '3',
          exchange: 'kraken',
          instrument: 'SOL/USD',
          side: 'buy',
          strength: 0.4,
          reason: 'newer-timestamp',
          strategy_name: 'test',
          price: 100,
          timestamp: new Date('2026-01-15T12:00:00Z'),
        },
      ] as never)
      const { result } = renderHook(() => useLatestSignals(3), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toHaveLength(3)
      expect(result.current.data?.[0].id).toBe('3')
      expect(result.current.data?.[1].id).toBe('1')
      expect(result.current.data?.[2].id).toBe('2')
    })
  })
  describe('useAvailableProcesses', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useAvailableProcesses(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useConfiguredProcesses', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useConfiguredProcesses(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useProcessSchema', () => {
    it('returns data when name is provided', async () => {
      const { result } = renderHook(() => useProcessSchema('collector'), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
  })
  describe('useProcessRuns', () => {
    it('returns data with default options', async () => {
      const { result } = renderHook(() => useProcessRuns(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('passes options correctly', async () => {
      const options = { name: 'collector', limit: 100 }
      const { result } = renderHook(() => useProcessRuns(options), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('respects enabled option', async () => {
      const options = { enabled: false }
      const { result } = renderHook(() => useProcessRuns(options), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(mockedApiClient.getProcessRuns).not.toHaveBeenCalled()
    })
  })
  describe('useOrdersGrouped', () => {
    it('groups orders by status', async () => {
      mockedApiClient.getOrders.mockResolvedValueOnce([
        { id: '1', status: 'NEW', instrument: 'BTC/USD', side: 'buy', price: 100, quantity: 1 },
        { id: '2', status: 'OPEN', instrument: 'BTC/USD', side: 'buy', price: 100, quantity: 1 },
        { id: '3', status: 'FILLED', instrument: 'BTC/USD', side: 'buy', price: 100, quantity: 1 },
        {
          id: '4',
          status: 'PARTIALLY_FILLED',
          instrument: 'BTC/USD',
          side: 'buy',
          price: 100,
          quantity: 1,
        },
        {
          id: '5',
          status: 'CANCELLED',
          instrument: 'BTC/USD',
          side: 'buy',
          price: 100,
          quantity: 1,
        },
        {
          id: '6',
          status: 'REJECTED',
          instrument: 'BTC/USD',
          side: 'buy',
          price: 100,
          quantity: 1,
        },
      ])
      const { result } = renderHook(() => useOrdersGrouped(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
      expect(result.current.data?.new).toHaveLength(1)
      expect(result.current.data?.open).toHaveLength(1)
      expect(result.current.data?.filled).toHaveLength(1)
      expect(result.current.data?.partially_filled).toHaveLength(1)
      expect(result.current.data?.cancelled).toHaveLength(1)
      expect(result.current.data?.rejected).toHaveLength(1)
    })
    it('returns null when no orders', async () => {
      mockedApiClient.getOrders.mockResolvedValueOnce(null as unknown as [])
      const { result } = renderHook(() => useOrdersGrouped(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeNull()
    })
  })
  describe('usePositionsSummary', () => {
    it('calculates position summary', async () => {
      mockedApiClient.getPositions.mockResolvedValueOnce([
        {
          id: '1',
          instrument: 'BTC/USD',
          quantity: 10,
          average_price: 100,
          unrealized_pnl: 50,
          realized_pnl: 20,
        },
        {
          id: '2',
          instrument: 'ETH/USD',
          quantity: 5,
          average_price: 200,
          unrealized_pnl: -10,
          realized_pnl: 30,
        },
      ])
      const { result } = renderHook(() => usePositionsSummary(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
      expect(result.current.data?.count).toBe(2)
      expect(result.current.data?.instruments).toContain('BTC/USD')
      expect(result.current.data?.instruments).toContain('ETH/USD')
      expect(result.current.data?.totalPnL).toBe(90)
    })
    it('returns null when no positions', async () => {
      mockedApiClient.getPositions.mockResolvedValueOnce(null as unknown as [])
      const { result } = renderHook(() => usePositionsSummary(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeNull()
    })
    it('handles zero totalCost', async () => {
      mockedApiClient.getPositions.mockResolvedValueOnce([
        {
          id: '1',
          instrument: 'BTC/USD',
          quantity: 0,
          average_price: 0,
          unrealized_pnl: 0,
          realized_pnl: 0,
        },
      ])
      const { result } = renderHook(() => usePositionsSummary(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data?.pnlPercent).toBe(0)
    })
  })
  describe('useStartProcessByName', () => {
    it('starts process and invalidates queries', async () => {
      const { result } = renderHook(() => useStartProcessByName(), { wrapper: createWrapper() })

      await act(async () => {
        await result.current.mutateAsync({
          name: 'collector',
          mode: 'thread',
          args: [1, 2],
          kwargs: { key: 'value' },
          autostart: true,
        })
      })
      expect(mockedApiClient.startProcessByName).toHaveBeenCalledWith('collector', {
        mode: 'thread',
        args: [1, 2],
        kwargs: { key: 'value' },
        autostart: true,
      })
    })
    it('uses exponential retryDelay', async () => {
      const { queryClient, wrapper } = createWrapperWithClient()
      const { result } = renderHook(() => useStartProcessByName(), { wrapper })

      await act(async () => {
        await result.current.mutateAsync({ name: 'collector' })
      })
      const mutation = queryClient.getMutationCache().getAll()[0]

      expect(mutation).toBeDefined()
      const retryDelay = mutation?.options.retryDelay

      expect(retryDelay).toEqual(expect.any(Function))

      if (typeof retryDelay === 'function') {
        expect(retryDelay(0, new Error('test'))).toBe(1000)
        expect(retryDelay(5, new Error('test'))).toBe(30000)
      }
    })
  })
  describe('useStopProcessByName', () => {
    it('stops process and invalidates queries', async () => {
      const { result } = renderHook(() => useStopProcessByName(), { wrapper: createWrapper() })

      await act(async () => {
        await result.current.mutateAsync({ name: 'collector' })
      })
      expect(mockedApiClient.stopProcessByName).toHaveBeenCalledWith('collector')
    })
    it('uses exponential retryDelay', async () => {
      const { queryClient, wrapper } = createWrapperWithClient()
      const { result } = renderHook(() => useStopProcessByName(), { wrapper })

      await act(async () => {
        await result.current.mutateAsync({ name: 'collector' })
      })
      const mutation = queryClient.getMutationCache().getAll()[0]

      expect(mutation).toBeDefined()
      const retryDelay = mutation?.options.retryDelay

      expect(retryDelay).toEqual(expect.any(Function))

      if (typeof retryDelay === 'function') {
        expect(retryDelay(0, new Error('test'))).toBe(1000)
        expect(retryDelay(5, new Error('test'))).toBe(30000)
      }
    })
  })
  describe('useCreateProcessConfig', () => {
    it('creates process config and invalidates queries', async () => {
      const { result } = renderHook(() => useCreateProcessConfig(), { wrapper: createWrapper() })

      await act(async () => {
        await result.current.mutateAsync({ name: 'new-process', template: 'test-template' })
      })
      expect(mockedApiClient.createProcessConfig).toHaveBeenCalledWith({
        name: 'new-process',
        template: 'test-template',
      })
    })
  })
  describe('authentication behavior', () => {
    it('does not fetch when not authenticated', async () => {
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: false } as ReturnType<typeof useAuth>)
      const { result } = renderHook(() => useSystemStatus(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(mockedApiClient.getSystemStatus).not.toHaveBeenCalled()
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: true } as ReturnType<typeof useAuth>)
    })
    it('useCandles does not fetch when not authenticated', async () => {
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: false } as ReturnType<typeof useAuth>)
      const { result } = renderHook(() => useCandles('BTC/USD'), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(mockedApiClient.getCandles).not.toHaveBeenCalled()
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: true } as ReturnType<typeof useAuth>)
    })
  })
})
