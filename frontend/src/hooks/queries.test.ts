import { createElement, type ReactNode } from 'react'
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  useSystemStatus,
  useCandles,
  useExchanges,
  useExchangeInstruments,
  useOrders,
  useExecutions,
  useAvailableProcesses,
  useConfiguredProcesses,
  useProcessSummary,
  useStrategies,
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

const ENV = {
  seq: 0,
  pid: 'test-pid',
  ts: '2024-01-01T00:00:00Z',
  sid: 'test-sid',
}

function envelope<T extends string>(type: T, extra: Record<string, unknown> = {}) {
  return {
    type,
    sequence_id: ENV.seq,
    public_id: ENV.pid,
    timestamp: ENV.ts,
    session_id: ENV.sid,
    ...extra,
  }
}

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    getSystemStatus: vi.fn(() =>
      Promise.resolve(
        envelope('system_status_response', {
          payload: envelope('system_status', {
            trader: { status: 'running' },
            backtests: {},
          }),
        })
      )
    ),
    getCandles: vi.fn(() => Promise.resolve([])),
    getExchanges: vi.fn(() =>
      Promise.resolve(envelope('exchange_list', { payload: ['kraken', 'binance'], count: 2 }))
    ),
    getExchangeInstruments: vi.fn(() =>
      Promise.resolve(envelope('instrument_list', { payload: ['BTC/USD', 'ETH/USD'], count: 2 }))
    ),
    getOrders: vi.fn(() => Promise.resolve(envelope('order_list', { payload: [], count: 0 }))),
    getExecutions: vi.fn(() =>
      Promise.resolve(envelope('execution_list', { payload: [], count: 0 }))
    ),
    getPositions: vi.fn(() =>
      Promise.resolve(envelope('position_list', { payload: [], count: 0 }))
    ),
    getSignals: vi.fn(() => Promise.resolve(envelope('signal_list', { payload: [], count: 0 }))),
    getAvailableProcesses: vi.fn(() =>
      Promise.resolve(envelope('available_processes', { payload: [], count: 0 }))
    ),
    getConfiguredProcesses: vi.fn(() =>
      Promise.resolve(envelope('configured_processes', { payload: [], count: 0 }))
    ),
    getProcessSummary: vi.fn(() =>
      Promise.resolve(
        envelope('process_summary_response', {
          payload: envelope('process_summary', {
            feeds: { running: 0, total: 0 },
            strategies: { running: 0, total: 0 },
            executors: { running: 0, total: 0 },
            brokers: { running: 0, total: 0 },
          }),
        })
      )
    ),
    getStrategies: vi.fn(() =>
      Promise.resolve(
        envelope('strategy_list', {
          payload: [
            envelope('strategy_process', {
              name: 'strategy_test',
              running: false,
              enabled: true,
              mode: 'thread',
            }),
          ],
          count: 1,
        })
      )
    ),
    getProcessSchema: vi.fn(() =>
      Promise.resolve(
        envelope('process_schema_response', {
          payload: envelope('process_schema', {
            name: 'collector',
            description: '',
            class_path: '',
            method: '',
            default_enabled: true,
            default_mode: 'thread',
            lifecycle: 'long_running',
          }),
        })
      )
    ),
    getProcessRuns: vi.fn(() =>
      Promise.resolve(envelope('process_runs', { payload: [], count: 0 }))
    ),
    startProcessByName: vi.fn(() =>
      Promise.resolve(
        envelope('process_start_response', {
          payload: envelope('process_start', {
            status: 'success',
            name: 'collector',
            message: 'started',
          }),
        })
      )
    ),
    stopProcessByName: vi.fn(() =>
      Promise.resolve(
        envelope('process_stop_response', {
          payload: envelope('process_stop', {
            status: 'success',
            name: 'collector',
            message: 'stopped',
          }),
        })
      )
    ),
    createProcessConfig: vi.fn(() =>
      Promise.resolve(
        envelope('process_create_response', {
          payload: envelope('process_create', {
            status: 'created',
            process: { name: 'test', template: 'test-template' },
          }),
        })
      )
    ),
  },
}))
vi.mock('../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    isAuthenticated: true,
  })),
}))
vi.mock('../lib/transforms', () => ({
  safeOrderFromAPI: vi.fn(o => o),
  safeExecutionFromAPI: vi.fn(e => e),
  safeSignalFromAPI: vi.fn(s => s),
  positionFromAPI: vi.fn(p => ({
    publicId: p.public_id,
    timestamp: p.timestamp ? new Date(p.timestamp) : undefined,
    instrument: p.instrument ?? '',
    exchange: p.exchange ?? '',
    quantity: p.quantity ?? 0,
    averagePrice: p.average_price ?? 0,
    unrealizedPnl: p.unrealized_pnl ?? 0,
    realizedPnl: p.realized_pnl ?? 0,
  })),
}))
const mockedApiClient = apiClient as unknown as {
  getSystemStatus: Mock
  getCandles: Mock
  getExchanges: Mock
  getExchangeInstruments: Mock
  getOrders: Mock
  getExecutions: Mock
  getPositions: Mock
  getSignals: Mock
  getAvailableProcesses: Mock
  getConfiguredProcesses: Mock
  getProcessSummary: Mock
  getStrategies: Mock
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
      const { result } = renderHook(() => useCandles('EUR-USD', 'kraken', '1h'), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
    })
    it('does not fetch when enabled is false', async () => {
      const { result } = renderHook(() => useCandles('EUR-USD', 'kraken', '1h', 100, false), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeUndefined()
    })
  })
  describe('useExchanges', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useExchanges(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data?.payload).toEqual(['kraken', 'binance'])
    })
  })
  describe('useExchangeInstruments', () => {
    it('returns data when exchange is provided', async () => {
      const { result } = renderHook(() => useExchangeInstruments('kraken'), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data?.payload).toEqual(['BTC/USD', 'ETH/USD'])
    })
    it('does not fetch when exchange is null', async () => {
      const { result } = renderHook(() => useExchangeInstruments(null), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeUndefined()
      expect(mockedApiClient.getExchangeInstruments).not.toHaveBeenCalled()
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
      mockedApiClient.getExecutions.mockResolvedValueOnce(
        envelope('execution_list', {
          payload: [null, { public_id: 'exec-1' }],
          count: 2,
        })
      )
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
      mockedApiClient.getSignals.mockResolvedValueOnce(
        envelope('signal_list', {
          payload: [
            {
              exchange: 'kraken',
              instrument: 'BTC/USD',
              side: 'buy',
              strength: 0.5,
              reason: 'oldest',
              strategyName: 'test',
              price: 50000,
              firedAt: new Date('2026-01-15T10:00:00Z'),
            },
            {
              exchange: 'kraken',
              instrument: 'BTC/USD',
              side: 'buy',
              strength: 0.6,
              reason: 'newest',
              strategyName: 'test',
              price: 50100,
              firedAt: new Date('2026-01-15T12:00:00Z'),
            },
            {
              exchange: 'kraken',
              instrument: 'BTC/USD',
              side: 'sell',
              strength: 0.4,
              reason: 'middle',
              strategyName: 'test',
              price: 49900,
              firedAt: new Date('2026-01-15T11:00:00Z'),
            },
          ],
          count: 3,
        }) as never
      )
      const { result } = renderHook(() => useLatestSignals(2), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toHaveLength(2)
      expect(result.current.data?.[0].reason).toBe('newest')
      expect(result.current.data?.[1].reason).toBe('middle')
    })
    it('handles signals with undefined timestamp in sorting', async () => {
      mockedApiClient.getSignals.mockResolvedValueOnce(
        envelope('signal_list', {
          payload: [
            {
              exchange: 'kraken',
              instrument: 'BTC/USD',
              side: 'buy',
              strength: 0.5,
              reason: 'with-timestamp',
              strategyName: 'test',
              price: 50000,
              firedAt: new Date('2026-01-15T10:00:00Z'),
            },
            {
              exchange: 'kraken',
              instrument: 'ETH/USD',
              side: 'sell',
              strength: 0.6,
              reason: 'no-timestamp',
              strategyName: 'test',
              price: 3000,
              firedAt: undefined,
            },
            {
              exchange: 'kraken',
              instrument: 'SOL/USD',
              side: 'buy',
              strength: 0.4,
              reason: 'newer-timestamp',
              strategyName: 'test',
              price: 100,
              firedAt: new Date('2026-01-15T12:00:00Z'),
            },
          ],
          count: 3,
        }) as never
      )
      const { result } = renderHook(() => useLatestSignals(3), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toHaveLength(3)
      expect(result.current.data?.[0].reason).toBe('newer-timestamp')
      expect(result.current.data?.[1].reason).toBe('with-timestamp')
      expect(result.current.data?.[2].reason).toBe('no-timestamp')
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
  describe('useProcessSummary', () => {
    it('returns data when authenticated', async () => {
      const { result } = renderHook(() => useProcessSummary(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
      expect(result.current.data?.payload?.feeds).toEqual({ running: 0, total: 0 })
    })
  })
  describe('useStrategies', () => {
    it('returns strategy list when authenticated', async () => {
      const { result } = renderHook(() => useStrategies(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeDefined()
      expect(result.current.data?.payload).toHaveLength(1)
      expect(result.current.data?.payload[0].name).toBe('strategy_test')
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
      mockedApiClient.getOrders.mockResolvedValueOnce(
        envelope('order_list', {
          payload: [
            {
              public_id: '1',
              status: 'NEW',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
            {
              public_id: '2',
              status: 'OPEN',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
            {
              public_id: '3',
              status: 'FILLED',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
            {
              public_id: '4',
              status: 'PARTIALLY_FILLED',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
            {
              public_id: '5',
              status: 'CANCELLED',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
            {
              public_id: '6',
              status: 'REJECTED',
              instrument: 'BTC/USD',
              side: 'buy',
              price: 100,
              quantity: 1,
            },
          ],
          count: 6,
        })
      )
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
      mockedApiClient.getOrders.mockResolvedValueOnce(null as never)
      const { result } = renderHook(() => useOrdersGrouped(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeNull()
    })
  })
  describe('usePositionsSummary', () => {
    it('calculates position summary', async () => {
      mockedApiClient.getPositions.mockResolvedValueOnce(
        envelope('position_list', {
          payload: [
            {
              type: 'position' as const,
              public_id: '1',
              timestamp: new Date().toISOString(),
              instrument: 'BTC/USD',
              exchange: 'kraken' as const,
              quantity: 10,
              average_price: 100,
              unrealized_pnl: 50,
              realized_pnl: 20,
            },
            {
              type: 'position' as const,
              public_id: '2',
              timestamp: new Date().toISOString(),
              instrument: 'ETH/USD',
              exchange: 'kraken' as const,
              quantity: 5,
              average_price: 200,
              unrealized_pnl: -10,
              realized_pnl: 30,
            },
          ],
          count: 2,
        })
      )
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
      mockedApiClient.getPositions.mockResolvedValueOnce(null as never)
      const { result } = renderHook(() => usePositionsSummary(), { wrapper: createWrapper() })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(result.current.data).toBeNull()
    })
    it('handles zero totalCost', async () => {
      mockedApiClient.getPositions.mockResolvedValueOnce(
        envelope('position_list', {
          payload: [
            {
              public_id: '1',
              instrument: 'BTC/USD',
              quantity: 0,
              average_price: 0,
              unrealized_pnl: 0,
              realized_pnl: 0,
            },
          ],
          count: 1,
        })
      )
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
      const { result } = renderHook(() => useCandles('BTC/USD', 'kraken'), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(mockedApiClient.getCandles).not.toHaveBeenCalled()
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: true } as ReturnType<typeof useAuth>)
    })
    it('usePositionsSummary does not fetch when not authenticated', async () => {
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: false } as ReturnType<typeof useAuth>)
      const { result } = renderHook(() => usePositionsSummary(), {
        wrapper: createWrapper(),
      })

      await waitFor(() => {
        expect(result.current.isLoading).toBe(false)
      })
      expect(mockedApiClient.getPositions).not.toHaveBeenCalled()
      vi.mocked(useAuth).mockReturnValue({ isAuthenticated: true } as ReturnType<typeof useAuth>)
    })
  })
})
