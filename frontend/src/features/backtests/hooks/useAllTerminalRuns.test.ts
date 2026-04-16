import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createElement, type ReactNode } from 'react'
import { useAllTerminalRuns } from './useAllTerminalRuns'
import { apiClient } from '../../../lib/apiClient'
import { useAppStore } from '../../../stores/app'

vi.mock('../../../lib/apiClient', () => ({
  apiClient: {
    getBacktests: vi.fn(),
  },
}))

const makeRun = (publicId: string, ts: string) => ({
  type: 'backtest_run',
  sequence_id: 1,
  public_id: publicId,
  timestamp: ts,
  session_id: 's1',
  wallet_public_id: 'w-1',
  strategy_name: 'sma',
  strategy_params: {},
  instrument_public_id: 'BTC-USD',
  exchange: 'kraken',
  timeframe: '1m',
  start_date: '2026-01-01T00:00:00Z',
  end_date: '2026-06-01T00:00:00Z',
  initial_cash: 10000,
  status: 'completed',
  execution_mode: 'direct_db',
  fill_model: 'next_open',
  slippage_bps: 0,
  commission_bps: 0,
})

const renderWithClient = () => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(QueryClientProvider, { client: queryClient }, children)

  return { queryClient, wrapper }
}

describe('useAllTerminalRuns', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAppStore.setState({ currentWalletPublicId: 'wallet-1' })
  })
  afterEach(() => {
    useAppStore.setState({ currentWalletPublicId: null })
  })

  it('fires three parallel typed queries (completed/failed/cancelled) merged + sorted desc', async () => {
    const mock = apiClient.getBacktests as ReturnType<typeof vi.fn>

    mock.mockImplementation((_l: number, _o: number, _s, status: string) => {
      if (status === 'completed') {
        return Promise.resolve({
          type: 'backtest_run_list',
          payload: [makeRun('r-completed', '2026-03-01T00:00:00Z')],
          count: 1,
        })
      }

      if (status === 'failed') {
        return Promise.resolve({
          type: 'backtest_run_list',
          payload: [makeRun('r-failed', '2026-03-03T00:00:00Z')],
          count: 1,
        })
      }

      if (status === 'cancelled') {
        return Promise.resolve({
          type: 'backtest_run_list',
          payload: [makeRun('r-cancelled', '2026-03-02T00:00:00Z')],
          count: 1,
        })
      }

      return Promise.resolve({ type: 'backtest_run_list', payload: [], count: 0 })
    })
    const { wrapper } = renderWithClient()
    const { result } = renderHook(() => useAllTerminalRuns({ enabled: true }), { wrapper })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(mock).toHaveBeenCalledTimes(3)
    expect(mock).toHaveBeenCalledWith(50, 0, undefined, 'completed')
    expect(mock).toHaveBeenCalledWith(50, 0, undefined, 'failed')
    expect(mock).toHaveBeenCalledWith(50, 0, undefined, 'cancelled')
    expect(result.current.data?.map(r => r.public_id)).toEqual([
      'r-failed',
      'r-cancelled',
      'r-completed',
    ])
  })

  it('is idle when enabled is false', () => {
    const { wrapper } = renderWithClient()
    const { result } = renderHook(() => useAllTerminalRuns({ enabled: false }), { wrapper })

    expect(result.current.fetchStatus).toBe('idle')
    expect(apiClient.getBacktests).not.toHaveBeenCalled()
  })

  it('handles empty payloads gracefully (defensive .payload ?? [])', async () => {
    ;(apiClient.getBacktests as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_list',
      payload: undefined,
      count: 0,
    })
    const { wrapper } = renderWithClient()
    const { result } = renderHook(() => useAllTerminalRuns({ enabled: true }), { wrapper })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data).toEqual([])
  })

  it('scopes the query key by walletId', async () => {
    ;(apiClient.getBacktests as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_list',
      payload: [],
      count: 0,
    })
    const { queryClient, wrapper } = renderWithClient()

    renderHook(() => useAllTerminalRuns({ enabled: true }), { wrapper })
    await waitFor(() =>
      expect(
        queryClient.getQueryCache().find({ queryKey: ['backtests', 'all-terminal', 'wallet-1'] })
      ).toBeDefined()
    )
  })
})
