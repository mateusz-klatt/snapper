import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createElement, type ReactNode } from 'react'
import { ComparePage } from './ComparePage'
import { apiClient, APIError } from '../../lib/apiClient'
import { useAppStore } from '../../stores/app'

vi.mock('../../lib/apiClient', async () => {
  const actual = await vi.importActual('../../lib/apiClient')

  return {
    ...actual,
    apiClient: {
      getBacktestComparison: vi.fn(),
    },
  }
})

vi.mock('./compare/MetricsDiffTable', () => ({
  MetricsDiffTable: ({ rows }: { rows: unknown[] }) => (
    <div data-testid='mock-metrics'>{rows.length}</div>
  ),
}))

vi.mock('./compare/EquityOverlayChart', () => ({
  EquityOverlayChart: ({ points }: { points: unknown[] }) => (
    <div data-testid='mock-equity'>{points.length}</div>
  ),
}))

vi.mock('./compare/TradesDiffList', () => ({
  TradesDiffList: ({ entries }: { entries: unknown[] }) => (
    <div data-testid='mock-trades'>{entries.length}</div>
  ),
}))

vi.mock('./compare/SignalsDiffList', () => ({
  SignalsDiffList: ({ entries }: { entries: unknown[] }) => (
    <div data-testid='mock-signals'>{entries.length}</div>
  ),
}))

const renderWithClient = (ui: ReactNode) => {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, retryDelay: 0 } },
  })

  return render(createElement(QueryClientProvider, { client: qc }, ui))
}

const makeRun = (overrides: Record<string, unknown> = {}) => ({
  type: 'backtest_run',
  sequence_id: 1,
  public_id: 'run-aaaaaaaa',
  timestamp: '2026-01-01T00:00:00Z',
  session_id: 's1',
  wallet_public_id: 'wallet-1',
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
  ...overrides,
})

const makeDetail = (overrides: Record<string, unknown> = {}) => ({
  type: 'backtest_comparison_detail_response',
  payload: {
    comparison: {
      type: 'backtest_comparison',
      public_id: 'cmp-1',
      pairing_mode: 'auto',
      config_hash: 'cfg-deadbeef',
    },
    run_a: makeRun({ public_id: 'run-aaaaaaaa', status: 'completed' }),
    run_b: makeRun({ public_id: 'run-bbbbbbbb', status: 'failed' }),
    metrics_diff: [{ name: 'sharpe', run_a: 1, run_b: 2, delta: 1, pct: 1 }],
    equity_overlay: [{ point_time: '2026-01-01T00:00:00Z', equity_a: 100, equity_b: 200 }],
    trades_diff: [{ leg: 'common' }],
    signals_diff: [{ leg: 'common' }, { leg: 'a' }],
    ...overrides,
  },
})

beforeEach(() => {
  vi.clearAllMocks()
  useAppStore.setState({ currentWalletPublicId: 'wallet-1' })
})

describe('ComparePage', () => {
  it('renders loading state while query is in flight', () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockReturnValue(
      new Promise(() => {})
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(screen.getByText(/Loading comparison/i)).toBeDefined()
  })

  it('renders all 4 panels when query succeeds', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue(makeDetail())
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(await screen.findByTestId('mock-metrics')).toBeDefined()
    expect(screen.getByTestId('mock-equity')).toBeDefined()
    expect(screen.getByTestId('mock-trades')).toBeDefined()
    expect(screen.getByTestId('mock-signals')).toBeDefined()
    expect(screen.getByTestId('compare-run-a-label').textContent).toContain('Run A')
    expect(screen.getByTestId('compare-run-b-label').textContent).toContain('Run B')
    expect(screen.getByTestId('compare-pairing-mode').textContent).toBe('auto')
  })

  it('shows "not found in current wallet" message on APIError 404', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockRejectedValue(
      new APIError('Comparison not found', 404, 'Not Found')
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-missing' />)
    expect(await screen.findByText(/Comparison not found in current wallet/i)).toBeDefined()
    expect(screen.getByText(/Back to backtests/i)).toBeDefined()
    expect(screen.queryByTestId('mock-metrics')).toBeNull()
  })

  it('shows generic error fallback on non-404 error', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockRejectedValue(
      new APIError('Server boom', 500, 'Internal Server Error')
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(await screen.findByText(/Failed to load comparison.*Server boom/i)).toBeDefined()
  })

  it('shows generic error fallback on non-Error throw (covers "unknown error" branch)', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockRejectedValue('string-error')
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(await screen.findByText(/Failed to load comparison.*unknown error/i)).toBeDefined()
  })

  it('shows empty-payload fallback when payload is null', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_comparison_detail_response',
      payload: null,
    })
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(await screen.findByText(/Empty comparison response/i)).toBeDefined()
  })

  it('renders config_hash badge when present and skips when null', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue(
      makeDetail({
        comparison: {
          type: 'backtest_comparison',
          public_id: 'cmp-1',
          pairing_mode: 'manual',
          config_hash: null,
        },
      })
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    expect(await screen.findByTestId('compare-pairing-mode')).toBeDefined()
    expect(screen.queryByText('cfg-deadbeef')).toBeNull()
  })

  it('renders unknown run status with default muted color', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue(
      makeDetail({ run_a: makeRun({ public_id: 'run-aaaaaaaa', status: 'mystery' }) })
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    const labelA = await screen.findByTestId('compare-run-a-label')

    expect(labelA.className).toContain('text-muted-400')
  })

  it('renders unknown run_b status with default muted color', async () => {
    ;(apiClient.getBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue(
      makeDetail({ run_b: makeRun({ public_id: 'run-bbbbbbbb', status: 'unicorn' }) })
    )
    renderWithClient(<ComparePage comparisonPublicId='cmp-1' />)
    const labelB = await screen.findByTestId('compare-run-b-label')

    expect(labelB.className).toContain('text-muted-400')
  })
})
