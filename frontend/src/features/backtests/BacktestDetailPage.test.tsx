import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { BacktestDetailPage } from './BacktestDetailPage'
import { apiClient } from '../../lib/apiClient'

vi.mock('./hooks/useBacktestProgressSubscription', () => ({
  useBacktestProgressSubscription: (runId: string | null) => {
    if (!runId) return null

    return {
      type: 'backtest_progress',
      sequence_id: 1,
      public_id: 'p1',
      timestamp: '2026-01-01T00:00:00Z',
      session_id: 's1',
      run_public_id: runId,
      wallet_public_id: 'w1',
      event: 'progress',
      candles_done: 1,
      total_candles: 10,
      signals_count: 0,
      trades_count: 0,
      equity: 100,
      progress_pct: 0.1,
    }
  },
}))

vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getBacktest: vi.fn(),
  },
}))

vi.mock('./CompareLauncher', () => ({
  CompareLauncher: ({ currentRun }: { currentRun: { public_id: string; status: string } }) => (
    <div data-testid='compare-launcher-mount'>
      {currentRun.public_id}|{currentRun.status}
    </div>
  ),
}))

const VALID_RUN = '01948f94-1234-7abc-8def-1234567890ab'

const makeRunPayload = (overrides: Record<string, unknown> = {}) => ({
  type: 'backtest_run' as const,
  sequence_id: 1,
  public_id: VALID_RUN,
  timestamp: '2026-01-01T00:00:00Z',
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
  config_hash: 'cfg-hash-deadbeef',
  ...overrides,
})

const renderWithQuery = (ui: ReactNode) => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('BacktestDetailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('shows error when runPublicId is not a UUID7 + suppresses the fetch', () => {
    renderWithQuery(<BacktestDetailPage runPublicId='not-a-uuid' />)
    expect(screen.getByText(/Invalid run id/i)).toBeDefined()
    expect(screen.getByText(/not-a-uuid/)).toBeDefined()
    expect(apiClient.getBacktest).not.toHaveBeenCalled()
  })
  it('renders progress bar + UUID while loading the run', () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockReturnValue(new Promise(() => {}))
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    expect(screen.getByText('Backtest run')).toBeDefined()
    expect(screen.getByText(VALID_RUN)).toBeDefined()
    expect(screen.getByTestId('bt-progress-fill')).toBeDefined()
    expect(screen.queryByTestId('compare-launcher-mount')).toBeNull()
  })
  it('shows run summary + mounts CompareLauncher with run data when fetch succeeds', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: makeRunPayload(),
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)

    const launcher = await screen.findByTestId('compare-launcher-mount')

    expect(launcher.textContent).toBe(`${VALID_RUN}|completed`)
    expect(screen.getByText('sma')).toBeDefined()
    expect(screen.getByText('BTC-USD')).toBeDefined()
    expect(screen.getByText('completed')).toBeDefined()
    expect(screen.getByText('cfg-hash-deadbeef')).toBeDefined()
  })
  it('renders error fallback when fetch fails', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('boom'))
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    expect(await screen.findByText(/Failed to load run.*boom/i)).toBeDefined()
    expect(screen.getByText(/Back to backtests/i)).toBeDefined()
    expect(screen.queryByTestId('compare-launcher-mount')).toBeNull()
  })
  it('omits config_hash row when run carries no hash (pre-0006 run)', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: makeRunPayload({ config_hash: null }),
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    await waitFor(() => expect(screen.getByTestId('compare-launcher-mount')).toBeTruthy())
    expect(screen.queryByText('Config hash')).toBeNull()
  })
  it('renders fallback when payload is empty', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: null,
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    expect(await screen.findByText(/Failed to load run.*not found/i)).toBeDefined()
  })
  it('renders unknown-status with default styling', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: makeRunPayload({ status: 'mystery' }),
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    const statusEl = await screen.findByText('mystery')

    expect(statusEl.className).toContain('text-muted-400')
  })
  it('renders cross-asset attribution row when target_execution_exchange is set', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: makeRunPayload({
        exchange: 'kraken_futures',
        target_execution_exchange: 'kraken',
      }),
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)

    expect(await screen.findByText('Cross-asset attribution')).toBeDefined()
    expect(screen.getByText(/kraken_futures feed → kraken fills/)).toBeDefined()
  })
  it('omits the cross-asset row when target_execution_exchange is null', async () => {
    ;(apiClient.getBacktest as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_response',
      payload: makeRunPayload({ target_execution_exchange: null }),
    })
    renderWithQuery(<BacktestDetailPage runPublicId={VALID_RUN} />)
    await waitFor(() => expect(screen.getByTestId('compare-launcher-mount')).toBeTruthy())
    expect(screen.queryByText('Cross-asset attribution')).toBeNull()
  })
})
