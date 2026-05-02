import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Backtests } from './Backtests'

vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    isAuthenticated: true,
  })),
}))
vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ asOf: null, isTimeTraveling: false })
  ),
}))

const mockGetBacktests = vi.fn()
const mockCancelBacktest = vi.fn()
const mockRerunBacktest = vi.fn()

vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getBacktests: (...args: unknown[]) => mockGetBacktests(...args),
    cancelBacktest: (...args: unknown[]) => mockCancelBacktest(...args),
    rerunBacktest: (...args: unknown[]) => mockRerunBacktest(...args),
  },
}))

const NOW = new Date().toISOString()

function makeRun(overrides: Record<string, unknown> = {}) {
  return {
    type: 'backtest_run',
    public_id: 'run-abc123',
    timestamp: NOW,
    session_id: 's1',
    sequence_id: 1,
    wallet_public_id: 'w-1',
    strategy_name: 'sma_cross',
    strategy_params: {},
    instrument_public_id: 'BTC-USD',
    exchange: 'kraken',
    timeframe: '1h',
    start_date: '2026-01-01T00:00:00Z',
    end_date: '2026-06-01T00:00:00Z',
    initial_cash: 10000,
    status: 'completed',
    started_at: null,
    completed_at: null,
    error: null,
    ...overrides,
  }
}

function renderWithQuery(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('Backtests', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders empty state when no backtests', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [],
      count: 0,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByText('No backtests')).toBeInTheDocument()
  })

  it('renders backtest runs', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun()],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByText('sma_cross')).toBeInTheDocument()
    expect(screen.getByText('BTC-USD')).toBeInTheDocument()
    expect(screen.getByText('completed')).toBeInTheDocument()
  })

  it('renders cross-asset attribution arrow when target_execution_exchange is set', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ exchange: 'kraken_futures', target_execution_exchange: 'kraken' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByText(/→ kraken/)).toBeInTheDocument()
  })

  it('omits the cross-asset arrow when target_execution_exchange is null', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun()],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    await screen.findByText('sma_cross')
    expect(screen.queryByText(/→/)).not.toBeInTheDocument()
  })

  it('renders loading skeletons', () => {
    mockGetBacktests.mockReturnValue(new Promise(() => {}))

    renderWithQuery(<Backtests />)
    expect(document.querySelectorAll('[class*="animate-pulse"]').length).toBeGreaterThan(0)
  })

  it('shows cancel button for running backtests', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'running' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByTestId('cancel-run-abc123')).toBeInTheDocument()
  })

  it('hides cancel button for completed backtests', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'completed' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    await screen.findByText('sma_cross')
    expect(screen.queryByTestId('cancel-run-abc123')).not.toBeInTheDocument()
  })

  it('shows rerun button for all backtests', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'completed' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByTestId('rerun-run-abc123')).toBeInTheDocument()
  })

  it('filters by status via server-side query', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'completed' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    await screen.findByText('sma_cross')

    const select = screen.getByTestId('status-filter')

    fireEvent.change(select, { target: { value: 'failed' } })
    await waitFor(() => {
      expect(mockGetBacktests).toHaveBeenCalledWith(50, 0, undefined, 'failed')
    })
  })

  it('renders pending status with default color', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'pending' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByText('pending')).toBeInTheDocument()
  })

  it('shows error message for failed backtests', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'failed', error: 'No candle data found' })],
      count: 1,
    })

    renderWithQuery(<Backtests />)
    expect(await screen.findByText(/No candle data found/)).toBeInTheDocument()
  })

  it('calls cancel mutation on button click', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'running' })],
      count: 1,
    })
    mockCancelBacktest.mockResolvedValue({})

    renderWithQuery(<Backtests />)
    const cancelBtn = await screen.findByTestId('cancel-run-abc123')

    fireEvent.click(cancelBtn)
    await waitFor(() => {
      expect(mockCancelBacktest).toHaveBeenCalledWith('run-abc123')
    })
  })

  it('calls rerun mutation on button click', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun()],
      count: 1,
    })
    mockRerunBacktest.mockResolvedValue({})

    renderWithQuery(<Backtests />)
    const rerunBtn = await screen.findByTestId('rerun-run-abc123')

    fireEvent.click(rerunBtn)
    await waitFor(() => {
      expect(mockRerunBacktest).toHaveBeenCalledWith('run-abc123')
    })
  })

  it('renders a navigable anchor on each row pointing at #backtests/{id}', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun()],
      count: 1,
    })
    renderWithQuery(<Backtests />)
    const anchor = await screen.findByTestId('open-run-abc123')

    expect(anchor.tagName).toBe('A')
    expect(anchor.getAttribute('href')).toBe('#backtests/run-abc123')
  })

  it('cancel/rerun buttons are siblings of the row anchor, not descendants', async () => {
    mockGetBacktests.mockResolvedValue({
      type: 'backtest_run_list',
      session_id: 's1',
      sequence_id: 1,
      public_id: 'resp-1',
      timestamp: NOW,
      payload: [makeRun({ status: 'running' })],
      count: 1,
    })
    renderWithQuery(<Backtests />)
    const anchor = await screen.findByTestId('open-run-abc123')
    const cancelBtn = await screen.findByTestId('cancel-run-abc123')
    const rerunBtn = await screen.findByTestId('rerun-run-abc123')

    expect(anchor.contains(cancelBtn)).toBe(false)
    expect(anchor.contains(rerunBtn)).toBe(false)
    fireEvent.click(anchor)
    expect(mockCancelBacktest).not.toHaveBeenCalled()
    expect(mockRerunBacktest).not.toHaveBeenCalled()
  })
})
