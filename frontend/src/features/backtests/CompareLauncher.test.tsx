import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { CompareLauncher } from './CompareLauncher'
import { apiClient } from '../../lib/apiClient'
import { useAppStore } from '../../stores/app'
import type { BacktestRunData } from '../../types/api'

vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getBacktests: vi.fn(),
    createBacktestComparison: vi.fn(),
  },
}))

const makeRun = (overrides: Partial<BacktestRunData> = {}): BacktestRunData =>
  ({
    type: 'backtest_run',
    sequence_id: 1,
    public_id: 'run-current',
    timestamp: '2026-03-01T00:00:00Z',
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
    config_hash: 'cfg-deadbeef',
    ...overrides,
  }) as BacktestRunData

const renderWithClient = (ui: ReactNode) => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const utils = render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)

  return { ...utils, qc }
}

const mockSameConfig = (runs: BacktestRunData[]) => {
  ;(apiClient.getBacktests as ReturnType<typeof vi.fn>).mockImplementation(
    (_l: number, _o: number, _strategy?: string, _status?: string, configHash?: string | null) => {
      if (configHash) {
        return Promise.resolve({ type: 'backtest_run_list', payload: runs, count: runs.length })
      }

      return Promise.resolve({ type: 'backtest_run_list', payload: [], count: 0 })
    }
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  useAppStore.setState({ currentWalletPublicId: 'wallet-1' })
  globalThis.location.hash = ''
})
afterEach(() => {
  useAppStore.setState({ currentWalletPublicId: null })
  globalThis.location.hash = ''
})

describe('CompareLauncher', () => {
  it('(g) renders the terminal-gate message + fires zero queries when run is not terminal', () => {
    renderWithClient(<CompareLauncher currentRun={makeRun({ status: 'running' })} />)
    expect(
      screen.getByText(/compare available once this run reaches a terminal status/i)
    ).toBeDefined()
    expect(apiClient.getBacktests).not.toHaveBeenCalled()
  })

  it('(a) renders auto-pair when hash present + candidates.length >= 1 (post-exclusion)', async () => {
    const sibling = makeRun({ public_id: 'run-sibling', status: 'completed' })

    mockSameConfig([sibling])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    expect(await screen.findByTestId('compare-auto-pair')).toBeDefined()
    expect(apiClient.getBacktests).toHaveBeenCalledWith(20, 0, undefined, undefined, 'cfg-deadbeef')
  })

  it('(b) hides auto-pair + shows "no other runs" when candidates.length === 0', async () => {
    mockSameConfig([])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await waitFor(() => expect(screen.getByTestId('compare-no-siblings')).toBeDefined())
    expect(screen.queryByTestId('compare-auto-pair')).toBeNull()
  })

  it('(c) shows manual-only (no auto-pair, no chip "same config") when config_hash is null', async () => {
    ;(apiClient.getBacktests as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_run_list',
      payload: [makeRun({ public_id: 'r-other', timestamp: '2026-02-01T00:00:00Z' })],
      count: 1,
    })
    renderWithClient(<CompareLauncher currentRun={makeRun({ config_hash: null })} />)
    await waitFor(() =>
      expect(screen.getByTestId('compare-source-chip').textContent).toBe('all runs')
    )
    expect(screen.queryByTestId('compare-auto-pair')).toBeNull()
    expect(screen.queryByTestId('compare-show-all')).toBeNull()
  })

  it('(d) on mount fires exactly one same-config query with the correct args', async () => {
    mockSameConfig([])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await waitFor(() => expect(screen.getByTestId('compare-no-siblings')).toBeDefined())
    expect((apiClient.getBacktests as ReturnType<typeof vi.fn>).mock.calls.length).toBe(1)
    expect(apiClient.getBacktests).toHaveBeenCalledWith(20, 0, undefined, undefined, 'cfg-deadbeef')
  })

  it('(e) "show all runs" toggle fires three parallel typed queries + excludes current run + excludes pending sibling', async () => {
    const completed = makeRun({
      public_id: 'r-c',
      status: 'completed',
      timestamp: '2026-03-03T00:00:00Z',
    })
    const failed = makeRun({
      public_id: 'r-f',
      status: 'failed',
      timestamp: '2026-03-02T00:00:00Z',
    })
    const cancelled = makeRun({
      public_id: 'r-x',
      status: 'cancelled',
      timestamp: '2026-03-01T00:00:00Z',
    })
    const sibling = makeRun({ public_id: 'r-sib', status: 'completed' })

    ;(apiClient.getBacktests as ReturnType<typeof vi.fn>).mockImplementation(
      (_l: number, _o: number, _s, status?: string, hash?: string | null) => {
        if (hash) {
          return Promise.resolve({ type: 'backtest_run_list', payload: [sibling], count: 1 })
        }

        if (status === 'completed') {
          return Promise.resolve({
            type: 'backtest_run_list',
            payload: [completed, makeRun()],
            count: 2,
          })
        }

        if (status === 'failed') {
          return Promise.resolve({ type: 'backtest_run_list', payload: [failed], count: 1 })
        }

        if (status === 'cancelled') {
          return Promise.resolve({ type: 'backtest_run_list', payload: [cancelled], count: 1 })
        }

        return Promise.resolve({ type: 'backtest_run_list', payload: [], count: 0 })
      }
    )
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await screen.findByTestId('compare-show-all')

    fireEvent.click(screen.getByTestId('compare-show-all'))

    await waitFor(() =>
      expect(screen.getByTestId('compare-source-chip').textContent).toBe('all runs')
    )
    await screen.findByText(/r-c/)

    const allCalls = (apiClient.getBacktests as ReturnType<typeof vi.fn>).mock.calls
    const statuses = allCalls.filter(c => c[3]).map(c => c[3])

    expect(statuses).toEqual(expect.arrayContaining(['completed', 'failed', 'cancelled']))

    const optionTexts = Array.from(
      (screen.getByTestId('compare-combobox') as HTMLSelectElement).options
    ).map(o => o.textContent)

    expect(optionTexts.some(t => t?.includes('r-c'))).toBe(true)
    expect(optionTexts.some(t => t?.includes('r-f'))).toBe(true)
    expect(optionTexts.some(t => t?.includes('r-x'))).toBe(true)
    expect(optionTexts.some(t => t?.includes('run-current'))).toBe(false)
  })

  it('(f) filters non-terminal sibling out of same-config combobox even though backend returns it', async () => {
    const pending = makeRun({ public_id: 'r-pending', status: 'pending' })
    const ok = makeRun({ public_id: 'r-ok', status: 'completed' })

    mockSameConfig([pending, ok])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await screen.findByTestId('compare-auto-pair')
    const optionTexts = Array.from(
      (screen.getByTestId('compare-combobox') as HTMLSelectElement).options
    ).map(o => o.textContent)

    expect(optionTexts.some(t => t?.includes('r-pending'))).toBe(false)
    expect(optionTexts.some(t => t?.includes('r-ok'))).toBe(true)
  })

  it('auto-pair click POSTs correct body + navigates to #backtests/compare/{id}', async () => {
    const sibling = makeRun({ public_id: 'run-sibling' })

    mockSameConfig([sibling])
    ;(apiClient.createBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_comparison_response',
      payload: { public_id: 'cmp-new' },
    })
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    fireEvent.click(await screen.findByTestId('compare-auto-pair'))
    await waitFor(() => expect(globalThis.location.hash).toBe('#backtests/compare/cmp-new'))
    expect(apiClient.createBacktestComparison).toHaveBeenCalledWith({
      mode: 'auto',
      config_hash: 'cfg-deadbeef',
      anchor_run_public_id: 'run-current',
    })
  })

  it('manual submit with no selection shows client error', async () => {
    mockSameConfig([makeRun({ public_id: 'r-other' })])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    fireEvent.click(await screen.findByTestId('compare-manual-submit'))
    expect(screen.getByTestId('compare-client-error').textContent).toBe(
      'select a run from the list'
    )
    expect(apiClient.createBacktestComparison).not.toHaveBeenCalled()
  })

  it('manual submit POSTs run_a + run_b body shape', async () => {
    const other = makeRun({ public_id: 'r-other' })

    mockSameConfig([other])
    ;(apiClient.createBacktestComparison as ReturnType<typeof vi.fn>).mockResolvedValue({
      type: 'backtest_comparison_response',
      payload: { public_id: 'cmp-manual' },
    })
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await screen.findByText(/r-other/)
    fireEvent.change(screen.getByTestId('compare-combobox'), { target: { value: 'r-other' } })
    fireEvent.click(screen.getByTestId('compare-manual-submit'))
    await waitFor(() => expect(globalThis.location.hash).toBe('#backtests/compare/cmp-manual'))
    expect(apiClient.createBacktestComparison).toHaveBeenCalledWith({
      mode: 'manual',
      run_a_public_id: 'run-current',
      run_b_public_id: 'r-other',
    })
  })

  it('shows overflow note when same-config returns >= SAME_CONFIG_LIMIT (20)', async () => {
    const siblings = Array.from({ length: 21 }, (_, i) =>
      makeRun({
        public_id: `sib-${i}`,
        timestamp: `2026-02-${String(i + 1).padStart(2, '0')}T00:00:00Z`,
      })
    )

    mockSameConfig(siblings)
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    expect(await screen.findByText(/showing 20 most recent/i)).toBeDefined()
  })

  it('blocks self-compare client-side when picked id equals currentRun.public_id', async () => {
    const self = makeRun()
    const other = makeRun({ public_id: 'r-other' })

    mockSameConfig([self, other])
    renderWithClient(<CompareLauncher currentRun={makeRun()} />)
    await screen.findByTestId('compare-combobox')
    const select = screen.getByTestId('compare-combobox') as HTMLSelectElement

    expect(Array.from(select.options).some(o => o.value === 'run-current')).toBe(false)
  })
})
