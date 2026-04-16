import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CompareLauncher } from './CompareLauncher'
import type { BacktestRunData } from '../../types/api'

const stubRun = {
  type: 'backtest_run' as const,
  sequence_id: 1,
  public_id: 'run-stub',
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
} as unknown as BacktestRunData

describe('CompareLauncher (Step 4 stub)', () => {
  it('renders the run public_id in a marker element', () => {
    render(<CompareLauncher currentRun={stubRun} />)
    expect(screen.getByTestId('compare-launcher-stub').textContent).toBe('run-stub')
  })
})
