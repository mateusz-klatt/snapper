import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BacktestProgressBar } from './BacktestProgressBar'
import type { BacktestProgressSnapshot } from './hooks/useBacktestProgressSubscription'

const baseSnapshot: BacktestProgressSnapshot = {
  type: 'backtest_progress',
  sequence_id: 1,
  public_id: 'p1',
  timestamp: '2026-01-01T00:00:00Z',
  session_id: 's1',
  run_public_id: 'r1',
  wallet_public_id: 'w1',
  event: 'progress',
  candles_done: 10,
  total_candles: 100,
  signals_count: 2,
  trades_count: 1,
  equity: 1234.5,
  progress_pct: 0.42,
}

describe('BacktestProgressBar', () => {
  it('renders waiting message when snapshot is null', () => {
    render(<BacktestProgressBar snapshot={null} />)
    expect(screen.getByText(/Waiting for progress/i)).toBeDefined()
  })
  it('renders bar width from progress_pct', () => {
    render(<BacktestProgressBar snapshot={baseSnapshot} />)
    const fill = screen.getByTestId('bt-progress-fill') as HTMLElement

    expect(fill.style.width).toBe('42%')
    expect(screen.getByText(/10 \/ 100 candles/)).toBeDefined()
    expect(screen.getByText(/equity 1234.50/)).toBeDefined()
    expect(screen.getByText(/2 sig \/ 1 trades/)).toBeDefined()
  })
  it('omits "/ total" when total_candles is null', () => {
    render(<BacktestProgressBar snapshot={{ ...baseSnapshot, total_candles: null }} />)
    expect(screen.getByText(/^10 candles$/)).toBeDefined()
  })
  it('renders milestone label when event is milestone', () => {
    render(
      <BacktestProgressBar snapshot={{ ...baseSnapshot, event: 'milestone', milestone: '50pct' }} />
    )
    expect(screen.getByText(/milestone 50 %/i)).toBeDefined()
  })
  it('omits milestone label when not a milestone event', () => {
    render(<BacktestProgressBar snapshot={baseSnapshot} />)
    expect(screen.queryByText(/milestone/i)).toBeNull()
  })
  it('treats missing progress_pct as zero', () => {
    const snap = { ...baseSnapshot, progress_pct: undefined } as unknown as BacktestProgressSnapshot

    render(<BacktestProgressBar snapshot={snap} />)
    const fill = screen.getByTestId('bt-progress-fill') as HTMLElement

    expect(fill.style.width).toBe('0%')
  })
})
