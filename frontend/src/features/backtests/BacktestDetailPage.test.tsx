import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BacktestDetailPage } from './BacktestDetailPage'

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

const VALID_RUN = '01948f94-1234-7abc-8def-1234567890ab'

describe('BacktestDetailPage', () => {
  it('shows error when runPublicId is not a UUID7', () => {
    render(<BacktestDetailPage runPublicId='not-a-uuid' />)
    expect(screen.getByText(/Invalid run id/i)).toBeDefined()
    expect(screen.getByText(/not-a-uuid/)).toBeDefined()
  })
  it('renders the run id and progress bar for a valid UUID7', () => {
    render(<BacktestDetailPage runPublicId={VALID_RUN} />)
    expect(screen.getByText('Backtest run')).toBeDefined()
    expect(screen.getByText(VALID_RUN)).toBeDefined()
    expect(screen.getByTestId('bt-progress-fill')).toBeDefined()
  })
})
