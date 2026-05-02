import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

import { RetentionCard } from './RetentionCard'

import type { RetentionRunResponse } from '../../types/api'

vi.mock('../../hooks/queries', () => ({
  useRetentionRun: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const sampleSnapshot: RetentionRunResponse = {
  type: 'retention_run_response',
  sequence_id: 1,
  public_id: 'env-r',
  timestamp: '2026-05-02T17:00:00Z',
  session_id: 's',
  payload: {
    type: 'retention_run',
    sequence_id: 2,
    public_id: 'p-r',
    timestamp: '2026-05-02T17:00:00Z',
    session_id: 's',
    run_started_at: '2026-05-02T17:00:00Z',
    run_completed_at: '2026-05-02T17:00:01Z',
    dry_run: false,
    results: [
      {
        table: 'orders',
        retain_days: 30,
        backlog_lookback_days: 7,
        day_start: '2026-04-01',
        day_end: '2026-04-02',
        archived_rows: 1234,
        purged_rows: 1234,
        files_written: 1,
        error: null,
      },
      {
        table: 'signal_event',
        retain_days: 7,
        backlog_lookback_days: 1,
        day_start: null,
        day_end: null,
        archived_rows: 0,
        purged_rows: 0,
        files_written: 0,
        error: 'permission denied',
      },
    ],
  },
}

describe('RetentionCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders the card title without metrics during loading', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.getByText('Retention Scheduler')).toBeInTheDocument()
    expect(screen.getByText(/Loading retention run/)).toBeInTheDocument()
  })

  it('renders an error fallback when the query fails', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('scheduler not yet run'),
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.getByText(/Retention run unavailable: scheduler not yet run/)).toBeInTheDocument()
  })

  it('renders the policy table when data is available', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.getByText('orders')).toBeInTheDocument()
    expect(screen.getByText('signal_event')).toBeInTheDocument()
    expect(screen.getByText('30d')).toBeInTheDocument()
    expect(screen.getByText('7d')).toBeInTheDocument()
    expect(screen.getByText('2026-04-01 → 2026-04-02')).toBeInTheDocument()
    expect(screen.getAllByText('1,234').length).toBeGreaterThan(0)
    expect(screen.getByText('ok')).toBeInTheDocument()
    expect(screen.getByText('error')).toBeInTheDocument()
  })

  it('shows a dry-run banner when dry_run is true', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, dry_run: true } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.getByText(/Dry-run mode/)).toBeInTheDocument()
  })

  it('does not show a dry-run banner when dry_run is false', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.queryByText(/Dry-run mode/)).not.toBeInTheDocument()
  })

  it('renders em-dash when archived window is null', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(screen.getByText('—')).toBeInTheDocument()
  })

  it('exposes the policy error string as a tooltip', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    const errSpan = screen.getByText('error')

    expect(errSpan.getAttribute('title')).toBe('permission denied')
  })

  it('renders relative age across all four time ranges', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    const cases = [
      { offsetMs: 1_000, regex: /just now/ },
      { offsetMs: -30_000, regex: /\d+s ago/ },
      { offsetMs: -5 * 60_000, regex: /\d\.\dmin ago/ },
      { offsetMs: -2 * 3600_000, regex: /\d\.\dh ago/ },
      { offsetMs: -2 * 86400_000, regex: /\d\.\dd ago/ },
    ]

    for (const { offsetMs, regex } of cases) {
      const ts = new Date(Date.now() + offsetMs).toISOString()

      vi.mocked(useRetentionRun).mockReturnValue({
        data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, run_completed_at: ts } },
        isLoading: false,
        error: null,
      } as unknown as ReturnType<typeof useRetentionRun>)
      const { unmount } = renderWithProviders(<RetentionCard />)

      expect(screen.getByText(regex)).toBeInTheDocument()
      unmount()
    }
  })

  it('falls back to a generic error message for non-Error throwables', async () => {
    const { useRetentionRun } = await import('../../hooks/queries')

    vi.mocked(useRetentionRun).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: { reason: 'unknown' } as unknown as Error,
    } as unknown as ReturnType<typeof useRetentionRun>)
    renderWithProviders(<RetentionCard />)
    expect(
      screen.getByText(/Retention run unavailable: Failed to load retention run/)
    ).toBeInTheDocument()
  })
})
