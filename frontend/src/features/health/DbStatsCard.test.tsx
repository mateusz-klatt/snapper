import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

import { DbStatsCard } from './DbStatsCard'

import type { DbStatsResponse } from '../../types/api'

vi.mock('../../hooks/queries', () => ({
  useDbStats: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const sampleSnapshot: DbStatsResponse = {
  type: 'db_stats_response',
  sequence_id: 1,
  public_id: 'env-1',
  timestamp: '2026-05-02T17:00:00Z',
  session_id: 's',
  payload: {
    type: 'db_stats',
    sequence_id: 2,
    public_id: 'p-1',
    timestamp: '2026-05-02T17:00:00Z',
    session_id: 's',
    snapshot_started_at: '2026-05-02T17:00:00Z',
    snapshot_completed_at: '2026-05-02T17:00:01Z',
    interval_seconds: 60,
    tables: [
      {
        table: 'orders',
        table_kind: 'state',
        total: 12_345,
        current: 1_000,
        closed: 11_345,
        archivable: 5_000,
        is_stale: false,
        last_sampled_at: '2026-05-02T17:00:01Z',
      },
      {
        table: 'signal_event',
        table_kind: 'event',
        total: 78_910,
        current: null,
        closed: null,
        archivable: null,
        is_stale: true,
        last_sampled_at: '2026-05-02T16:55:00Z',
      },
    ],
  },
}

describe('DbStatsCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders the card title without data during loading', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText('Database Statistics')).toBeInTheDocument()
    expect(screen.getByText(/Loading database stats/)).toBeInTheDocument()
  })

  it('renders an error fallback when the query fails', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('snapshotter not yet running'),
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(
      screen.getByText(/Database stats unavailable: snapshotter not yet running/)
    ).toBeInTheDocument()
  })

  it('renders the table when data is available', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText('orders')).toBeInTheDocument()
    expect(screen.getByText('signal_event')).toBeInTheDocument()
    expect(screen.getByText('12,345')).toBeInTheDocument()
    expect(screen.getByText('78,910')).toBeInTheDocument()
    expect(screen.getByText('stale')).toBeInTheDocument()
    expect(screen.getByText(/Sampler interval: 60s · 2 tables/)).toBeInTheDocument()
  })

  it('renders em-dashes for null SCD2 columns on event tables', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(3)
  })

  it('renders relative age for the snapshot timestamp', async () => {
    const { useDbStats } = await import('../../hooks/queries')
    const recent = new Date(Date.now() - 30 * 1000).toISOString()

    vi.mocked(useDbStats).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, snapshot_completed_at: recent },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText(/sampled \d+s ago/)).toBeInTheDocument()
  })

  it('renders the just-now badge for snapshots taken instantly', async () => {
    const { useDbStats } = await import('../../hooks/queries')
    const future = new Date(Date.now() + 1_000).toISOString()

    vi.mocked(useDbStats).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, snapshot_completed_at: future },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText(/sampled just now/)).toBeInTheDocument()
  })

  it('renders relative age in minutes between one minute and one hour', async () => {
    const { useDbStats } = await import('../../hooks/queries')
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString()

    vi.mocked(useDbStats).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, snapshot_completed_at: fiveMinutesAgo },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText(/sampled \d\.\dmin ago/)).toBeInTheDocument()
  })

  it('renders relative age in hours for older snapshots', async () => {
    const { useDbStats } = await import('../../hooks/queries')
    const twoHoursAgo = new Date(Date.now() - 2 * 3600 * 1000).toISOString()

    vi.mocked(useDbStats).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, snapshot_completed_at: twoHoursAgo },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.getByText(/sampled \d\.\dh ago/)).toBeInTheDocument()
  })

  it('falls back to a generic error message for non-Error throwables', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: { reason: 'unknown' } as unknown as Error,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(
      screen.getByText(/Database stats unavailable: Failed to load database statistics/)
    ).toBeInTheDocument()
  })

  it('does not show the stale badge for fresh rows', async () => {
    const { useDbStats } = await import('../../hooks/queries')

    vi.mocked(useDbStats).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          tables: [
            {
              table: 'positions',
              table_kind: 'state',
              total: 100,
              current: 50,
              closed: 50,
              archivable: 0,
              is_stale: false,
              last_sampled_at: '2026-05-02T17:00:01Z',
            },
          ],
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useDbStats>)
    renderWithProviders(<DbStatsCard />)
    expect(screen.queryByText('stale')).not.toBeInTheDocument()
  })
})
