import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

import { SystemMetricsCard } from './SystemMetricsCard'

import type { SystemMetricsResponse } from '../../types/api'

vi.mock('../../hooks/queries', () => ({
  useSystemMetrics: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const sampleSnapshot: SystemMetricsResponse = {
  type: 'system_metrics_response',
  sequence_id: 1,
  public_id: 'env-1',
  timestamp: '2026-05-02T17:00:00Z',
  session_id: 's',
  payload: {
    type: 'system_metrics',
    sequence_id: 2,
    public_id: 'p-1',
    timestamp: '2026-05-02T17:00:00Z',
    session_id: 's',
    bus_time: '2026-05-02T17:00:00Z',
    process: {
      pid: 12345,
      uptime_seconds: 3600,
      status: 'running',
      num_threads: 12,
      num_fds: 84,
      num_connections: 5,
    },
    cpu: {
      process_percent: 4.7,
      user_time_seconds: 12.3,
      system_time_seconds: 1.1,
      cgroup_quota_microseconds: null,
      cgroup_throttled_count: 3,
    },
    memory: {
      rss_bytes: 134_217_728,
      rss_peak_bytes: 200_000_000,
      vms_bytes: 500_000_000,
      python_traced_bytes: null,
      native_bytes: null,
      cgroup_limit_bytes: null,
      cgroup_current_bytes: null,
      saturation_pct: 0.42,
    },
    asyncio: { active_tasks: 7, pending_tasks: 2 },
    gc: {
      collections_gen0: 100,
      collections_gen1: 10,
      collections_gen2: 1,
      uncollectable: 0,
      current_objects: 12_345,
    },
    limits: {
      rlimit_nproc: 4096,
      rlimit_nofile: 65536,
      rlimit_as_bytes: -1,
    },
    saturation: {
      threads_pct: 0.15,
      fds_pct: null,
    },
    db_internal: {
      aiosqlite_live_connections: 4,
      pool_size: 20,
      pool_checked_out: 6,
    },
    tracemalloc_active: false,
    cgroup_version: 'v2',
  },
}

describe('SystemMetricsCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders the card title without metrics during loading', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('Process Health')).toBeInTheDocument()
    expect(screen.getByText(/Loading process metrics/)).toBeInTheDocument()
  })

  it('renders an error fallback when the query fails', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('snapshotter not yet running'),
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(
      screen.getByText(/Process metrics unavailable: snapshotter not yet running/)
    ).toBeInTheDocument()
  })

  it('renders the metrics grid when data is available', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('PID')).toBeInTheDocument()
    expect(screen.getByText('12345')).toBeInTheDocument()
    expect(screen.getByText('Status: running')).toBeInTheDocument()
    expect(screen.getByText('128.0 MB')).toBeInTheDocument()
    expect(screen.getByText('1.0h')).toBeInTheDocument()
    expect(screen.getByText('4.7%')).toBeInTheDocument()
    expect(screen.getByText('Throttled 3×')).toBeInTheDocument()
    expect(screen.getByText('7 active')).toBeInTheDocument()
    expect(screen.getByText('+ 2 pending')).toBeInTheDocument()
    expect(screen.getByText('6 / 20')).toBeInTheDocument()
    expect(screen.getByText('Live conns: 4')).toBeInTheDocument()
    expect(screen.getByText('100 / 10 / 1')).toBeInTheDocument()
    expect(screen.getByText('Idle')).toBeInTheDocument()
  })

  it('renders the raw JSON debug view when expanded', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    const summary = screen.getByText('Raw JSON')

    expect(summary).toBeInTheDocument()
    const pre = summary.parentElement?.querySelector('pre')

    expect(pre?.textContent).toContain('"pid": 12345')
  })

  it('handles cold-start tracemalloc-active state', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, tracemalloc_active: true } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('Active')).toBeInTheDocument()
  })

  it('renders multi-day uptime in days', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          process: { ...sampleSnapshot.payload.process, uptime_seconds: 5 * 86400 },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('5.0d')).toBeInTheDocument()
  })

  it('renders memory >= 1 GiB in GB units', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          memory: {
            ...sampleSnapshot.payload.memory,
            rss_bytes: 2 * 1024 * 1024 * 1024,
          },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText(/2\.00 GB/)).toBeInTheDocument()
  })

  it('renders relative age in different time ranges based on bus_time', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')
    const recent = new Date(Date.now() - 30 * 1000).toISOString()

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, bus_time: recent } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText(/\ds ago/)).toBeInTheDocument()
  })

  it('renders relative age for hour-old snapshots', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')
    const oneHourAgo = new Date(Date.now() - 3600 * 1000).toISOString()

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, bus_time: oneHourAgo } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText(/\d\.\dh ago/)).toBeInTheDocument()
  })

  it('shows the just-now badge for snapshots that bus-time as the current instant', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')
    const future = new Date(Date.now() + 1_000).toISOString()

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, bus_time: future } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('just now')).toBeInTheDocument()
  })

  it('falls back to a generic error message for non-Error throwables', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: { reason: 'unknown' } as unknown as Error,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(
      screen.getByText(/Process metrics unavailable: Failed to load process health metrics/)
    ).toBeInTheDocument()
  })

  it('toggles the raw JSON details when summary is clicked', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    const { rerender } = renderWithProviders(<SystemMetricsCard />)
    const details = screen.getByText('Raw JSON').parentElement as HTMLDetailsElement

    expect(details.open).toBe(false)
    details.open = true
    details.dispatchEvent(new Event('toggle'))
    rerender(<SystemMetricsCard />)
  })

  it('renders uptime in seconds for very young processes', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          process: { ...sampleSnapshot.payload.process, uptime_seconds: 30 },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('30s')).toBeInTheDocument()
  })

  it('renders uptime in minutes between one minute and one hour', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          process: { ...sampleSnapshot.payload.process, uptime_seconds: 300 },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('5.0min')).toBeInTheDocument()
  })

  it('renders relative age in minutes between one minute and one hour', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString()

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: { ...sampleSnapshot, payload: { ...sampleSnapshot.payload, bus_time: fiveMinutesAgo } },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText(/\d\.\dmin ago/)).toBeInTheDocument()
  })

  it('renders em-dash when threads saturation is null', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          saturation: { threads_pct: null, fds_pct: null },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getAllByText('—').length).toBeGreaterThan(0)
  })

  it('shows em-dash placeholders when DB pool sizes are null', async () => {
    const { useSystemMetrics } = await import('../../hooks/queries')

    vi.mocked(useSystemMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: {
          ...sampleSnapshot.payload,
          db_internal: { aiosqlite_live_connections: 1, pool_size: null, pool_checked_out: null },
          memory: { ...sampleSnapshot.payload.memory, saturation_pct: null },
          cpu: { ...sampleSnapshot.payload.cpu, cgroup_throttled_count: 0 },
        },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useSystemMetrics>)
    renderWithProviders(<SystemMetricsCard />)
    expect(screen.getByText('—')).toBeInTheDocument()
    expect(screen.queryByText(/Saturation —/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Throttled 0×/)).not.toBeInTheDocument()
  })
})
