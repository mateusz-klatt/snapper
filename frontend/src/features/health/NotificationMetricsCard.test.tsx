import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

import { NotificationMetricsCard } from './NotificationMetricsCard'

import type { NotificationMetricsResponse } from '../../types/api'

vi.mock('../../hooks/queries', () => ({
  useNotificationMetrics: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const sampleSnapshot: NotificationMetricsResponse = {
  type: 'notification_metrics_response',
  sequence_id: 1,
  public_id: 'env-n',
  timestamp: '2026-05-02T17:00:00Z',
  session_id: 's',
  payload: {
    type: 'notification_metrics',
    sequence_id: 2,
    public_id: 'p-n',
    timestamp: '2026-05-02T17:00:00Z',
    session_id: 's',
    delivery_success_total: 1234,
    delivery_failed_total: 0,
    delivery_410_unregistered_total: 5,
    delivery_cancelled_scope_total: 12,
    outbox_queued_depth: 0,
  },
}

describe('NotificationMetricsCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders the card title without metrics during loading', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    expect(screen.getByText('Notification Sidecar')).toBeInTheDocument()
    expect(screen.getByText(/Loading notification metrics/)).toBeInTheDocument()
  })

  it('renders an error fallback when the query fails', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('repository unavailable'),
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    expect(
      screen.getByText(/Notification metrics unavailable: repository unavailable/)
    ).toBeInTheDocument()
  })

  it('renders the counters grid when data is available', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: sampleSnapshot,
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    expect(screen.getByText('Sent')).toBeInTheDocument()
    expect(screen.getByText('1,234')).toBeInTheDocument()
    expect(screen.getByText('Failed')).toBeInTheDocument()
    expect(screen.getByText('Unregistered')).toBeInTheDocument()
    expect(screen.getByText('Cancelled (scope)')).toBeInTheDocument()
    expect(screen.getByText('Outbox depth')).toBeInTheDocument()
    expect(screen.getByText('5')).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
  })

  it('applies loss tone when failures exceed zero', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, delivery_failed_total: 7 },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    const failedValue = screen.getByText('7')

    expect(failedValue.className).toContain('text-loss-700')
  })

  it('applies warning tone when outbox depth exceeds zero', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: {
        ...sampleSnapshot,
        payload: { ...sampleSnapshot.payload, outbox_queued_depth: 42 },
      },
      isLoading: false,
      error: null,
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    const depthValue = screen.getByText('42')

    expect(depthValue.className).toContain('text-warning-700')
  })

  it('falls back to a generic error message for non-Error throwables', async () => {
    const { useNotificationMetrics } = await import('../../hooks/queries')

    vi.mocked(useNotificationMetrics).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: { reason: 'unknown' } as unknown as Error,
    } as unknown as ReturnType<typeof useNotificationMetrics>)
    renderWithProviders(<NotificationMetricsCard />)
    expect(
      screen.getByText(/Notification metrics unavailable: Failed to load notification metrics/)
    ).toBeInTheDocument()
  })
})
