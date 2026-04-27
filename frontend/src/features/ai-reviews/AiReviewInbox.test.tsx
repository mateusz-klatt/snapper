import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { AiReviewInbox } from './AiReviewInbox'
import type { AiReviewActivityFrame } from '../../stores/wsDispatcher'

const mockUseAuth = vi.fn()
const mockUsePendingAiReviews = vi.fn()
const mockUseAiReviewActivity = vi.fn()

vi.mock('../../stores/auth', () => ({
  useAuth: () => mockUseAuth(),
}))

vi.mock('../../hooks/queries', () => ({
  usePendingAiReviews: () => mockUsePendingAiReviews(),
  useAiReviewActivity: () => mockUseAiReviewActivity(),
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const requestFrame = {
  type: 'ai_review.request',
  sequence_id: 1,
  public_id: 'pub-r1',
  timestamp: '2026-04-27T10:00:00Z',
  session_id: 'sess-1',
  review_public_id: 'rev-1',
  user_public_id: 'u-1',
  strategy_public_id: 's-1',
  wallet_public_id: 'w-1',
  instrument_public_id: 'i-1',
  selected_delegate_public_id: 'd-1',
  deadline: '2026-04-27T10:05:00Z',
  signal_envelope: {},
  instrument_metadata: {},
  dispatch_version: 0,
} as unknown as AiReviewActivityFrame

describe('AiReviewInbox', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUsePendingAiReviews.mockReturnValue({
      isLoading: false,
      error: null,
      data: { items: [], count: 0 },
    })
    mockUseAiReviewActivity.mockReturnValue([])
  })

  it('renders a role-aware notice for non-delegate roles', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'operator' } })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText('Reserved for AI delegates')).toBeTruthy()
    expect(screen.getByText(/operator/)).toBeTruthy()
  })

  it('falls back to "unknown" role label when no user is authenticated', () => {
    mockUseAuth.mockReturnValue({ user: null })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText('Reserved for AI delegates')).toBeTruthy()
    expect(screen.getByText(/unknown/)).toBeTruthy()
  })

  it('renders loading state while pending reviews query is in flight', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    mockUsePendingAiReviews.mockReturnValue({
      isLoading: true,
      error: null,
      data: undefined,
    })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText(/Loading pending reviews/)).toBeTruthy()
  })

  it('renders error banner when the pending reviews query fails', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    mockUsePendingAiReviews.mockReturnValue({
      isLoading: false,
      error: new Error('upstream 503'),
      data: undefined,
    })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText(/upstream 503/)).toBeTruthy()
  })

  it('renders empty states when there are no pending reviews and no activity', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText('No pending reviews')).toBeTruthy()
    expect(screen.getByText('No activity yet')).toBeTruthy()
  })

  it('renders a row per pending review when the snapshot is non-empty', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    mockUsePendingAiReviews.mockReturnValue({
      isLoading: false,
      error: null,
      data: {
        items: [
          {
            review_public_id: 'rev-pending-1',
            selected_delegate_public_id: 'del-1',
            wallet_public_id: 'wal-1',
            dispatch_version: 0,
            status: 'pending',
            deadline: '2026-04-27T10:05:00Z',
            fanout_after: '2026-04-27T10:00:00Z',
          },
          {
            review_public_id: 'rev-pending-2',
            selected_delegate_public_id: 'del-1',
            wallet_public_id: 'wal-2',
            dispatch_version: 1,
            status: 'pending',
            deadline: '2026-04-27T10:06:00Z',
            fanout_after: '2026-04-27T10:00:30Z',
          },
        ],
        count: 2,
      },
    })
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByTestId('pending-review-row-rev-pending-1')).toBeTruthy()
    expect(screen.getByTestId('pending-review-row-rev-pending-2')).toBeTruthy()
  })

  it('renders the activity stream and reports buffered count', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    const frames: AiReviewActivityFrame[] = Array.from({ length: 3 }, (_, i) => ({
      ...(requestFrame as unknown as Record<string, unknown>),
      review_public_id: `rev-${i}`,
      dispatch_version: i,
    })) as unknown as AiReviewActivityFrame[]

    mockUseAiReviewActivity.mockReturnValue(frames)
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText('Showing 3 of 3 buffered')).toBeTruthy()
    expect(screen.getByTestId('ai-review-activity-list')).toBeTruthy()
    expect(screen.getByTestId('ai-review-activity-row-rev-0')).toBeTruthy()
    expect(screen.getByTestId('ai-review-activity-row-rev-2')).toBeTruthy()
  })

  it('caps the activity stream rendering at 50 frames even if buffer holds more', () => {
    mockUseAuth.mockReturnValue({ user: { role: 'ai_delegate' } })
    const frames: AiReviewActivityFrame[] = Array.from({ length: 60 }, (_, i) => ({
      ...(requestFrame as unknown as Record<string, unknown>),
      review_public_id: `rev-${i}`,
      dispatch_version: i,
    })) as unknown as AiReviewActivityFrame[]

    mockUseAiReviewActivity.mockReturnValue(frames)
    renderWithProviders(<AiReviewInbox />)
    expect(screen.getByText('Showing 50 of 60 buffered')).toBeTruthy()
    expect(screen.queryByTestId('ai-review-activity-row-rev-0')).toBeNull()
    expect(screen.getByTestId('ai-review-activity-row-rev-59')).toBeTruthy()
  })
})
