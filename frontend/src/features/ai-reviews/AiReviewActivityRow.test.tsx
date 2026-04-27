import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { AiReviewActivityRow } from './AiReviewActivityRow'
import type { AiReviewActivityFrame } from '../../stores/wsDispatcher'

function renderRow(frame: AiReviewActivityFrame): ReturnType<typeof render> {
  return render(
    <ul>
      <AiReviewActivityRow frame={frame} />
    </ul>
  )
}

const requestFrame = {
  type: 'ai_review.request',
  sequence_id: 1,
  public_id: 'pub-r1',
  timestamp: '2026-04-27T10:00:00Z',
  session_id: 'sess-1',
  review_public_id: 'rev-req',
  user_public_id: 'user-1',
  strategy_public_id: 'strat-1',
  wallet_public_id: 'wal-1',
  instrument_public_id: 'BTC-USD',
  selected_delegate_public_id: 'del-77',
  deadline: '2026-04-27T10:05:00Z',
  signal_envelope: {},
  instrument_metadata: {},
  dispatch_version: 0,
} as unknown as AiReviewActivityFrame

const ackApproveFrame = {
  type: 'ai_review.decision_ack',
  sequence_id: 2,
  public_id: 'pub-a1',
  timestamp: '2026-04-27T10:01:00Z',
  session_id: 'sess-1',
  review_public_id: 'rev-ack',
  user_public_id: 'user-1',
  strategy_public_id: 'strat-1',
  wallet_public_id: 'wal-1',
  instrument_public_id: 'BTC-USD',
  responding_delegate_public_id: 'del-99',
  decision: 'approve',
  new_status: 'resolved_approved',
  resolution_mode: 'race_to_first',
  rationale: 'looks good',
  dispatch_version: 0,
} as unknown as AiReviewActivityFrame

const ackRejectFrame = {
  ...(ackApproveFrame as unknown as Record<string, unknown>),
  decision: 'reject',
  new_status: 'resolved_rejected',
} as unknown as AiReviewActivityFrame

const capsFrame = {
  type: 'ai_review.caps_violation',
  sequence_id: 3,
  public_id: 'pub-c1',
  timestamp: '2026-04-27T10:02:00Z',
  session_id: 'sess-1',
  review_public_id: 'rev-caps',
  user_public_id: 'user-1',
  strategy_public_id: 'strat-1',
  wallet_public_id: 'wal-1',
  instrument_public_id: 'BTC-USD',
  cap_type: 'max_daily_notional_usd',
  attempted: 50000,
  limit: 10000,
  dispatch_version: 0,
} as unknown as AiReviewActivityFrame

describe('AiReviewActivityRow', () => {
  it('renders a request frame with selected delegate + deadline', () => {
    renderRow(requestFrame)
    expect(screen.getByText('Request')).toBeTruthy()
    expect(screen.getByText(/del-77/)).toBeTruthy()
    expect(screen.getByText(/BTC-USD/)).toBeTruthy()
    const row = screen.getByTestId('ai-review-activity-row-rev-req')

    expect(row.getAttribute('data-frame-tone')).toBe('neutral')
  })

  it('renders an approve decision_ack with positive tone', () => {
    renderRow(ackApproveFrame)
    expect(screen.getByText('Decision')).toBeTruthy()
    expect(screen.getByText('approve')).toBeTruthy()
    const row = screen.getByTestId('ai-review-activity-row-rev-ack')

    expect(row.getAttribute('data-frame-tone')).toBe('positive')
  })

  it('renders a reject decision_ack with negative tone', () => {
    renderRow(ackRejectFrame)
    expect(screen.getByText('reject')).toBeTruthy()
    const row = screen.getByTestId('ai-review-activity-row-rev-ack')

    expect(row.getAttribute('data-frame-tone')).toBe('negative')
  })

  it('renders a caps_violation with warning tone + cap details', () => {
    renderRow(capsFrame)
    expect(screen.getByText('Caps violation')).toBeTruthy()
    expect(screen.getByText('max_daily_notional_usd')).toBeTruthy()
    expect(screen.getByText('50000')).toBeTruthy()
    expect(screen.getByText('10000')).toBeTruthy()
    const row = screen.getByTestId('ai-review-activity-row-rev-caps')

    expect(row.getAttribute('data-frame-tone')).toBe('warning')
  })

  it('throws on an unknown frame discriminator (exhaustive guard)', () => {
    const rogue = {
      type: 'ai_review.something_else',
      review_public_id: 'rev-x',
      dispatch_version: 0,
      timestamp: '2026-04-27T10:00:00Z',
    } as unknown as AiReviewActivityFrame

    expect(() => renderRow(rogue)).toThrow(/Unhandled AI review activity frame/)
  })
})
