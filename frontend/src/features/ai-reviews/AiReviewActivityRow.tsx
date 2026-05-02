import React from 'react'
import { AlertTriangle, Inbox, ShieldCheck } from 'lucide-react'
import type { AiReviewActivityFrame } from '../../stores/wsDispatcher'

/**
 * Single-row renderer for the WS-driven AI review activity stream.
 *
 * The frame's discriminator (`type`) drives the visual variant; the
 * three external WS frame types are ``ai_review.request`` (a CONSULT
 * was dispatched to the delegate), ``ai_review.decision_ack`` (the
 * delegate's decision was committed and broadcast), and
 * ``ai_review.caps_violation`` (an AI-approved trade tripped a
 * TradingCaps gate).
 *
 * The component is intentionally exhaustive on the discriminator so
 * adding a fourth frame type later becomes a TS compile error rather
 * than a silent no-op.
 */
export function AiReviewActivityRow({
  frame,
}: Readonly<{ frame: AiReviewActivityFrame }>): React.ReactElement {
  switch (frame.type) {
    case 'ai_review.request':
      return (
        <RowShell
          icon={<Inbox className='w-4 h-4 text-brand-600' />}
          label='Request'
          tone='neutral'
          timestamp={frame.timestamp}
          reviewPublicId={frame.review_public_id}
        >
          <span className='text-muted-700'>
            Selected delegate{' '}
            <span className='font-mono text-xs'>{frame.selected_delegate_public_id}</span> ·
            instrument <span className='font-mono text-xs'>{frame.instrument_public_id}</span> ·
            deadline {new Date(frame.deadline).toLocaleTimeString()}
          </span>
        </RowShell>
      )
    case 'ai_review.decision_ack':
      return (
        <RowShell
          icon={<ShieldCheck className='w-4 h-4 text-gain-600' />}
          label='Decision'
          tone={frame.decision === 'approve' ? 'positive' : 'negative'}
          timestamp={frame.timestamp}
          reviewPublicId={frame.review_public_id}
        >
          <span className='text-muted-700'>
            <span className='font-semibold'>{frame.decision}</span> by{' '}
            <span className='font-mono text-xs'>{frame.responding_delegate_public_id}</span> →{' '}
            status <span className='font-semibold'>{frame.new_status}</span> · mode{' '}
            <span className='font-mono text-xs'>{frame.resolution_mode}</span>
          </span>
        </RowShell>
      )
    case 'ai_review.caps_violation':
      return (
        <RowShell
          icon={<AlertTriangle className='w-4 h-4 text-warn-600' />}
          label='Caps violation'
          tone='warning'
          timestamp={frame.timestamp}
          reviewPublicId={frame.review_public_id}
        >
          <span className='text-muted-700'>
            <span className='font-semibold'>{frame.cap_type}</span>: attempted{' '}
            <span className='font-mono'>{frame.attempted}</span> vs limit{' '}
            <span className='font-mono'>{frame.limit}</span> on instrument{' '}
            <span className='font-mono text-xs'>{frame.instrument_public_id}</span>
          </span>
        </RowShell>
      )
    default:
      return assertUnreachable(frame)
  }
}

function RowShell({
  icon,
  label,
  tone,
  timestamp,
  reviewPublicId,
  children,
}: Readonly<{
  icon: React.ReactNode
  label: string
  tone: 'neutral' | 'positive' | 'negative' | 'warning'
  timestamp: string
  reviewPublicId: string
  children: React.ReactNode
}>): React.ReactElement {
  return (
    <li
      data-testid={`ai-review-activity-row-${reviewPublicId}`}
      data-frame-tone={tone}
      className='flex items-start gap-3 px-4 py-3 border-b border-dark-700 last:border-b-0'
    >
      <div className='shrink-0 pt-0.5'>{icon}</div>
      <div className='flex-1 text-sm space-y-1'>
        <div className='flex items-center justify-between'>
          <span className='font-semibold text-muted-900'>{label}</span>
          <span className='text-xs text-muted-500'>{new Date(timestamp).toLocaleTimeString()}</span>
        </div>
        <div>{children}</div>
        <div className='font-mono text-[11px] text-muted-500'>review {reviewPublicId}</div>
      </div>
    </li>
  )
}

function assertUnreachable(value: never): never {
  throw new Error(`Unhandled AI review activity frame: ${JSON.stringify(value)}`)
}
