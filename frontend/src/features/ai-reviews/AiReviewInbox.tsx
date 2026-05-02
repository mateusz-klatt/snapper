import React, { useMemo } from 'react'
import { Activity, Inbox as InboxIcon, Loader2, MailOpen, ShieldAlert } from 'lucide-react'
import { EmptyState } from '../../components/ui'
import { useAuth } from '../../stores/auth'
import { useAiReviewActivity, usePendingAiReviews } from '../../hooks/queries'
import { AiReviewActivityRow } from './AiReviewActivityRow'

const ACTIVITY_DISPLAY_LIMIT = 50

/**
 * Delegate-facing AI Review inbox.
 *
 * Two surfaces stitched together:
 *
 *   1. **Pending list** — REST poll of ``GET /api/ai-reviews/pending``
 *      (5s + refetch-on-focus); the snapshot of every CONSULT review
 *      currently waiting on this delegate's decision.
 *   2. **Activity stream** — WS-driven view of the last
 *      ``ACTIVITY_DISPLAY_LIMIT`` frames maintained by
 *      :class:`WSDispatcher`. Reverses chronological order so the
 *      most recent event is at the top.
 *
 * Non-delegate roles are routed here by the resource gate but the
 * REST returns 422 + the WS scope filter drops every ai_reviews
 * frame, so the page renders a role-aware notice instead.
 */
export function AiReviewInbox(): React.ReactElement {
  const { user } = useAuth()
  const isDelegate = user?.role === 'ai_delegate'
  const pendingQuery = usePendingAiReviews()
  const activity = useAiReviewActivity()
  const recent = useMemo(() => [...activity].slice(-ACTIVITY_DISPLAY_LIMIT).reverse(), [activity])

  if (!isDelegate) {
    return (
      <div className='p-6'>
        <div className='flex items-start gap-3 p-4 rounded-lg bg-warn-50 border border-warn-200 text-warn-800'>
          <ShieldAlert className='w-5 h-5 shrink-0 mt-0.5' />
          <div>
            <h2 className='font-semibold mb-1'>Reserved for AI delegates</h2>
            <p className='text-sm'>
              This inbox surfaces CONSULT reviews routed to an AI delegate principal. Sign in with
              an AI delegate account to see pending reviews and the live activity stream. Your
              current role is <span className='font-semibold'>{user?.role ?? 'unknown'}</span>.
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className='p-6 space-y-6'>
      <header>
        <h1 className='text-2xl font-bold'>AI Review Inbox</h1>
        <p className='text-sm text-muted-500'>
          Pending CONSULT reviews routed to this delegate plus the live event stream.
        </p>
      </header>

      <PendingReviewsSection
        loading={pendingQuery.isLoading}
        error={pendingQuery.error}
        items={pendingQuery.data?.items ?? []}
      />

      <ActivitySection frames={recent} totalBuffered={activity.length} />
    </div>
  )
}

function PendingReviewsSection({
  loading,
  error,
  items,
}: Readonly<{
  loading: boolean
  error: Error | null
  items: ReadonlyArray<{
    review_public_id: string
    selected_delegate_public_id: string
    wallet_public_id: string
    dispatch_version: number
    status: string
    deadline: string | Date
    fanout_after: string | Date
  }>
}>): React.ReactElement {
  let content: React.ReactNode

  if (loading) {
    content = (
      <div className='flex items-center gap-2 text-muted-500 text-sm'>
        <Loader2 className='w-4 h-4 animate-spin' />
        Loading pending reviews…
      </div>
    )
  } else if (error !== null) {
    content = (
      <div className='p-3 rounded-lg bg-loss-50 border border-loss-200 text-loss-800 text-sm'>
        Failed to load pending reviews: {error.message}
      </div>
    )
  } else if (items.length === 0) {
    content = (
      <EmptyState
        icon={<MailOpen className='w-5 h-5' />}
        title='No pending reviews'
        message='New CONSULT requests routed to this delegate will appear here.'
      />
    )
  } else {
    content = (
      <div className='overflow-x-auto border border-dark-600 rounded-lg'>
        <table className='min-w-full text-sm'>
          <thead className='bg-dark-700 text-muted-700 text-left'>
            <tr>
              <th className='px-4 py-2 font-semibold'>Review</th>
              <th className='px-4 py-2 font-semibold'>Wallet</th>
              <th className='px-4 py-2 font-semibold'>Status</th>
              <th className='px-4 py-2 font-semibold'>Dispatch</th>
              <th className='px-4 py-2 font-semibold'>Deadline</th>
            </tr>
          </thead>
          <tbody className='divide-y divide-dark-600'>
            {items.map(item => (
              <tr
                key={item.review_public_id}
                data-testid={`pending-review-row-${item.review_public_id}`}
              >
                <td className='px-4 py-2 font-mono text-xs'>{item.review_public_id}</td>
                <td className='px-4 py-2 font-mono text-xs'>{item.wallet_public_id}</td>
                <td className='px-4 py-2'>{item.status}</td>
                <td className='px-4 py-2 font-mono text-xs'>v{item.dispatch_version}</td>
                <td className='px-4 py-2 text-muted-600'>
                  {new Date(item.deadline).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  return (
    <section>
      <div className='flex items-center gap-2 mb-3'>
        <InboxIcon className='w-4 h-4 text-brand-600' />
        <h2 className='text-sm font-semibold text-muted-800'>Pending reviews</h2>
      </div>
      {content}
    </section>
  )
}

function ActivitySection({
  frames,
  totalBuffered,
}: Readonly<{
  frames: ReadonlyArray<Parameters<typeof AiReviewActivityRow>[0]['frame']>
  totalBuffered: number
}>): React.ReactElement {
  let summary = 'No frames received'

  if (totalBuffered > 0) {
    summary = `Showing ${frames.length} of ${totalBuffered} buffered`
  }

  let content: React.ReactNode

  if (frames.length === 0) {
    content = (
      <EmptyState
        icon={<Activity className='w-5 h-5' />}
        title='No activity yet'
        message='AI review request, decision, and caps-violation frames will stream in here.'
      />
    )
  } else {
    content = (
      <ul
        data-testid='ai-review-activity-list'
        className='border border-dark-600 rounded-lg bg-alpine-50 dark:bg-dark-800'
      >
        {frames.map(frame => (
          <AiReviewActivityRow
            key={`${frame.type}|${frame.review_public_id}|${frame.dispatch_version}`}
            frame={frame}
          />
        ))}
      </ul>
    )
  }

  return (
    <section>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center gap-2'>
          <Activity className='w-4 h-4 text-brand-600' />
          <h2 className='text-sm font-semibold text-muted-800'>Recent activity</h2>
        </div>
        <span className='text-xs text-muted-500'>{summary}</span>
      </div>
      {content}
    </section>
  )
}
