import React, { useMemo } from 'react'

import { useNotificationMetrics } from '../../hooks/queries'
import { formatNumber } from '../../lib/utils'

interface CounterCellProps {
  readonly label: string
  readonly value: number
  readonly description?: string
  readonly tone?: 'neutral' | 'warning' | 'loss'
}

const TONE_CLASS_BY_TONE = {
  loss: 'text-loss-700',
  neutral: 'text-alpine-900',
  warning: 'text-warning-700',
} satisfies Record<NonNullable<CounterCellProps['tone']>, string>

const CounterCell: React.FC<CounterCellProps> = ({
  label,
  value,
  description,
  tone = 'neutral',
}) => {
  const toneClass = TONE_CLASS_BY_TONE[tone]

  return (
    <div className='flex flex-col rounded-md border border-dark-600 bg-alpine-50 p-3'>
      <span className='text-xs uppercase tracking-wide text-muted-600'>{label}</span>
      <span className={`mt-1 text-2xl font-semibold ${toneClass}`}>{formatNumber(value)}</span>
      {description !== undefined && (
        <span className='mt-1 text-xs text-muted-600'>{description}</span>
      )}
    </div>
  )
}

export const NotificationMetricsCard: React.FC = () => {
  const { data, isLoading, error } = useNotificationMetrics()

  const errorMessage = useMemo(() => {
    if (!error) return null
    if (error instanceof Error) return error.message

    return 'Failed to load notification metrics.'
  }, [error])

  return (
    <section className='space-y-3 rounded-lg border border-dark-600 bg-white p-4'>
      <header>
        <h3 className='text-lg font-medium text-alpine-900'>Notification Sidecar</h3>
      </header>
      {isLoading && <p className='text-sm text-muted-600'>Loading notification metrics…</p>}
      {errorMessage !== null && (
        <p className='text-sm text-loss-600'>Notification metrics unavailable: {errorMessage}</p>
      )}
      {data?.payload && (
        <div className='grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5'>
          <CounterCell
            label='Sent'
            value={data.payload.delivery_success_total}
            description='Terminal: sent'
          />
          <CounterCell
            label='Failed'
            value={data.payload.delivery_failed_total}
            description='Retry budget exhausted'
            tone={data.payload.delivery_failed_total > 0 ? 'loss' : 'neutral'}
          />
          <CounterCell
            label='Unregistered'
            value={data.payload.delivery_410_unregistered_total}
            description='APNs 410 (BadDeviceToken)'
          />
          <CounterCell
            label='Cancelled (scope)'
            value={data.payload.delivery_cancelled_scope_total}
            description='Scope revoke / user deactivate'
          />
          <CounterCell
            label='Outbox depth'
            value={data.payload.outbox_queued_depth}
            description='Queued, awaiting retry'
            tone={data.payload.outbox_queued_depth > 0 ? 'warning' : 'neutral'}
          />
        </div>
      )}
    </section>
  )
}
