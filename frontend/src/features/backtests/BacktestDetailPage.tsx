import React from 'react'
import clsx from 'clsx'
import { useBacktest } from '../../hooks/queries'
import { useBacktestProgressSubscription } from './hooks/useBacktestProgressSubscription'
import { BacktestProgressBar } from './BacktestProgressBar'
import { CompareLauncher } from './CompareLauncher'
import { isUuid7 } from '../../lib/ids'

interface Props {
  runPublicId: string
}

const STATUS_COLOR: Record<string, string> = {
  completed: 'text-gain-400',
  failed: 'text-loss-400',
  cancelled: 'text-muted-400',
  running: 'text-brand-400',
  pending: 'text-muted-400',
}

const formatDate = (iso: string): string => new Date(iso).toLocaleDateString()

/**
 * Backtest detail page.
 *
 * Fetches the full run row so CompareLauncher can read currentRun.status
 * + currentRun.config_hash for terminal-status gating + auto-pair gating
 * without an extra round trip.
 */
export const BacktestDetailPage: React.FC<Props> = ({ runPublicId }) => {
  const validRun = isUuid7(runPublicId)
  const snapshot = useBacktestProgressSubscription(validRun ? runPublicId : null)
  const { data, isLoading, error } = useBacktest(validRun ? runPublicId : undefined)

  if (!validRun) {
    return (
      <div className='p-4 text-sm text-red-600'>
        Invalid run id — must be a UUID7 (got &quot;{runPublicId}&quot;).
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className='space-y-4 p-4'>
        <h2 className='text-lg font-semibold'>Backtest run</h2>
        <code className='text-xs opacity-70'>{runPublicId}</code>
        <BacktestProgressBar snapshot={snapshot} />
      </div>
    )
  }

  if (error || !data?.payload) {
    return (
      <div className='space-y-3 p-4'>
        <h2 className='text-lg font-semibold'>Backtest run</h2>
        <div className='text-sm text-red-600'>
          Failed to load run: {error instanceof Error ? error.message : 'not found'}
        </div>
        <a href='#backtests' className='text-sm text-brand-400 hover:underline'>
          ← Back to backtests
        </a>
      </div>
    )
  }

  const run = data.payload

  return (
    <div className='space-y-4 p-4'>
      <div className='flex items-center justify-between'>
        <h2 className='text-lg font-semibold'>Backtest run</h2>
        <a href='#backtests' className='text-sm text-brand-400 hover:underline'>
          ← Back
        </a>
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm md:grid-cols-4'>
        <div>
          <div className='text-muted-500'>Strategy</div>
          <div className='text-alpine-900'>{run.strategy_name}</div>
        </div>
        <div>
          <div className='text-muted-500'>Instrument</div>
          <div className='font-mono text-alpine-900'>{run.instrument_public_id}</div>
        </div>
        <div>
          <div className='text-muted-500'>Status</div>
          <div className={clsx('font-medium', STATUS_COLOR[run.status] ?? 'text-muted-400')}>
            {run.status}
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Period</div>
          <div className='font-mono text-alpine-900'>
            {formatDate(run.start_date)} — {formatDate(run.end_date)}
          </div>
        </div>
        {run.target_execution_exchange && (
          <div className='col-span-2 md:col-span-4'>
            <div className='text-muted-500'>Cross-asset attribution</div>
            <div className='font-mono text-alpine-900'>
              {run.exchange} feed → {run.target_execution_exchange} fills
            </div>
          </div>
        )}
        {run.config_hash && (
          <div className='col-span-2 md:col-span-4'>
            <div className='text-muted-500'>Config hash</div>
            <code className='font-mono text-xs text-muted-400'>{run.config_hash}</code>
          </div>
        )}
      </div>
      <code className='text-xs opacity-70'>{runPublicId}</code>
      <BacktestProgressBar snapshot={snapshot} />
      <CompareLauncher currentRun={run} />
    </div>
  )
}
