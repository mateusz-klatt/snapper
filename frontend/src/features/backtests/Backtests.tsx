import React, { useState } from 'react'
import clsx from 'clsx'
import { Play, RotateCcw, XCircle } from 'lucide-react'
import { useBacktests, useCancelBacktest, useRerunBacktest } from '../../hooks/queries'
import { OrderCardSkeleton } from '../../components/Skeleton'
import { EmptyState } from '../../components/ui'
import type { BacktestRunData } from '../../types/api'

type StatusColor = 'text-gain-400' | 'text-loss-400' | 'text-brand-400' | 'text-muted-400'

const getStatusColor = (status: string): StatusColor => {
  switch (status) {
    case 'completed':
      return 'text-gain-400'
    case 'failed':
      return 'text-loss-400'
    case 'running':
      return 'text-brand-400'
    default:
      return 'text-muted-400'
  }
}

const formatDate = (dateStr: string): string => {
  const d = new Date(dateStr)

  return d.toLocaleDateString()
}

interface BacktestRowProps {
  run: BacktestRunData
  onCancel: (publicId: string) => void
  onRerun: (publicId: string) => void
}

const BacktestRow: React.FC<BacktestRowProps> = ({ run, onCancel, onRerun }) => {
  const canCancel = run.status === 'pending' || run.status === 'running'

  return (
    <div
      className='rounded-2xl border border-dark-600 bg-alpine-50 p-5 transition-colors hover:border-muted-400'
      data-testid={`backtest-${run.public_id}`}
    >
      <div className='mb-3 flex items-center justify-between'>
        <a
          href={`#backtests/${run.public_id}`}
          className='flex cursor-pointer items-center space-x-3 hover:underline'
          data-testid={`open-${run.public_id}`}
        >
          <span className='font-semibold text-alpine-900'>{run.strategy_name}</span>
          <span className='text-sm text-muted-500'>{run.instrument_public_id}</span>
          <span className='text-sm text-muted-500'>
            {run.exchange}
            {run.target_execution_exchange && (
              <span className='ml-1 text-brand-500'>→ {run.target_execution_exchange}</span>
            )}
          </span>
          <span className={clsx('text-sm font-medium', getStatusColor(run.status))}>
            {run.status}
          </span>
        </a>
        <div className='flex items-center gap-2'>
          {canCancel && (
            <button
              type='button'
              onClick={() => onCancel(run.public_id)}
              className='flex items-center gap-1 rounded-lg border border-loss-500 px-2 py-1 text-xs font-medium text-loss-500 transition-colors hover:bg-loss-900/20'
              data-testid={`cancel-${run.public_id}`}
            >
              <XCircle size={12} />
              Cancel
            </button>
          )}
          <button
            type='button'
            onClick={() => onRerun(run.public_id)}
            className='flex items-center gap-1 rounded-lg border border-brand-500 px-2 py-1 text-xs font-medium text-brand-500 transition-colors hover:bg-brand-900/20'
            data-testid={`rerun-${run.public_id}`}
          >
            <RotateCcw size={12} />
            Rerun
          </button>
        </div>
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm md:grid-cols-4'>
        <div>
          <div className='text-muted-500'>Timeframe</div>
          <div className='font-mono text-alpine-900'>{run.timeframe}</div>
        </div>
        <div>
          <div className='text-muted-500'>Period</div>
          <div className='font-mono text-alpine-900'>
            {formatDate(run.start_date)} - {formatDate(run.end_date)}
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Initial Cash</div>
          <div className='font-mono text-alpine-900'>${run.initial_cash.toFixed(2)}</div>
        </div>
        <div>
          <div className='text-muted-500'>ID</div>
          <div className='font-mono text-xs text-muted-400'>{run.public_id.slice(0, 12)}</div>
        </div>
      </div>
      {run.error && <div className='mt-3 text-xs text-loss-400'>Error: {run.error}</div>}
    </div>
  )
}

export const Backtests: React.FC = () => {
  const [statusFilter, setStatusFilter] = useState<string>('')
  const { data, isLoading } = useBacktests(undefined, statusFilter || undefined)
  const cancelMutation = useCancelBacktest()
  const rerunMutation = useRerunBacktest()

  const runs = data?.payload ?? []

  const handleCancel = (publicId: string) => {
    cancelMutation.mutate(publicId)
  }

  const handleRerun = (publicId: string) => {
    rerunMutation.mutate(publicId)
  }

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-semibold text-alpine-900'>Backtests</h2>
        <div className='flex items-center gap-2'>
          <select
            className='rounded-lg border border-dark-600 bg-alpine-50 px-3 py-1 text-sm text-alpine-900'
            value={statusFilter}
            onChange={e => setStatusFilter(e.target.value)}
            data-testid='status-filter'
          >
            <option value=''>All statuses</option>
            <option value='pending'>Pending</option>
            <option value='running'>Running</option>
            <option value='completed'>Completed</option>
            <option value='failed'>Failed</option>
            <option value='cancelled'>Cancelled</option>
          </select>
        </div>
      </div>
      <div className='space-y-4'>
        {isLoading && (
          <div className='space-y-3'>
            <OrderCardSkeleton />
            <OrderCardSkeleton />
          </div>
        )}
        {!isLoading && runs.length === 0 && (
          <EmptyState
            icon={<Play className='h-6 w-6' />}
            title='No backtests'
            message='Run a backtest via the CLI or API to see results here.'
          />
        )}
        {!isLoading && runs.length > 0 && (
          <div className='grid gap-4'>
            {runs.map((run: BacktestRunData) => (
              <BacktestRow
                key={run.public_id}
                run={run}
                onCancel={handleCancel}
                onRerun={handleRerun}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
