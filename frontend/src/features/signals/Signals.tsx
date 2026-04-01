import React, { useState } from 'react'
import { Download } from 'lucide-react'
import { useAppStore } from '../../stores/app'
import { SignalCardSkeleton } from '../../components/Skeleton'
import { ThemeSelect } from '../../components/ThemeSelect'
import { exportToCSV } from '../../lib/csvExport'
import { EmptyState } from '../../components/ui'
import { useSignals } from '../../hooks/queries'
import type { Signal } from '../../types/entities'
import clsx from 'clsx'
import {
  SIGNAL_STRENGTH_STRONG,
  SIGNAL_STRENGTH_MEDIUM,
  SIGNAL_STRENGTH_WEAK,
} from '../../lib/constants'

const SignalCard: React.FC<{ signal: Signal }> = ({ signal }) => {
  const asOf = useAppStore(s => s.asOf)

  const getSideColor = (side: string) => {
    return side === 'buy' ? 'text-gain-400 bg-gain-900/20' : 'text-loss-400 bg-loss-900/20'
  }

  const getStrengthColor = (strength: number) => {
    if (strength >= SIGNAL_STRENGTH_STRONG) return 'text-gain-400'
    if (strength >= SIGNAL_STRENGTH_MEDIUM) return 'text-warning-400'
    if (strength >= SIGNAL_STRENGTH_WEAK) return 'text-warning-400'

    return 'text-loss-400'
  }

  const getStrengthLabel = (strength: number) => {
    if (strength >= SIGNAL_STRENGTH_STRONG) return 'Strong'
    if (strength >= SIGNAL_STRENGTH_MEDIUM) return 'Medium'
    if (strength >= SIGNAL_STRENGTH_WEAK) return 'Weak'

    return 'Very Weak'
  }

  const formatTime = (date: Date) => {
    const now = asOf ? new Date(asOf) : new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMins = Math.floor(diffMs / 60000)

    if (diffMins < 1) return 'Just now'
    if (diffMins < 60) return `${diffMins}m ago`
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h ago`

    return date.toLocaleDateString()
  }

  return (
    <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-5 transition-colors hover:border-muted-400'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <span className='font-semibold text-alpine-900'>{signal.instrument}</span>
          <span
            className={clsx(
              'px-2 py-1 text-xs font-medium rounded-full',
              getSideColor(signal.side)
            )}
          >
            {signal.side.toUpperCase()}
          </span>
          {signal.strategyName && (
            <span className='rounded-md bg-info-50 px-2 py-1 text-xs text-info-600'>
              {signal.strategyName}
            </span>
          )}
        </div>
        <div className='text-xs text-muted-500'>{formatTime(signal.firedAt)}</div>
      </div>
      <div className='grid grid-cols-3 gap-4 text-sm mb-3'>
        <div>
          <div className='text-muted-500'>Strength</div>
          <div className={clsx('font-medium', getStrengthColor(signal.strength))}>
            {getStrengthLabel(signal.strength)} ({(signal.strength * 100).toFixed(0)}%)
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Price</div>
          <div className='font-mono text-alpine-900'>
            {signal.price ? `$${signal.price.toFixed(2)}` : 'N/A'}
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Exchange</div>
          <div className='text-xs font-mono text-alpine-900'>{signal.exchange}</div>
        </div>
      </div>
      {signal.reason && (
        <div className='rounded-lg border border-dark-600 bg-dark-700 p-2 text-xs text-muted-700'>
          <div className='mb-1 text-muted-500'>Reason:</div>
          {signal.reason}
        </div>
      )}
    </div>
  )
}

export const Signals: React.FC = () => {
  const [strategyFilter, setStrategyFilter] = useState<string>('all')
  const { data: signals = [], isLoading } = useSignals(
    strategyFilter === 'all' ? undefined : strategyFilter,
    50
  )
  const availableStrategies = Array.from(
    new Set(signals.map(signal => signal.strategyName).filter(Boolean))
  )
  const filteredSignals = signals.filter(
    signal => strategyFilter === 'all' || signal.strategyName === strategyFilter
  )
  const totalSignals = filteredSignals.length
  const buySignals = filteredSignals.filter(s => s.side === 'buy').length
  const sellSignals = filteredSignals.filter(s => s.side === 'sell').length
  const avgStrength =
    totalSignals > 0 ? filteredSignals.reduce((sum, s) => sum + s.strength, 0) / totalSignals : 0

  const handleExport = () => {
    const headers = [
      'instrument',
      'exchange',
      'side',
      'strength',
      'strategy',
      'price',
      'reason',
      'fired_at',
    ]
    const rows = filteredSignals.map(s => [
      s.instrument,
      s.exchange,
      s.side,
      String(s.strength),
      s.strategyName ?? '',
      s.price == null ? '' : String(s.price),
      s.reason,
      s.firedAt.toISOString(),
    ])

    exportToCSV('signals.csv', headers, rows)
  }

  if (isLoading) {
    return (
      <div className='space-y-6'>
        <h2 className='text-xl font-semibold text-alpine-900'>Signals</h2>
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
          {Array.from({ length: 4 }, (_, i) => (
            <SignalCardSkeleton key={`signal-skeleton-${i}`} />
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className='space-y-6'>
      <div className='flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4'>
        <h2 className='text-xl font-semibold text-alpine-900'>Signals</h2>
        <div className='flex items-center gap-3'>
          <ThemeSelect
            value={strategyFilter}
            onChange={setStrategyFilter}
            options={[
              { value: 'all', label: 'All Strategies' },
              ...availableStrategies.map(s => ({
                value: String(s),
                label: String(s),
              })),
            ]}
          />
          <button
            onClick={handleExport}
            className='flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg border border-dark-600 text-muted-700 hover:bg-dark-700 transition-colors'
            disabled={filteredSignals.length === 0}
          >
            <Download className='w-4 h-4' />
            Export
          </button>
        </div>
      </div>
      {}
      <div className='grid grid-cols-2 sm:grid-cols-4 gap-3'>
        <div className='rounded-xl border border-dark-600 bg-alpine-50 p-4 text-center'>
          <div className='text-2xl font-bold text-alpine-900'>{totalSignals}</div>
          <div className='text-xs text-muted-500'>Total</div>
        </div>
        <div className='rounded-xl border border-dark-600 bg-alpine-50 p-4 text-center'>
          <div className='text-2xl font-bold text-gain-400'>{buySignals}</div>
          <div className='text-xs text-muted-500'>Buy</div>
        </div>
        <div className='rounded-xl border border-dark-600 bg-alpine-50 p-4 text-center'>
          <div className='text-2xl font-bold text-loss-400'>{sellSignals}</div>
          <div className='text-xs text-muted-500'>Sell</div>
        </div>
        <div className='rounded-xl border border-dark-600 bg-alpine-50 p-4 text-center'>
          <div className='text-2xl font-bold text-alpine-900'>
            {(avgStrength * 100).toFixed(0)}%
          </div>
          <div className='text-xs text-muted-500'>Avg Strength</div>
        </div>
      </div>
      {}
      {filteredSignals.length === 0 ? (
        <EmptyState
          icon={<span className='text-4xl'>📡</span>}
          title='No signals found'
          message='No trading signals match your current filters.'
        />
      ) : (
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
          {filteredSignals.map((signal, index) => (
            <SignalCard key={signal.publicId ?? `signal-${index}`} signal={signal} />
          ))}
        </div>
      )}
    </div>
  )
}
