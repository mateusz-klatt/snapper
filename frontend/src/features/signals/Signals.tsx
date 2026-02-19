import React, { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Download } from 'lucide-react'
import { useAuth } from '../../stores/auth'
import { apiClient } from '../../lib/apiClient'
import { SignalCardSkeleton } from '../../components/Skeleton'
import { ThemeSelect } from '../../components/ThemeSelect'
import { exportToCSV } from '../../lib/csvExport'
import type { TradingSignal } from '../../types/api'
import clsx from 'clsx'
import {
  SIGNAL_STRENGTH_STRONG,
  SIGNAL_STRENGTH_MEDIUM,
  SIGNAL_STRENGTH_WEAK,
} from '../../lib/constants'

const SignalCard: React.FC<{ signal: TradingSignal }> = ({ signal }) => {
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

  const formatTime = (timestamp: string) => {
    const date = new Date(timestamp)
    const now = new Date()
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
          {signal.strategy_name && (
            <span className='rounded-md bg-info-50 px-2 py-1 text-xs text-info-600'>
              {signal.strategy_name}
            </span>
          )}
        </div>
        <div className='text-xs text-muted-500'>{formatTime(signal.timestamp)}</div>
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
          <div className='text-muted-500'>Signal ID</div>
          <div className='text-xs font-mono text-alpine-900'>#{signal.id}</div>
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
  const { isAuthenticated } = useAuth()
  const { data: signals = [], isLoading } = useQuery({
    queryKey: ['signals', strategyFilter],
    queryFn: async () => {
      return await apiClient.getSignals(strategyFilter === 'all' ? undefined : strategyFilter, 50)
    },
    refetchInterval: false,
    enabled: isAuthenticated,
  })
  const availableStrategies = Array.from(
    new Set(signals.map((signal: TradingSignal) => signal.strategy_name).filter(Boolean))
  )
  const filteredSignals = signals.filter(
    (signal: TradingSignal) => strategyFilter === 'all' || signal.strategy_name === strategyFilter
  )
  const totalSignals = filteredSignals.length
  const buySignals = filteredSignals.filter((s: TradingSignal) => s.side === 'buy').length
  const sellSignals = filteredSignals.filter((s: TradingSignal) => s.side === 'sell').length
  const avgStrength =
    totalSignals > 0
      ? filteredSignals.reduce((sum: number, s: TradingSignal) => sum + s.strength, 0) /
        totalSignals
      : 0

  const handleExportSignals = () => {
    const headers = [
      'ID',
      'Instrument',
      'Side',
      'Strength',
      'Price',
      'Strategy',
      'Reason',
      'Timestamp',
    ]
    const rows = filteredSignals.map((s: TradingSignal) => [
      String(s.id),
      s.instrument,
      s.side,
      (s.strength * 100).toFixed(0) + '%',
      s.price ? s.price.toFixed(2) : '',
      s.strategy_name ?? '',
      s.reason ?? '',
      s.timestamp,
    ])

    exportToCSV('signals.csv', headers, rows)
  }

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-semibold text-alpine-900'>Trading Signals</h2>
        <button
          onClick={handleExportSignals}
          disabled={filteredSignals.length === 0}
          className='flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border border-dark-600 bg-alpine-50 hover:bg-muted-200 disabled:opacity-50 disabled:cursor-not-allowed text-alpine-900 rounded-lg transition-colors'
        >
          <Download size={14} />
          Export CSV
        </button>
      </div>
      <div className='grid grid-cols-2 gap-4 sm:grid-cols-4'>
        <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-4'>
          <div className='text-sm text-muted-500'>Total Signals</div>
          <div className='text-2xl font-semibold text-alpine-900'>{totalSignals}</div>
        </div>
        <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-4'>
          <div className='text-sm text-muted-500'>Buy Signals</div>
          <div className='text-2xl font-semibold text-gain-600'>{buySignals}</div>
        </div>
        <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-4'>
          <div className='text-sm text-muted-500'>Sell Signals</div>
          <div className='text-2xl font-semibold text-loss-600'>{sellSignals}</div>
        </div>
        <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-4'>
          <div className='text-sm text-muted-500'>Avg Strength</div>
          <div className='text-2xl font-semibold text-info-600'>
            {(avgStrength * 100).toFixed(0)}%
          </div>
        </div>
      </div>
      <div className='flex flex-col gap-3 rounded-xl border border-dark-600 bg-alpine-50 px-4 py-3 sm:flex-row sm:items-center sm:justify-between'>
        <div className='flex items-center space-x-4'>
          <label htmlFor='strategy-filter' className='text-sm text-muted-600'>
            Filter by strategy:
          </label>
          <ThemeSelect
            id='strategy-filter'
            value={strategyFilter}
            onChange={setStrategyFilter}
            options={[
              { value: 'all', label: 'All Strategies' },
              ...availableStrategies.map(strategy => ({ value: strategy, label: strategy })),
            ]}
            className='max-w-56'
          />
        </div>
        {}
        <div className='flex items-center space-x-4 text-sm'>
          <div className='text-muted-600'>Market Sentiment:</div>
          <div
            className={clsx(
              'rounded-full px-3 py-1 text-xs font-medium',
              buySignals > sellSignals && 'bg-gain-50 text-gain-600',
              sellSignals > buySignals && 'bg-loss-50 text-loss-600',
              buySignals === sellSignals && 'bg-dark-700 text-muted-600'
            )}
          >
            {(() => {
              if (buySignals > sellSignals) return 'Bullish'
              if (sellSignals > buySignals) return 'Bearish'

              return 'Neutral'
            })()}
          </div>
        </div>
      </div>
      <div className='space-y-4'>
        {isLoading && (
          <div className='space-y-3'>
            <SignalCardSkeleton />
            <SignalCardSkeleton />
            <SignalCardSkeleton />
            <SignalCardSkeleton />
          </div>
        )}
        {!isLoading && filteredSignals.length === 0 && (
          <div className='py-8 text-center text-muted-500'>
            <div className='mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-dark-700'>
              <svg className='w-6 h-6' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                <path
                  strokeLinecap='round'
                  strokeLinejoin='round'
                  strokeWidth={2}
                  d='M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
                />
              </svg>
            </div>
            <p>No signals found</p>
            <p className='mt-1 text-sm'>
              {strategyFilter === 'all'
                ? 'Signals from active strategies will appear here'
                : `No signals from ${strategyFilter} strategy`}
            </p>
          </div>
        )}
        {!isLoading && filteredSignals.length > 0 && (
          <div className='space-y-3'>
            <div className='flex items-center justify-between text-sm text-muted-500'>
              <span>Showing {filteredSignals.length} signals</span>
              <span>Latest signals first</span>
            </div>
            {filteredSignals.map((signal: TradingSignal) => (
              <SignalCard key={signal.id} signal={signal} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
