import React, { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useAuth } from '../../stores/auth'
import { useAppStore } from '../../stores/app'
import { apiClient } from '../../lib/apiClient'
import { SignalCardSkeleton } from '../../components/Skeleton'
import type { TradingSignal } from '../../types/api'
import clsx from 'clsx'
import {
  SIGNAL_STRENGTH_STRONG,
  SIGNAL_STRENGTH_MEDIUM,
  SIGNAL_STRENGTH_WEAK,
} from '../../lib/constants'

const SignalCard: React.FC<{ signal: TradingSignal }> = ({ signal }) => {
  const getSideColor = (side: string) => {
    return side === 'buy' ? 'text-green-400 bg-green-900/20' : 'text-red-400 bg-red-900/20'
  }

  const getStrengthColor = (strength: number) => {
    if (strength >= SIGNAL_STRENGTH_STRONG) return 'text-green-400'
    if (strength >= SIGNAL_STRENGTH_MEDIUM) return 'text-yellow-400'
    if (strength >= SIGNAL_STRENGTH_WEAK) return 'text-orange-400'

    return 'text-red-400'
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
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-4 hover:border-dark-600 transition-colors'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <span className='font-medium text-white'>{signal.instrument}</span>
          <span
            className={clsx(
              'px-2 py-1 text-xs font-medium rounded-full',
              getSideColor(signal.side)
            )}
          >
            {signal.side.toUpperCase()}
          </span>
          {signal.strategy_name && (
            <span className='px-2 py-1 text-xs bg-blue-900/20 text-blue-400 rounded-sm'>
              {signal.strategy_name}
            </span>
          )}
        </div>
        <div className='text-xs text-dark-400'>{formatTime(signal.timestamp)}</div>
      </div>
      <div className='grid grid-cols-3 gap-4 text-sm mb-3'>
        <div>
          <div className='text-dark-400'>Strength</div>
          <div className={clsx('font-medium', getStrengthColor(signal.strength))}>
            {getStrengthLabel(signal.strength)} ({(signal.strength * 100).toFixed(0)}%)
          </div>
        </div>
        <div>
          <div className='text-dark-400'>Price</div>
          <div className='text-white font-mono'>
            {signal.price ? `$${signal.price.toFixed(2)}` : 'N/A'}
          </div>
        </div>
        <div>
          <div className='text-dark-400'>Signal ID</div>
          <div className='text-white text-xs font-mono'>#{signal.id}</div>
        </div>
      </div>
      {signal.reason && (
        <div className='bg-dark-900 rounded-sm p-2 text-xs text-dark-300'>
          <div className='text-dark-400 mb-1'>Reason:</div>
          {signal.reason}
        </div>
      )}
    </div>
  )
}

const SignalStreamIndicator: React.FC<{ isActive: boolean }> = ({ isActive }) => (
  <div className='flex items-center space-x-2 text-sm'>
    <div
      className={clsx(
        'w-2 h-2 rounded-full',
        isActive ? 'bg-green-400 animate-pulse' : 'bg-red-400'
      )}
    ></div>
    <span className={isActive ? 'text-green-400' : 'text-red-400'}>
      {isActive ? 'Live Stream Active' : 'Stream Disconnected'}
    </span>
  </div>
)

export const Signals: React.FC = () => {
  const [strategyFilter, setStrategyFilter] = useState<string>('all')
  const { isAuthenticated } = useAuth()
  const isConnected = useAppStore(state => state.isConnected)
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

  return (
    <div className='p-4 space-y-6'>
      {}
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-bold text-white'>Trading Signals</h2>
        <SignalStreamIndicator isActive={isConnected} />
      </div>
      {}
      <div className='grid grid-cols-4 gap-4'>
        <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
          <div className='text-dark-400 text-sm'>Total Signals</div>
          <div className='text-2xl font-bold text-white'>{totalSignals}</div>
        </div>
        <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
          <div className='text-dark-400 text-sm'>Buy Signals</div>
          <div className='text-2xl font-bold text-green-400'>{buySignals}</div>
        </div>
        <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
          <div className='text-dark-400 text-sm'>Sell Signals</div>
          <div className='text-2xl font-bold text-red-400'>{sellSignals}</div>
        </div>
        <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
          <div className='text-dark-400 text-sm'>Avg Strength</div>
          <div className='text-2xl font-bold text-blue-400'>{(avgStrength * 100).toFixed(0)}%</div>
        </div>
      </div>
      {}
      <div className='flex items-center justify-between'>
        <div className='flex items-center space-x-4'>
          <label htmlFor='strategy-filter' className='text-sm text-dark-400'>
            Filter by strategy:
          </label>
          <select
            id='strategy-filter'
            value={strategyFilter}
            onChange={e => setStrategyFilter(e.target.value)}
            className='px-3 py-1 bg-dark-800 border border-dark-600 rounded-sm text-white text-sm focus:outline-hidden focus:ring-2 focus:ring-blue-500'
          >
            <option value='all'>All Strategies</option>
            {availableStrategies.map(strategy => (
              <option key={strategy} value={strategy}>
                {strategy}
              </option>
            ))}
          </select>
        </div>
        {}
        <div className='flex items-center space-x-4 text-sm'>
          <div className='text-dark-400'>Market Sentiment:</div>
          <div
            className={clsx(
              'px-3 py-1 rounded-full text-xs font-medium',
              buySignals > sellSignals && 'bg-green-900/20 text-green-400',
              sellSignals > buySignals && 'bg-red-900/20 text-red-400',
              buySignals === sellSignals && 'bg-gray-900/20 text-gray-400'
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
      {}
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
          <div className='text-center py-8 text-dark-400'>
            <div className='w-12 h-12 bg-dark-700 rounded-full flex items-center justify-center mx-auto mb-3'>
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
            <p className='text-sm mt-1'>
              {strategyFilter === 'all'
                ? 'Signals from active strategies will appear here'
                : `No signals from ${strategyFilter} strategy`}
            </p>
          </div>
        )}
        {!isLoading && filteredSignals.length > 0 && (
          <div className='space-y-3'>
            <div className='flex items-center justify-between text-sm text-dark-400'>
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
