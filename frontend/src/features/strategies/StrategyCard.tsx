import React, { useState } from 'react'
import clsx from 'clsx'

interface FeedHealth {
  status: string
  lag_ms: number
  heartbeat_age_ms: number
  healthy: boolean
}
interface HealthStatus {
  status: 'ok' | 'warn' | 'error'
  lag_ms: number
  timestamp: number
  seq?: number
  feed_health?: Record<string, FeedHealth>
  inputs?: string[]
  outputs?: string[]
}
interface StrategyCardProps {
  name: string
  running: boolean
  autoStartEnabled: boolean
  mode: 'thread' | 'process'
  health?: HealthStatus
  onStart: () => void
  onStop: () => void
  isStarting?: boolean
  isStopping?: boolean
}

export const StrategyCard: React.FC<StrategyCardProps> = React.memo(
  ({
    name,
    running,
    autoStartEnabled,
    mode,
    health,
    onStart,
    onStop,
    isStarting = false,
    isStopping = false,
  }) => {
    const [expanded, setExpanded] = useState(false)
    const isRunning = running || isStarting
    const showStopButton = running || isStopping
    const healthColor = health
      ? {
          ok: 'bg-green-500',
          warn: 'bg-orange-500',
          error: 'bg-red-500',
        }[health.status]
      : 'bg-gray-400'
    const healthLabel = health
      ? {
          ok: 'Healthy - receiving fresh data',
          warn: 'Warning - data is stale',
          error: 'Error - no recent data',
        }[health.status]
      : 'Unknown - no heartbeat data'
    const statusColor = {
      running: 'text-green-400 bg-green-400/10',
      stopped: 'text-gray-400 bg-gray-400/10',
      starting: 'text-blue-400 bg-blue-400/10',
    }[isStarting ? 'starting' : isRunning ? 'running' : 'stopped']
    const statusText = isStarting
      ? 'starting'
      : isStopping
        ? 'stopping'
        : isRunning
          ? 'running'
          : 'stopped'
    const showLagBadge = health && health.lag_ms > 2000
    const displayName = name
      .replace(/^strategy_/, '')
      .split('_')
      .map(part => part.toUpperCase())
      .join(' ')

    return (
      <div
        className='bg-dark-800 border border-dark-700 rounded-lg p-6 space-y-4'
        role='article'
        aria-label={`Strategy: ${displayName}`}
      >
        {}
        <div className='flex items-start justify-between'>
          <div className='flex items-center space-x-3'>
            {}
            <div
              className={clsx('w-3 h-3 rounded-full', healthColor)}
              title={healthLabel}
              aria-label={healthLabel}
              role='status'
            />
            <div>
              <h3 className='text-lg font-semibold text-white'>{displayName}</h3>
              <p className='text-sm text-dark-300 mt-1'>Mode: {mode}</p>
              <p className='text-xs text-dark-400 mt-0.5'>
                Autostart: {autoStartEnabled ? 'enabled' : 'disabled'}
              </p>
            </div>
          </div>
          <div className='flex items-center space-x-2'>
            <span
              className={clsx('px-2 py-1 rounded-md text-xs font-medium', statusColor)}
              role='status'
              aria-label={`Status: ${statusText}`}
            >
              {statusText}
            </span>
            {showLagBadge && (
              <span
                className='px-2 py-1 rounded-md text-xs font-medium text-orange-400 bg-orange-400/10'
                role='status'
                aria-label={`Data lag: ${Math.round(health.lag_ms / 1000)} seconds`}
              >
                lag: {Math.round(health.lag_ms / 1000)}s
              </span>
            )}
          </div>
        </div>
        {}
        {health && isRunning && (
          <div className='space-y-3'>
            {}
            <div className='grid grid-cols-3 gap-3 text-xs'>
              <div className='bg-dark-900/50 rounded p-2'>
                <div className='text-dark-400 mb-1'>Status</div>
                <div
                  className={clsx('font-medium', {
                    'text-green-400': health.status === 'ok',
                    'text-orange-400': health.status === 'warn',
                    'text-red-400': health.status === 'error',
                  })}
                >
                  {health.status.toUpperCase()}
                </div>
              </div>
              <div className='bg-dark-900/50 rounded p-2'>
                <div className='text-dark-400 mb-1'>Data Lag</div>
                <div className='text-white font-medium'>{health.lag_ms}ms</div>
              </div>
              <div className='bg-dark-900/50 rounded p-2'>
                <div className='text-dark-400 mb-1'>Heartbeat</div>
                <div className='text-white font-medium'>#{health.seq || '?'}</div>
              </div>
            </div>
            {}
            <button
              onClick={() => setExpanded(!expanded)}
              className='w-full text-xs text-dark-400 hover:text-white transition-colors flex items-center justify-center space-x-1'
            >
              <span>{expanded ? 'Hide Details' : 'Show Details'}</span>
              <svg
                className={clsx('w-4 h-4 transition-transform', { 'rotate-180': expanded })}
                fill='none'
                viewBox='0 0 24 24'
                stroke='currentColor'
              >
                <path
                  strokeLinecap='round'
                  strokeLinejoin='round'
                  strokeWidth={2}
                  d='M19 9l-7 7-7-7'
                />
              </svg>
            </button>
            {expanded && (
              <div className='space-y-3 pt-2 border-t border-dark-700'>
                {}
                {(health.inputs || health.outputs) && (
                  <div className='space-y-2'>
                    {health.inputs && health.inputs.length > 0 && (
                      <div>
                        <div className='text-xs font-medium text-dark-400 mb-1'>
                          Inputs ({health.inputs.length})
                        </div>
                        <div className='space-y-1'>
                          {health.inputs.map(input => (
                            <div
                              key={input}
                              className='text-xs text-dark-300 bg-dark-900/50 rounded px-2 py-1 font-mono'
                            >
                              {input}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    {health.outputs && health.outputs.length > 0 && (
                      <div>
                        <div className='text-xs font-medium text-dark-400 mb-1'>
                          Outputs ({health.outputs.length})
                        </div>
                        <div className='flex flex-wrap gap-1'>
                          {health.outputs.map(output => (
                            <span
                              key={output}
                              className='text-xs text-blue-400 bg-blue-400/10 rounded px-2 py-1'
                            >
                              {output}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
                {}
                {health.feed_health && Object.keys(health.feed_health).length > 0 && (
                  <div>
                    <div className='text-xs font-medium text-dark-400 mb-2'>
                      Feed Publishers ({Object.keys(health.feed_health).length})
                    </div>
                    <div className='space-y-2'>
                      {Object.entries(health.feed_health).map(([feedKey, feed]) => {
                        const isHealthy = feed.healthy
                        const isFresh = feed.heartbeat_age_ms < 5000

                        return (
                          <div key={feedKey} className='bg-dark-900/50 rounded p-2 space-y-1'>
                            <div className='flex items-center justify-between'>
                              <div className='flex items-center space-x-2'>
                                <div
                                  className={clsx('w-2 h-2 rounded-full', {
                                    'bg-green-500': isHealthy && isFresh,
                                    'bg-orange-500': isHealthy && !isFresh,
                                    'bg-red-500': !isHealthy,
                                  })}
                                />
                                <span className='text-xs font-medium text-white'>{feedKey}</span>
                              </div>
                              <span
                                className={clsx('text-xs', {
                                  'text-green-400': feed.status === 'ok',
                                  'text-orange-400': feed.status === 'warn',
                                  'text-red-400': feed.status === 'error',
                                })}
                              >
                                {feed.status}
                              </span>
                            </div>
                            <div className='grid grid-cols-2 gap-2 text-xs text-dark-400'>
                              <div>Feed lag: {feed.lag_ms}ms</div>
                              <div>HB age: {Math.round(feed.heartbeat_age_ms / 1000)}s</div>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}
                {}
                <div className='text-xs text-dark-500 text-center pt-2 border-t border-dark-700'>
                  Last update: {new Date(health.timestamp).toLocaleTimeString()}
                </div>
              </div>
            )}
          </div>
        )}
        {!health && isRunning && (
          <div className='text-xs text-dark-500 text-center py-2'>Waiting for heartbeat...</div>
        )}
        {}
        <div className='flex space-x-2 pt-2 border-t border-dark-700'>
          {!showStopButton ? (
            <button
              onClick={onStart}
              disabled={isStarting || isStopping}
              aria-label={`Start ${displayName} strategy`}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStarting || isStopping
                  ? 'bg-green-400/20 text-green-300 cursor-not-allowed'
                  : 'bg-green-600 text-white hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500'
              )}
            >
              {isStarting ? (
                <>
                  <div className='w-4 h-4 border-2 border-green-300 border-t-transparent rounded-full animate-spin inline-block mr-2' />
                  Starting...
                </>
              ) : (
                'Start'
              )}
            </button>
          ) : (
            <button
              onClick={onStop}
              disabled={isStopping || isStarting}
              aria-label={`Stop ${displayName} strategy`}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStopping || isStarting
                  ? 'bg-red-400/20 text-red-300 cursor-not-allowed'
                  : 'bg-red-600 text-white hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500'
              )}
            >
              {isStopping ? (
                <>
                  <div className='w-4 h-4 border-2 border-red-300 border-t-transparent rounded-full animate-spin inline-block mr-2' />
                  Stopping...
                </>
              ) : (
                'Stop'
              )}
            </button>
          )}
        </div>
      </div>
    )
  }
)
