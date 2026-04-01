import React, { useState } from 'react'
import clsx from 'clsx'
import { LoadingSpinner } from '../../components/ui'

export interface FeedHealth {
  status: 'healthy' | 'warning' | 'error'
  lag_ms: number
  heartbeat_age_ms: number
  healthy: boolean
}
export interface HealthStatus {
  status: 'healthy' | 'warning' | 'error'
  lag_ms: number
  timestamp: number
  seq?: number
  feed_health?: Record<string, FeedHealth>
  inputs?: string[]
  outputs?: string[]
}

const FeedPublisherEntry: React.FC<Readonly<{ feedKey: string; feed: FeedHealth }>> = ({
  feedKey,
  feed,
}) => {
  const isHealthy = feed.healthy
  const isFresh = feed.heartbeat_age_ms < 5000

  return (
    <div className='bg-dark-700 rounded p-2 space-y-1'>
      <div className='flex items-center justify-between'>
        <div className='flex items-center space-x-2'>
          <div
            className={clsx('w-2 h-2 rounded-full', {
              'bg-accent-500': isHealthy && isFresh,
              'bg-warning-500': isHealthy && !isFresh,
              'bg-loss-500': !isHealthy,
            })}
          />
          <span className='text-xs font-medium text-alpine-900'>{feedKey}</span>
        </div>
        <span
          className={clsx('text-xs', {
            'text-accent-400': feed.status === 'healthy',
            'text-warning-400': feed.status === 'warning',
            'text-loss-400': feed.status === 'error',
          })}
        >
          {feed.status}
        </span>
      </div>
      <div className='grid grid-cols-2 gap-2 text-xs text-muted-500'>
        <div>Feed lag: {feed.lag_ms}ms</div>
        <div>HB age: {Math.round(feed.heartbeat_age_ms / 1000)}s</div>
      </div>
    </div>
  )
}

const HEALTH_COLOR: Record<HealthStatus['status'], string> = {
  healthy: 'bg-accent-500',
  warning: 'bg-warning-500',
  error: 'bg-loss-500',
}

const HEALTH_LABEL: Record<HealthStatus['status'], string> = {
  healthy: 'Healthy - receiving fresh data',
  warning: 'Warning - data is stale',
  error: 'Error - no recent data',
}

const resolveHealthColor = (health: HealthStatus | undefined): string =>
  health ? HEALTH_COLOR[health.status] : 'bg-muted-400'

const resolveHealthLabel = (health: HealthStatus | undefined): string =>
  health ? HEALTH_LABEL[health.status] : 'Unknown - no heartbeat data'

const resolveDisplayName = (name: string): string =>
  name
    .replace(/^strategy_/, '')
    .split('_')
    .map(part => part.toUpperCase())
    .join(' ')

const resolveStatusKey = (
  isStarting: boolean,
  isRunning: boolean
): 'starting' | 'running' | 'stopped' => {
  if (isStarting) return 'starting'
  if (isRunning) return 'running'

  return 'stopped'
}

const resolveStatusText = (
  isStarting: boolean,
  isStopping: boolean,
  isRunning: boolean
): string => {
  if (isStarting) return 'starting'
  if (isStopping) return 'stopping'
  if (isRunning) return 'running'

  return 'stopped'
}

interface StrategyCardProps {
  name: string
  running: boolean
  autoStartEnabled: boolean
  mode: 'thread' | 'process'
  health?: HealthStatus
  onStart?: () => void
  onStop?: () => void
  isStarting?: boolean
  isStopping?: boolean
  readOnly?: boolean
}

export const StrategyCard: React.FC<Readonly<StrategyCardProps>> = React.memo(
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
    readOnly = false,
  }) => {
    const [expanded, setExpanded] = useState(false)
    const isRunning = running || isStarting
    const showStopButton = running || isStopping
    const healthColor = resolveHealthColor(health)
    const healthLabel = resolveHealthLabel(health)
    const statusColor = {
      running: 'text-accent-400 bg-accent-400/10',
      stopped: 'text-muted-400 bg-muted-400/10',
      starting: 'text-info-400 bg-info-400/10',
    }[resolveStatusKey(isStarting, isRunning)]
    const statusText = resolveStatusText(isStarting, isStopping, isRunning)
    const showLagBadge = health && health.lag_ms > 2000
    const displayName = resolveDisplayName(name)

    return (
      <article
        className='bg-alpine-50 border border-dark-600 rounded-2xl p-6 space-y-4'
        aria-label={`Strategy: ${displayName}`}
      >
        {}
        <div className='flex items-start justify-between'>
          <div className='flex items-center space-x-3'>
            {}
            <output
              className={clsx('w-3 h-3 rounded-full block', healthColor)}
              title={healthLabel}
              aria-label={healthLabel}
            />
            <div>
              <h3 className='text-lg font-semibold text-alpine-900'>{displayName}</h3>
              <p className='text-sm text-muted-600 mt-1'>Mode: {mode}</p>
              <p className='text-xs text-muted-500 mt-0.5'>
                Autostart: {autoStartEnabled ? 'enabled' : 'disabled'}
              </p>
            </div>
          </div>
          <div className='flex items-center space-x-2'>
            <output
              className={clsx('px-2 py-1 rounded-md text-xs font-medium', statusColor)}
              aria-label={`Status: ${statusText}`}
            >
              {statusText}
            </output>
            {showLagBadge && (
              <output
                className='px-2 py-1 rounded-md text-xs font-medium text-warning-400 bg-warning-400/10'
                aria-label={`Data lag: ${Math.round(health.lag_ms / 1000)} seconds`}
              >
                lag: {Math.round(health.lag_ms / 1000)}s
              </output>
            )}
          </div>
        </div>
        {}
        {health && isRunning && (
          <div className='space-y-3'>
            {}
            <div className='grid grid-cols-3 gap-3 text-xs'>
              <div className='bg-dark-700 rounded p-2'>
                <div className='text-muted-500 mb-1'>Status</div>
                <div
                  className={clsx('font-medium', {
                    'text-accent-400': health.status === 'healthy',
                    'text-warning-400': health.status === 'warning',
                    'text-loss-400': health.status === 'error',
                  })}
                >
                  {health.status.toUpperCase()}
                </div>
              </div>
              <div className='bg-dark-700 rounded p-2'>
                <div className='text-muted-500 mb-1'>Data Lag</div>
                <div className='text-alpine-900 font-medium'>{health.lag_ms}ms</div>
              </div>
              <div className='bg-dark-700 rounded p-2'>
                <div className='text-muted-500 mb-1'>Heartbeat</div>
                <div className='text-alpine-900 font-medium'>#{health.seq || '?'}</div>
              </div>
            </div>
            {}
            <button
              onClick={() => setExpanded(!expanded)}
              className='w-full text-xs text-muted-500 hover:text-alpine-900 transition-colors flex items-center justify-center space-x-1'
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
              <div className='space-y-3 pt-2 border-t border-dark-600'>
                {}
                {(health.inputs || health.outputs) && (
                  <div className='space-y-2'>
                    {health.inputs && health.inputs.length > 0 && (
                      <div>
                        <div className='text-xs font-medium text-muted-500 mb-1'>
                          Inputs ({health.inputs.length})
                        </div>
                        <div className='space-y-1'>
                          {health.inputs.map(input => (
                            <div
                              key={input}
                              className='text-xs text-muted-600 bg-dark-700 rounded px-2 py-1 font-mono'
                            >
                              {input}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    {health.outputs && health.outputs.length > 0 && (
                      <div>
                        <div className='text-xs font-medium text-muted-500 mb-1'>
                          Outputs ({health.outputs.length})
                        </div>
                        <div className='flex flex-wrap gap-1'>
                          {health.outputs.map(output => (
                            <span
                              key={output}
                              className='text-xs text-info-400 bg-info-400/10 rounded px-2 py-1'
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
                    <div className='text-xs font-medium text-muted-500 mb-2'>
                      Feed Publishers ({Object.keys(health.feed_health).length})
                    </div>
                    <div className='space-y-2'>
                      {Object.entries(health.feed_health).map(([feedKey, feed]) => (
                        <FeedPublisherEntry key={feedKey} feedKey={feedKey} feed={feed} />
                      ))}
                    </div>
                  </div>
                )}
                {}
                <div className='text-xs text-muted-400 text-center pt-2 border-t border-dark-600'>
                  Last update: {new Date(health.timestamp).toLocaleTimeString()}
                </div>
              </div>
            )}
          </div>
        )}
        {!health && isRunning && (
          <div className='text-xs text-muted-400 text-center py-2'>Waiting for heartbeat...</div>
        )}
        {}
        {(onStart || onStop) && (
          <div className='flex space-x-2 pt-2 border-t border-dark-600'>
            {showStopButton ? (
              <button
                onClick={onStop}
                disabled={isStopping || isStarting || readOnly}
                aria-label={`Stop ${displayName} strategy`}
                className={clsx(
                  'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                  isStopping || isStarting || readOnly
                    ? 'bg-loss-400/20 text-loss-600 cursor-not-allowed'
                    : 'bg-loss-600 text-white hover:bg-loss-700 focus:outline-none focus:ring-2 focus:ring-loss-500'
                )}
              >
                {isStopping ? (
                  <>
                    <LoadingSpinner size='sm' className='inline-block mr-2' />
                    Stopping...
                  </>
                ) : (
                  'Stop'
                )}
              </button>
            ) : (
              <button
                onClick={onStart}
                disabled={isStarting || isStopping || readOnly}
                aria-label={`Start ${displayName} strategy`}
                className={clsx(
                  'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                  isStarting || isStopping || readOnly
                    ? 'bg-accent-400/20 text-accent-300 cursor-not-allowed'
                    : 'bg-accent-600 text-white hover:bg-accent-700 focus:outline-none focus:ring-2 focus:ring-accent-500'
                )}
              >
                {isStarting ? (
                  <>
                    <LoadingSpinner size='sm' className='inline-block mr-2' />
                    Starting...
                  </>
                ) : (
                  'Start'
                )}
              </button>
            )}
          </div>
        )}
      </article>
    )
  }
)
