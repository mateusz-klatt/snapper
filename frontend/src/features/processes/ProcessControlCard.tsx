import React from 'react'
import clsx from 'clsx'
import type { HeartbeatData } from '../../hooks/useHeartbeats'

interface ProcessListItem {
  id: string
  name: string
  status: 'running' | 'stopped' | 'error'
  onStop?: () => void
}
interface ProcessControlCardProps {
  title: string
  description: string
  status: 'running' | 'stopped' | 'error'
  statusBadge?: string
  lastHeartbeat?: string
  details?: Record<string, unknown>
  onStart: () => void
  onStop: () => void
  isStarting?: boolean
  isStopping?: boolean
  showList?: boolean
  listItems?: ProcessListItem[]
  heartbeatData?: Record<string, HeartbeatData>
  heartbeatLabel?: string
}

export const ProcessControlCard: React.FC<Readonly<ProcessControlCardProps>> = ({
  title,
  description,
  status,
  statusBadge,
  lastHeartbeat,
  details,
  onStart,
  onStop,
  isStarting = false,
  isStopping = false,
  showList = false,
  listItems = [],
  heartbeatData,
  heartbeatLabel = 'Components',
}) => {
  const isRunning = status === 'running'
  const statusColor = {
    running: 'text-accent-400 bg-accent-400/10',
    stopped: 'text-muted-400 bg-muted-400/10',
    error: 'text-loss-400 bg-loss-400/10',
  }[status]

  return (
    <div className='bg-alpine-50 border border-dark-600 rounded-2xl p-6 space-y-4'>
      {}
      <div className='flex items-start justify-between'>
        <div>
          <h3 className='text-lg font-semibold text-alpine-900'>{title}</h3>
          <p className='text-sm text-muted-600 mt-1'>{description}</p>
        </div>
        <div className='flex items-center space-x-2'>
          <span className={clsx('px-2 py-1 rounded-md text-xs font-medium', statusColor)}>
            {status}
          </span>
          {statusBadge && (
            <span className='px-2 py-1 rounded-md text-xs font-medium text-info-400 bg-info-400/10'>
              {statusBadge}
            </span>
          )}
        </div>
      </div>
      {}
      {(lastHeartbeat || details) && (
        <div className='space-y-1 text-xs text-muted-600'>
          {lastHeartbeat && (
            <div>Last heartbeat: {new Date(lastHeartbeat).toLocaleTimeString()}</div>
          )}
          {details &&
            Object.entries(details).map(([key, value]) => (
              <div key={key}>
                {key}:{' '}
                {typeof value === 'string' ||
                typeof value === 'number' ||
                typeof value === 'boolean'
                  ? String(value)
                  : JSON.stringify(value)}
              </div>
            ))}
        </div>
      )}
      {}
      {showList && listItems.length > 0 && (
        <div className='space-y-2'>
          <div className='text-sm font-medium text-muted-600'>Active processes:</div>
          <div className='space-y-1'>
            {listItems.map(item => (
              <div key={item.id} className='flex items-center justify-between py-1'>
                <div className='flex items-center space-x-2'>
                  <span
                    className={clsx(
                      'w-2 h-2 rounded-full',
                      item.status === 'running' ? 'bg-accent-400' : 'bg-muted-400'
                    )}
                  />
                  <span className='text-sm text-muted-700'>{item.name}</span>
                </div>
                {item.status === 'running' && item.onStop && (
                  <button
                    onClick={item.onStop}
                    className='text-xs text-loss-400 hover:text-loss-600 px-2 py-1 rounded-sm transition-colors'
                  >
                    Stop
                  </button>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
      {}
      <div className='flex items-start gap-3 pt-2 border-t border-dark-600'>
        {}
        <div className='flex space-x-2 flex-1'>
          {isRunning ? (
            <button
              onClick={onStop}
              disabled={isStopping}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStopping
                  ? 'bg-loss-400/20 text-loss-600 cursor-not-allowed'
                  : 'bg-loss-600 text-white hover:bg-loss-700'
              )}
            >
              {isStopping ? (
                <>
                  <div className='w-4 h-4 border-2 border-loss-300 border-t-transparent rounded-full animate-spin inline-block mr-2' />
                  Stopping...
                </>
              ) : (
                'Stop'
              )}
            </button>
          ) : (
            <button
              onClick={onStart}
              disabled={isStarting}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStarting
                  ? 'bg-accent-400/20 text-accent-300 cursor-not-allowed'
                  : 'bg-accent-600 text-white hover:bg-accent-700'
              )}
            >
              {isStarting ? (
                <>
                  <div className='w-4 h-4 border-2 border-accent-300 border-t-transparent rounded-full animate-spin inline-block mr-2' />
                  Starting...
                </>
              ) : (
                'Start'
              )}
            </button>
          )}
          {}
          {isRunning && (
            <button
              onClick={() => {
                onStop()
                setTimeout(onStart, 1000)
              }}
              className='px-4 py-2 rounded-md text-sm font-medium bg-info-600 text-white hover:bg-info-700 transition-colors'
            >
              Restart
            </button>
          )}
        </div>
        {}
        {heartbeatData && Object.keys(heartbeatData).length > 0 && (
          <div className='flex flex-col gap-1'>
            <div className='text-xs font-medium text-muted-600'>{heartbeatLabel}:</div>
            <div className='flex flex-wrap gap-1'>
              {Object.entries(heartbeatData).map(([key, data]) => {
                const isUnknown = data.status === 'unknown'
                const isHealthy = data.healthy && !isUnknown

                return (
                  <div
                    key={key}
                    className={clsx(
                      'px-2 py-1 rounded text-xs whitespace-nowrap',
                      isUnknown && 'bg-muted-500/10 text-muted-400',
                      !isUnknown && isHealthy && 'bg-accent-400/10 text-accent-400',
                      !isUnknown && !isHealthy && 'bg-loss-400/10 text-loss-400'
                    )}
                  >
                    <span className='font-medium capitalize'>{key}</span>
                    {!isUnknown && (
                      <>
                        <span className='opacity-70 ml-1'>{data.status}</span>
                        {data.lag_ms !== undefined && (
                          <span className='opacity-70 ml-1'>({data.lag_ms}ms)</span>
                        )}
                      </>
                    )}
                    {isUnknown && <span className='opacity-70 ml-1'>waiting...</span>}
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
