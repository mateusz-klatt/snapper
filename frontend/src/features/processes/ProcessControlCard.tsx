import React from 'react'
import clsx from 'clsx'

interface ProcessListItem {
  id: string
  name: string
  status: 'running' | 'stopped' | 'error'
  onStop?: () => void
}
interface HeartbeatData {
  status: string
  lag_ms?: number
  timestamp: number
  healthy: boolean
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

export const ProcessControlCard: React.FC<ProcessControlCardProps> = ({
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
    running: 'text-green-400 bg-green-400/10',
    stopped: 'text-gray-400 bg-gray-400/10',
    error: 'text-red-400 bg-red-400/10',
  }[status]

  return (
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-6 space-y-4'>
      {}
      <div className='flex items-start justify-between'>
        <div>
          <h3 className='text-lg font-semibold text-white'>{title}</h3>
          <p className='text-sm text-dark-300 mt-1'>{description}</p>
        </div>
        <div className='flex items-center space-x-2'>
          <span className={clsx('px-2 py-1 rounded-md text-xs font-medium', statusColor)}>
            {status}
          </span>
          {statusBadge && (
            <span className='px-2 py-1 rounded-md text-xs font-medium text-blue-400 bg-blue-400/10'>
              {statusBadge}
            </span>
          )}
        </div>
      </div>
      {}
      {(lastHeartbeat || details) && (
        <div className='space-y-1 text-xs text-dark-300'>
          {lastHeartbeat && (
            <div>Last heartbeat: {new Date(lastHeartbeat).toLocaleTimeString()}</div>
          )}
          {details &&
            Object.entries(details).map(([key, value]) => (
              <div key={key}>
                {key}: {typeof value === 'object' ? JSON.stringify(value) : String(value)}
              </div>
            ))}
        </div>
      )}
      {}
      {showList && listItems.length > 0 && (
        <div className='space-y-2'>
          <div className='text-sm font-medium text-dark-300'>Active processes:</div>
          <div className='space-y-1'>
            {listItems.map(item => (
              <div key={item.id} className='flex items-center justify-between py-1'>
                <div className='flex items-center space-x-2'>
                  <span
                    className={clsx(
                      'w-2 h-2 rounded-full',
                      item.status === 'running' ? 'bg-green-400' : 'bg-gray-400'
                    )}
                  />
                  <span className='text-sm text-dark-200'>{item.name}</span>
                </div>
                {item.status === 'running' && item.onStop && (
                  <button
                    onClick={item.onStop}
                    className='text-xs text-red-400 hover:text-red-300 px-2 py-1 rounded-sm transition-colors'
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
      <div className='flex items-start gap-3 pt-2 border-t border-dark-700'>
        {}
        <div className='flex space-x-2 flex-1'>
          {!isRunning ? (
            <button
              onClick={onStart}
              disabled={isStarting}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStarting
                  ? 'bg-green-400/20 text-green-300 cursor-not-allowed'
                  : 'bg-green-600 text-white hover:bg-green-700'
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
              disabled={isStopping}
              className={clsx(
                'flex-1 px-4 py-2 rounded-md text-sm font-medium transition-colors',
                isStopping
                  ? 'bg-red-400/20 text-red-300 cursor-not-allowed'
                  : 'bg-red-600 text-white hover:bg-red-700'
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
          {}
          {isRunning && (
            <button
              onClick={() => {
                onStop()
                setTimeout(onStart, 1000)
              }}
              className='px-4 py-2 rounded-md text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 transition-colors'
            >
              Restart
            </button>
          )}
        </div>
        {}
        {heartbeatData && Object.keys(heartbeatData).length > 0 && (
          <div className='flex flex-col gap-1'>
            <div className='text-xs font-medium text-dark-300'>{heartbeatLabel}:</div>
            <div className='flex flex-wrap gap-1'>
              {Object.entries(heartbeatData).map(([key, data]) => {
                const isUnknown = data.status === 'unknown'
                const isHealthy = data.healthy && !isUnknown

                return (
                  <div
                    key={key}
                    className={clsx(
                      'px-2 py-1 rounded text-xs whitespace-nowrap',
                      isUnknown
                        ? 'bg-gray-500/10 text-gray-400'
                        : isHealthy
                          ? 'bg-green-400/10 text-green-400'
                          : 'bg-red-400/10 text-red-400'
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
