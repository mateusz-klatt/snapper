import React from 'react'
import { useSystemStatus } from '../../hooks/queries'
import { HealthSkeleton } from '../../components/Skeleton'
import clsx from 'clsx'

interface ProcessStatus {
  status: string
  pid?: number | null
  started_at?: string | null
  command?: string | null
  exit_code?: number | null
  error?: string | null
}
type HealthStatus = 'healthy' | 'warning' | 'error'

interface HealthMetric {
  name: string
  value: string | number
  status: HealthStatus
  description: string
  icon: React.ReactNode
}

const StatusIndicator: React.FC<{ status: string; showLabel: boolean }> = ({
  status,
  showLabel,
}) => {
  const getStatusConfig = (status: string) => {
    switch (status.toLowerCase()) {
      case 'running':
        return { color: 'bg-green-400', label: 'Running', textColor: 'text-green-400' }
      case 'stopped':
      case 'not_running':
        return { color: 'bg-gray-400', label: 'Stopped', textColor: 'text-gray-400' }
      case 'error':
      case 'failed':
        return { color: 'bg-red-400', label: 'Error', textColor: 'text-red-400' }
      case 'completed':
        return { color: 'bg-blue-400', label: 'Completed', textColor: 'text-blue-400' }
      default:
        return { color: 'bg-yellow-400', label: 'Unknown', textColor: 'text-yellow-400' }
    }
  }

  const config = getStatusConfig(status)

  return (
    <div className='flex items-center space-x-2'>
      <div
        className={clsx(
          'w-2 h-2 rounded-full',
          config.color,
          status === 'running' && 'animate-pulse'
        )}
      />
      {showLabel && (
        <span className={clsx('text-sm font-medium', config.textColor)}>{config.label}</span>
      )}
    </div>
  )
}

const ProcessCard: React.FC<{
  name: string
  status: ProcessStatus
  type: 'service' | 'backtest'
}> = ({ name, status, type }) => {
  const formatUptime = (startedAt?: string) => {
    if (!startedAt) return 'N/A'
    const start = new Date(startedAt)
    const now = new Date()
    const diffMs = now.getTime() - start.getTime()
    const diffMins = Math.floor(diffMs / 60000)

    if (diffMins < 60) return `${diffMins}m`
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h ${diffMins % 60}m`

    return `${Math.floor(diffMins / 1440)}d ${Math.floor((diffMins % 1440) / 60)}h`
  }

  const getProcessIcon = (_name: string, type: string) => {
    if (type === 'backtest') {
      return (
        <svg className='w-5 h-5' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
          <path
            strokeLinecap='round'
            strokeLinejoin='round'
            strokeWidth={2}
            d='M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z'
          />
        </svg>
      )
    }

    return (
      <svg className='w-5 h-5' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
        <path
          strokeLinecap='round'
          strokeLinejoin='round'
          strokeWidth={2}
          d='M13 10V3L4 14h7v7l9-11h-7z'
        />
      </svg>
    )
  }

  return (
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <div className='text-dark-400'>{getProcessIcon(name, type)}</div>
          <div>
            <h3 className='font-medium text-white'>{name}</h3>
            <p className='text-xs text-dark-400 capitalize'>{type}</p>
          </div>
        </div>
        <StatusIndicator status={status.status} showLabel />
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm'>
        <div>
          <div className='text-dark-400'>PID</div>
          <div className='text-white font-mono'>{status.pid || 'N/A'}</div>
        </div>
        <div>
          <div className='text-dark-400'>Uptime</div>
          <div className='text-white'>{formatUptime(status.started_at ?? undefined)}</div>
        </div>
      </div>
      {status.error && (
        <div className='mt-3 p-2 bg-red-900/20 border border-red-800 rounded-sm text-xs text-red-400'>
          {status.error}
        </div>
      )}
      {status.exit_code !== undefined && status.status !== 'running' && (
        <div className='mt-2 text-xs text-dark-400'>Exit code: {status.exit_code}</div>
      )}
    </div>
  )
}

const MetricCard: React.FC<{ metric: HealthMetric }> = ({ metric }) => {
  const statusColors: Record<HealthMetric['status'], string> = {
    healthy: 'text-green-400 border-green-800 bg-green-900/20',
    warning: 'text-yellow-400 border-yellow-800 bg-yellow-900/20',
    error: 'text-red-400 border-red-800 bg-red-900/20',
  }
  const getStatusColor = (status: HealthMetric['status']) => statusColors[status]

  return (
    <div className={clsx('p-4 rounded-lg border', getStatusColor(metric.status))}>
      <div className='flex items-center justify-between mb-2'>
        <div className='flex items-center space-x-2'>
          <div className='text-current'>{metric.icon}</div>
          <span className='font-medium'>{metric.name}</span>
        </div>
        <div className='text-lg font-bold'>{metric.value}</div>
      </div>
      <p className='text-xs opacity-80'>{metric.description}</p>
    </div>
  )
}

export const Health: React.FC = () => {
  const { data: systemStatus, isLoading } = useSystemStatus()
  const backtestsList = Object.values(systemStatus?.backtests || {})
  const runningBacktests = backtestsList.filter(b => b.status === 'running').length
  const hasErroredBacktest = backtestsList.some(b => b.status === 'error')
  const backtestStatus: HealthStatus = hasErroredBacktest ? 'error' : 'healthy'
  const healthMetrics: HealthMetric[] = [
    {
      name: 'Trading Engine',
      value: systemStatus?.trader?.status === 'running' ? 'Active' : 'Inactive',
      status: systemStatus?.trader?.status === 'running' ? 'healthy' : 'warning',
      description: 'Strategy execution engine',
      icon: (
        <svg className='w-5 h-5' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
          <path
            strokeLinecap='round'
            strokeLinejoin='round'
            strokeWidth={2}
            d='M13 10V3L4 14h7v7l9-11h-7z'
          />
        </svg>
      ),
    },
    {
      name: 'Active Backtests',
      value: runningBacktests,
      status: backtestStatus,
      description: 'Running backtest processes',
      icon: (
        <svg className='w-5 h-5' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
          <path
            strokeLinecap='round'
            strokeLinejoin='round'
            strokeWidth={2}
            d='M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z'
          />
        </svg>
      ),
    },
  ]

  const resolveOverallHealth = (): HealthStatus => {
    if (healthMetrics.every(m => m.status === 'healthy')) return 'healthy'
    if (healthMetrics.some(m => m.status === 'error')) return 'error'

    return 'warning'
  }

  const overallHealth = resolveOverallHealth()

  return (
    <div className='p-4 space-y-6'>
      {}
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-bold text-white'>System Health</h2>
        <div className='flex items-center space-x-2'>
          <StatusIndicator status={overallHealth} showLabel />
          <span className='text-sm text-dark-400'>
            Last updated: {new Date().toLocaleTimeString()}
          </span>
        </div>
      </div>
      {}
      <div>
        <h3 className='text-lg font-medium text-white mb-4'>Health Metrics</h3>
        <div className='grid grid-cols-2 lg:grid-cols-4 gap-4'>
          {healthMetrics.map(metric => (
            <MetricCard key={metric.name} metric={metric} />
          ))}
        </div>
      </div>
      {}
      <div>
        <h3 className='text-lg font-medium text-white mb-4'>Process Status</h3>
        {isLoading ? (
          <HealthSkeleton className='mt-0 p-0' />
        ) : (
          <div className='grid gap-4'>
            {}
            <div className='grid grid-cols-1 md:grid-cols-2 gap-4'>
              <ProcessCard
                name='Trading Engine'
                status={systemStatus?.trader || { status: 'unknown' }}
                type='service'
              />
            </div>
            {}
            {systemStatus?.backtests && Object.keys(systemStatus.backtests).length > 0 && (
              <div>
                <h4 className='text-md font-medium text-white mb-3'>Active Backtests</h4>
                <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
                  {Object.entries(systemStatus.backtests).map(([id, status]) => (
                    <ProcessCard
                      key={id}
                      name={`Backtest ${id.slice(0, 8)}`}
                      status={status}
                      type='backtest'
                    />
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
      {}
      <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
        <h3 className='text-lg font-medium text-white mb-3'>Quick Actions</h3>
        <div className='flex flex-wrap gap-3'>
          <button
            disabled
            title='Feature not yet implemented'
            className='px-4 py-2 bg-blue-600/50 text-white/50 text-sm rounded-sm cursor-not-allowed'
          >
            Restart Services
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='px-4 py-2 bg-green-600/50 text-white/50 text-sm rounded-sm cursor-not-allowed'
          >
            Run Health Check
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='px-4 py-2 bg-orange-600/50 text-white/50 text-sm rounded-sm cursor-not-allowed'
          >
            View Logs
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='px-4 py-2 bg-purple-600/50 text-white/50 text-sm rounded-sm cursor-not-allowed'
          >
            Export Report
          </button>
        </div>
      </div>
    </div>
  )
}
