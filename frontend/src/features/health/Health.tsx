import React from 'react'
import { useSystemStatus } from '../../hooks/queries'
import { HealthSkeleton } from '../../components/Skeleton'
import { LiveOnlyNotice } from '../../components/LiveOnlyNotice'
import { SystemMetricsCard } from './SystemMetricsCard'
import { DbStatsCard } from './DbStatsCard'
import { NotificationMetricsCard } from './NotificationMetricsCard'
import { RetentionCard } from './RetentionCard'
import clsx from 'clsx'
import type { ProcessStatus } from '../../types/api'

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
      case 'healthy':
        return { color: 'bg-accent-500', label: 'Healthy', textColor: 'text-gain-600' }
      case 'stopped':
      case 'not_running':
        return { color: 'bg-muted-400', label: 'Stopped', textColor: 'text-muted-600' }
      case 'error':
      case 'failed':
        return { color: 'bg-loss-500', label: 'Error', textColor: 'text-loss-600' }
      case 'warning':
        return { color: 'bg-warning-500', label: 'Warning', textColor: 'text-warning-600' }
      case 'completed':
        return { color: 'bg-info-500', label: 'Completed', textColor: 'text-info-600' }
      default:
        return { color: 'bg-warning-500', label: 'Unknown', textColor: 'text-warning-600' }
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
    <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-5'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <div className='text-muted-500'>{getProcessIcon(name, type)}</div>
          <div>
            <h3 className='font-medium text-alpine-900'>{name}</h3>
            <p className='text-xs capitalize text-muted-500'>{type}</p>
          </div>
        </div>
        <StatusIndicator status={status.status} showLabel />
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm'>
        <div>
          <div className='text-muted-500'>PID</div>
          <div className='font-mono text-alpine-900'>{status.pid || 'N/A'}</div>
        </div>
        <div>
          <div className='text-muted-500'>Uptime</div>
          <div className='text-alpine-900'>{formatUptime(status.started_at ?? undefined)}</div>
        </div>
      </div>
      {status.error && (
        <div className='mt-3 rounded-lg border border-loss-100 bg-loss-50 p-2 text-xs text-loss-700'>
          {status.error}
        </div>
      )}
      {status.exit_code !== undefined && status.status !== 'running' && (
        <div className='mt-2 text-xs text-muted-500'>Exit code: {status.exit_code}</div>
      )}
    </div>
  )
}

const MetricCard: React.FC<{ metric: HealthMetric }> = ({ metric }) => {
  const statusColors: Record<HealthMetric['status'], string> = {
    healthy: 'border-accent-100 bg-accent-50 text-accent-700',
    warning: 'border-warning-100 bg-warning-50 text-warning-700',
    error: 'border-loss-100 bg-loss-50 text-loss-700',
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
  const statusPayload = systemStatus?.payload
  const backtestsList = Object.values(statusPayload?.backtests || {})
  const runningBacktests = backtestsList.filter(b => b.status === 'running').length
  const hasErroredBacktest = backtestsList.some(b => b.status === 'error')
  const backtestStatus: HealthStatus = hasErroredBacktest ? 'error' : 'healthy'
  const healthMetrics: HealthMetric[] = [
    {
      name: 'Trading Engine',
      value: statusPayload?.trader?.status === 'running' ? 'Active' : 'Inactive',
      status: statusPayload?.trader?.status === 'running' ? 'healthy' : 'warning',
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
    <div className='space-y-6'>
      <LiveOnlyNotice />
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-semibold text-alpine-900'>System Health</h2>
        <StatusIndicator status={overallHealth} showLabel />
      </div>
      <div>
        <h3 className='mb-4 text-lg font-medium text-alpine-900'>Health Metrics</h3>
        <div className='grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4'>
          {healthMetrics.map(metric => (
            <MetricCard key={metric.name} metric={metric} />
          ))}
        </div>
      </div>
      <div>
        <h3 className='mb-4 text-lg font-medium text-alpine-900'>Process Status</h3>
        {isLoading ? (
          <HealthSkeleton className='mt-0 p-0' />
        ) : (
          <div className='grid gap-4'>
            <div className='grid grid-cols-1 md:grid-cols-2 gap-4'>
              <ProcessCard
                name='Trading Engine'
                status={statusPayload?.trader || { status: 'not_running' }}
                type='service'
              />
            </div>
            {statusPayload?.backtests && Object.keys(statusPayload.backtests).length > 0 && (
              <div>
                <h4 className='mb-3 text-md font-medium text-alpine-900'>Active Backtests</h4>
                <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
                  {Object.entries(statusPayload.backtests).map(([id, status]) => (
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
      <SystemMetricsCard />
      <DbStatsCard />
      <NotificationMetricsCard />
      <RetentionCard />
      <div className='rounded-2xl border border-dark-600 bg-alpine-50 p-5'>
        <h3 className='mb-3 text-lg font-medium text-alpine-900'>Quick Actions</h3>
        <div className='flex flex-wrap gap-3'>
          <button
            disabled
            title='Feature not yet implemented'
            className='cursor-not-allowed rounded-md border border-dark-600 bg-dark-700 px-4 py-2 text-sm text-muted-500'
          >
            Restart Services
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='cursor-not-allowed rounded-md border border-dark-600 bg-dark-700 px-4 py-2 text-sm text-muted-500'
          >
            Run Health Check
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='cursor-not-allowed rounded-md border border-dark-600 bg-dark-700 px-4 py-2 text-sm text-muted-500'
          >
            View Logs
          </button>
          <button
            disabled
            title='Feature not yet implemented'
            className='cursor-not-allowed rounded-md border border-dark-600 bg-dark-700 px-4 py-2 text-sm text-muted-500'
          >
            Export Report
          </button>
        </div>
      </div>
    </div>
  )
}
