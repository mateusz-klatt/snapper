import React from 'react'
import { Card, MetricCard, StatusBadge } from '../../components/ui'
import { CardSkeleton } from '../../components/Skeleton'
import {
  usePositionsSummary,
  useLatestSignals,
  useOrdersGrouped,
  useConfiguredProcesses,
} from '../../hooks/queries'
import { useProcessStore } from '../../stores/process'
import { useTradeStore } from '../../stores/trade'

export const Overview: React.FC = () => {
  const { isLoading: processLoading } = useConfiguredProcesses()
  const { data: positionsSummary, isLoading: positionsLoading } = usePositionsSummary()
  const { data: latestSignals, isLoading: signalsLoading } = useLatestSignals(5)
  const { data: ordersGrouped } = useOrdersGrouped({ limit: 50 })
  const { feeds = {}, strategies = {}, executors = {}, brokers = {} } = useProcessStore()
  const { executions = [] } = useTradeStore()
  const runningFeeds = Object.values(feeds).filter(f => f.running).length
  const totalFeeds = Object.keys(feeds).length
  const runningStrategies = Object.values(strategies).filter(s => s.running).length
  const totalStrategies = Object.keys(strategies).length
  const runningExecutors = Object.values(executors).filter(e => e.running).length
  const totalExecutors = Object.keys(executors).length
  const runningBrokers = Object.values(brokers).filter(b => b.running).length
  const totalBrokers = Object.keys(brokers).length
  const recentExecutions = executions.slice(0, 5)
  const openOrdersCount = ordersGrouped?.open?.length || 0
  const todayExecutionsCount = executions.filter(e => {
    const dateStr = e.executedAt?.toDateString()
    const today = new Date().toDateString()

    return dateStr === today
  }).length

  return (
    <div className='h-full overflow-auto'>
      <div className='space-y-6'>
        {}
        <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
          <MetricCard
            label='Feeds Running'
            value={`${runningFeeds}/${totalFeeds}`}
            changeType={runningFeeds > 0 ? 'positive' : 'neutral'}
          />
          <MetricCard
            label='Strategies Active'
            value={`${runningStrategies}/${totalStrategies}`}
            changeType={runningStrategies > 0 ? 'positive' : 'neutral'}
          />
          <MetricCard label='Open Orders' value={openOrdersCount} changeType='neutral' />
          <MetricCard
            label="Today's Executions"
            value={todayExecutionsCount}
            changeType='positive'
          />
        </div>
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
          {}
          <Card title='Process Status'>
            {processLoading ? (
              <CardSkeleton showTitle={false} contentLines={4} className='border-0 p-0' />
            ) : (
              <div className='space-y-3'>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Feeds</span>
                  <StatusBadge status={runningFeeds > 0 ? 'connected' : 'disconnected'}>
                    {runningFeeds > 0 ? `${runningFeeds} Running` : 'Stopped'}
                  </StatusBadge>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Strategies</span>
                  <StatusBadge status={runningStrategies > 0 ? 'connected' : 'disconnected'}>
                    {runningStrategies > 0 ? `${runningStrategies} Active` : 'Stopped'}
                  </StatusBadge>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Executors</span>
                  <StatusBadge status={runningExecutors > 0 ? 'connected' : 'disconnected'}>
                    {runningExecutors > 0
                      ? `${runningExecutors}/${totalExecutors} Running`
                      : 'Stopped'}
                  </StatusBadge>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Brokers</span>
                  <StatusBadge status={runningBrokers > 0 ? 'connected' : 'disconnected'}>
                    {runningBrokers > 0 ? `${runningBrokers}/${totalBrokers} Running` : 'Stopped'}
                  </StatusBadge>
                </div>
              </div>
            )}
          </Card>
          {}
          <Card title='Portfolio Summary'>
            {positionsLoading ? (
              <CardSkeleton showTitle={false} contentLines={4} className='border-0 p-0' />
            ) : positionsSummary ? (
              <div className='space-y-3'>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Total Value</span>
                  <span className='font-mono text-right'>
                    $
                    {positionsSummary.totalValue.toLocaleString(undefined, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })}
                  </span>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Unrealized P&L</span>
                  <span
                    className={`font-mono text-right ${
                      positionsSummary.totalPnL >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}
                  >
                    {positionsSummary.totalPnL >= 0 ? '+' : ''}$
                    {positionsSummary.totalPnL.toLocaleString(undefined, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })}
                  </span>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>P&L %</span>
                  <span
                    className={`font-mono text-right ${
                      positionsSummary.pnlPercent >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}
                  >
                    {positionsSummary.pnlPercent >= 0 ? '+' : ''}
                    {positionsSummary.pnlPercent.toFixed(2)}%
                  </span>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm font-medium'>Positions</span>
                  <span className='font-mono text-right'>{positionsSummary.count} instruments</span>
                </div>
              </div>
            ) : (
              <div className='text-center py-8 text-dark-400'>No positions data available</div>
            )}
          </Card>
        </div>
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
          {}
          <Card title='Recent Signals'>
            {signalsLoading ? (
              <CardSkeleton showTitle={false} contentLines={5} className='border-0 p-0' />
            ) : latestSignals && latestSignals.length > 0 ? (
              <div className='space-y-2'>
                {latestSignals.map((signal, index) => {
                  const normalizedSide = signal.side.toLowerCase()

                  return (
                    <div
                      key={signal.id ?? signal.timestamp?.getTime() ?? `signal-${index}`}
                      className='flex items-center justify-between p-2 bg-dark-700 rounded-sm'
                    >
                      <div className='flex items-center gap-3'>
                        <StatusBadge status={normalizedSide === 'buy' ? 'connected' : 'error'}>
                          {normalizedSide.toUpperCase()}
                        </StatusBadge>
                        <span className='text-sm font-medium'>{signal.instrument}</span>
                      </div>
                      <div className='text-xs text-dark-300'>
                        {signal.timestamp?.toLocaleTimeString() ?? 'N/A'}
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div className='text-center py-8 text-dark-400'>No recent signals</div>
            )}
          </Card>
          {}
          <Card title='Recent Executions'>
            {recentExecutions.length > 0 ? (
              <div className='space-y-2'>
                {recentExecutions.map(execution => (
                  <div
                    key={execution.id}
                    className='flex items-center justify-between p-2 bg-dark-700 rounded-sm'
                  >
                    <div className='flex items-center gap-3'>
                      <StatusBadge status={execution.side === 'sell' ? 'error' : 'connected'}>
                        {execution.side.toUpperCase()}
                      </StatusBadge>
                      <span className='text-sm font-medium'>{execution.instrument}</span>
                      <span className='text-xs text-dark-300'>
                        {execution.size} @ ${execution.price}
                      </span>
                    </div>
                    <div className='text-xs text-dark-300'>
                      {execution.executedAt?.toLocaleTimeString() ?? 'N/A'}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className='text-center py-8 text-dark-400'>No recent executions</div>
            )}
          </Card>
        </div>
      </div>
    </div>
  )
}
