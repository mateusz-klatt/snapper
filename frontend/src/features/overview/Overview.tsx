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
import type { ProcessStatus } from '../../types/ui'
import type { Signal, Fill } from '../../types/entities'

const CURRENCY_FORMAT = { minimumFractionDigits: 2, maximumFractionDigits: 2 }

const formatCurrency = (value: number): string => value.toLocaleString(undefined, CURRENCY_FORMAT)

const pnlSign = (value: number): string => (value >= 0 ? '+' : '')

const runningBadgeStatus = (count: number): 'connected' | 'disconnected' =>
  count > 0 ? 'connected' : 'disconnected'

const countChangeType = (count: number): 'positive' | 'neutral' =>
  count > 0 ? 'positive' : 'neutral'

const countRunning = (processes: Record<string, ProcessStatus>): number =>
  Object.values(processes).filter(p => p.running).length

const countTotal = (processes: Record<string, ProcessStatus>): number =>
  Object.keys(processes).length

const sideStatus = (side: string): 'connected' | 'error' => (side === 'buy' ? 'connected' : 'error')

const statusBadgeLabel = (
  running: number,
  total: number | undefined,
  activeLabel: string
): string => {
  if (running <= 0) return 'Stopped'
  if (total !== undefined) return `${running}/${total} ${activeLabel}`

  return `${running} ${activeLabel}`
}

const ProcessStatusRow: React.FC<
  Readonly<{ label: string; running: number; total?: number; activeLabel: string }>
> = ({ label, running, total, activeLabel }) => (
  <div className='flex items-center justify-between'>
    <span className='text-sm font-medium'>{label}</span>
    <StatusBadge status={runningBadgeStatus(running)}>
      {statusBadgeLabel(running, total, activeLabel)}
    </StatusBadge>
  </div>
)

interface PortfolioContentProps {
  readonly totalValue: number
  readonly totalPnL: number
  readonly pnlPercent: number
  readonly count: number
}

const PortfolioContent: React.FC<PortfolioContentProps> = ({
  totalValue,
  totalPnL,
  pnlPercent,
  count,
}) => {
  const pnlColorClass = (value: number): string => (value >= 0 ? 'text-gain-600' : 'text-loss-600')

  return (
    <div className='space-y-3'>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Total Value</span>
        <span className='font-mono text-right'>${formatCurrency(totalValue)}</span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Unrealized P&L</span>
        <span className={`font-mono text-right ${pnlColorClass(totalPnL)}`}>
          {pnlSign(totalPnL)}${formatCurrency(totalPnL)}
        </span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>P&L %</span>
        <span className={`font-mono text-right ${pnlColorClass(pnlPercent)}`}>
          {pnlSign(pnlPercent)}
          {pnlPercent.toFixed(2)}%
        </span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Positions</span>
        <span className='font-mono text-right'>{count} instruments</span>
      </div>
    </div>
  )
}

const signalKey = (signal: Signal, index: number): string | number =>
  signal.id ?? signal.timestamp?.getTime() ?? `signal-${index}`

const SignalRow: React.FC<Readonly<{ signal: Signal; index: number }>> = ({ signal, index }) => {
  const normalizedSide = signal.side.toLowerCase()

  return (
    <div
      key={signalKey(signal, index)}
      className='flex items-center justify-between p-2 bg-dark-700 rounded-sm'
    >
      <div className='flex items-center gap-3'>
        <StatusBadge status={sideStatus(normalizedSide)}>
          {normalizedSide.toUpperCase()}
        </StatusBadge>
        <span className='text-sm font-medium'>{signal.instrument}</span>
      </div>
      <div className='text-xs text-dark-300'>{signal.timestamp?.toLocaleTimeString() ?? 'N/A'}</div>
    </div>
  )
}

const ExecutionRow: React.FC<Readonly<{ execution: Fill }>> = ({ execution }) => (
  <div className='flex items-center justify-between p-2 bg-dark-700 rounded-sm'>
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
)

const PortfolioCardContent: React.FC<
  Readonly<{
    loading: boolean
    summary: PortfolioContentProps | null
  }>
> = ({ loading, summary }) => {
  if (loading) return <CardSkeleton showTitle={false} contentLines={4} className='border-0 p-0' />
  if (summary) return <PortfolioContent {...summary} />

  return <div className='text-center py-8 text-dark-400'>No positions data available</div>
}

const SignalsCardContent: React.FC<
  Readonly<{
    loading: boolean
    signals: readonly Signal[] | undefined
  }>
> = ({ loading, signals }) => {
  if (loading) return <CardSkeleton showTitle={false} contentLines={5} className='border-0 p-0' />
  if (signals && signals.length > 0)
    return (
      <div className='space-y-2'>
        {signals.map((signal, index) => (
          <SignalRow key={signalKey(signal, index)} signal={signal} index={index} />
        ))}
      </div>
    )

  return <div className='text-center py-8 text-dark-400'>No recent signals</div>
}

export const Overview: React.FC = () => {
  const { isLoading: processLoading } = useConfiguredProcesses()
  const { data: positionsSummary, isLoading: positionsLoading } = usePositionsSummary()
  const { data: latestSignals, isLoading: signalsLoading } = useLatestSignals(5)
  const { data: ordersGrouped } = useOrdersGrouped({ limit: 50 })
  const { feeds = {}, strategies = {}, executors = {}, brokers = {} } = useProcessStore()
  const { executions = [] } = useTradeStore()
  const runningFeeds = countRunning(feeds)
  const totalFeeds = countTotal(feeds)
  const runningStrategies = countRunning(strategies)
  const totalStrategies = countTotal(strategies)
  const runningExecutors = countRunning(executors)
  const totalExecutors = countTotal(executors)
  const runningBrokers = countRunning(brokers)
  const totalBrokers = countTotal(brokers)
  const recentExecutions = executions.slice(0, 5)
  const openOrdersCount = ordersGrouped?.open?.length || 0
  const todayStr = new Date().toDateString()
  const todayExecutionsCount = executions.filter(
    e => e.executedAt?.toDateString() === todayStr
  ).length

  return (
    <div className='space-y-6'>
      <h2 className='text-xl font-semibold text-alpine-900'>Overview</h2>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
        <MetricCard
          label='Feeds Running'
          value={`${runningFeeds}/${totalFeeds}`}
          changeType={countChangeType(runningFeeds)}
        />
        <MetricCard
          label='Strategies Active'
          value={`${runningStrategies}/${totalStrategies}`}
          changeType={countChangeType(runningStrategies)}
        />
        <MetricCard label='Open Orders' value={openOrdersCount} changeType='neutral' />
        <MetricCard label="Today's Executions" value={todayExecutionsCount} changeType='positive' />
      </div>
      <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
        {}
        <Card title='Process Status'>
          {processLoading ? (
            <CardSkeleton showTitle={false} contentLines={4} className='border-0 p-0' />
          ) : (
            <div className='space-y-3'>
              <ProcessStatusRow label='Feeds' running={runningFeeds} activeLabel='Running' />
              <ProcessStatusRow
                label='Strategies'
                running={runningStrategies}
                activeLabel='Active'
              />
              <ProcessStatusRow
                label='Executors'
                running={runningExecutors}
                total={totalExecutors}
                activeLabel='Running'
              />
              <ProcessStatusRow
                label='Brokers'
                running={runningBrokers}
                total={totalBrokers}
                activeLabel='Running'
              />
            </div>
          )}
        </Card>
        {}
        <Card title='Portfolio Summary'>
          <PortfolioCardContent loading={positionsLoading} summary={positionsSummary ?? null} />
        </Card>
      </div>
      <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
        {}
        <Card title='Recent Signals'>
          <SignalsCardContent loading={signalsLoading} signals={latestSignals} />
        </Card>
        {}
        <Card title='Recent Executions'>
          {recentExecutions.length > 0 ? (
            <div className='space-y-2'>
              {recentExecutions.map(execution => (
                <ExecutionRow key={execution.clientOrderId} execution={execution} />
              ))}
            </div>
          ) : (
            <div className='text-center py-8 text-dark-400'>No recent executions</div>
          )}
        </Card>
      </div>
    </div>
  )
}
