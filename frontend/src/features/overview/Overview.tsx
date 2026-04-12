import React from 'react'
import { Card, MetricCard, StatusBadge } from '../../components/ui'
import { CardSkeleton } from '../../components/Skeleton'
import { useAppStore } from '../../stores/app'
import {
  usePositionsSummary,
  useLatestSignals,
  useOrdersGrouped,
  useProcessSummary,
  useExecutions,
} from '../../hooks/queries'
import type { Signal, Execution } from '../../types/entities'

const CURRENCY_FORMAT = { minimumFractionDigits: 2, maximumFractionDigits: 2 }

const formatCurrency = (value: number): string => value.toLocaleString('en-US', CURRENCY_FORMAT)

const pnlSign = (value: number): string => (value >= 0 ? '+' : '')

const runningBadgeStatus = (count: number): 'connected' | 'disconnected' =>
  count > 0 ? 'connected' : 'disconnected'

const countChangeType = (count: number): 'positive' | 'neutral' =>
  count > 0 ? 'positive' : 'neutral'

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
  readonly longCost: number
  readonly shortCost: number
}

const PortfolioContent: React.FC<PortfolioContentProps> = ({
  totalValue,
  totalPnL,
  pnlPercent,
  count,
  longCost,
  shortCost,
}) => {
  const pnlColorClass = (value: number): string => (value >= 0 ? 'text-gain-600' : 'text-loss-600')
  const netDelta = longCost - shortCost

  return (
    <div className='space-y-3'>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Total Value</span>
        <span className='font-mono text-right'>${formatCurrency(totalValue)}</span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Long exposure</span>
        <span className='font-mono text-right text-gain-600' data-testid='overview-long-exposure'>
          ${formatCurrency(longCost)}
        </span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Short exposure</span>
        <span className='font-mono text-right text-loss-600' data-testid='overview-short-exposure'>
          ${formatCurrency(shortCost)}
        </span>
      </div>
      <div className='flex items-center justify-between'>
        <span className='text-sm font-medium'>Net delta</span>
        <span
          className={`font-mono text-right ${pnlColorClass(netDelta)}`}
          data-testid='overview-net-delta'
        >
          {pnlSign(netDelta)}${formatCurrency(netDelta)}
        </span>
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
  signal.firedAt?.getTime() ?? `signal-${index}`

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
      <div className='text-xs text-dark-300'>{signal.firedAt?.toLocaleTimeString() ?? 'N/A'}</div>
    </div>
  )
}

const ExecutionRow: React.FC<Readonly<{ execution: Execution }>> = ({ execution }) => (
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

const zeroCounts = { running: 0, total: 0 }

export const Overview: React.FC = () => {
  const asOf = useAppStore(s => s.asOf)
  const { data: processSummary, isLoading: processLoading } = useProcessSummary()
  const { data: positionsSummary, isLoading: positionsLoading } = usePositionsSummary()
  const { data: latestSignals, isLoading: signalsLoading } = useLatestSignals(5)
  const { data: ordersGrouped } = useOrdersGrouped({ limit: 50 })
  const { data: executions = [] } = useExecutions()
  const feeds = processSummary?.payload?.feeds ?? zeroCounts
  const strategies = processSummary?.payload?.strategies ?? zeroCounts
  const executors = processSummary?.payload?.executors ?? zeroCounts
  const brokers = processSummary?.payload?.brokers ?? zeroCounts
  const recentExecutions = executions.slice(0, 5)
  const openOrdersCount = ordersGrouped?.open?.length || 0
  const referenceDate = asOf ? new Date(asOf) : new Date()
  const referenceDayStr = referenceDate.toDateString()
  const todayExecutionsCount = executions.filter(
    e => e.executedAt?.toDateString() === referenceDayStr
  ).length

  return (
    <div className='space-y-6'>
      <h2 className='text-xl font-semibold text-alpine-900'>Overview</h2>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
        <MetricCard
          label='Feeds Running'
          value={`${feeds.running}/${feeds.total}`}
          changeType={countChangeType(feeds.running)}
        />
        <MetricCard
          label='Strategies Active'
          value={`${strategies.running}/${strategies.total}`}
          changeType={countChangeType(strategies.running)}
        />
        <MetricCard label='Open Orders' value={openOrdersCount} changeType='neutral' />
        <MetricCard
          label={asOf ? `Executions (${referenceDate.toLocaleDateString()})` : "Today's Executions"}
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
              <ProcessStatusRow label='Feeds' running={feeds.running} activeLabel='Running' />
              <ProcessStatusRow
                label='Strategies'
                running={strategies.running}
                activeLabel='Active'
              />
              <ProcessStatusRow
                label='Executors'
                running={executors.running}
                total={executors.total}
                activeLabel='Running'
              />
              <ProcessStatusRow
                label='Brokers'
                running={brokers.running}
                total={brokers.total}
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
