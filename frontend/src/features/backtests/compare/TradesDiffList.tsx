import React, { useMemo } from 'react'
import clsx from 'clsx'
import type { TradeDiffEntry } from '../../../types/api'

interface Props {
  entries: TradeDiffEntry[]
}

const formatTime = (iso: string): string => new Date(iso).toLocaleString()

const formatPnl = (v: number | null | undefined): string => {
  if (v === null || v === undefined) return '—'

  return v.toFixed(2)
}

const pnlColor = (v: number | null | undefined): string => {
  if (v === null || v === undefined || v === 0) return 'text-muted-400'

  return v > 0 ? 'text-gain-400' : 'text-loss-400'
}

const TradeCard: React.FC<{ trade: TradeDiffEntry }> = ({ trade }) => (
  <div className='rounded-lg border border-dark-600 bg-alpine-50 p-2 text-xs'>
    <div className='flex items-center justify-between'>
      <span className='font-mono text-alpine-900'>{trade.instrument}</span>
      <span className='text-muted-500'>{formatTime(trade.executed_at)}</span>
    </div>
    <div className='mt-1 flex items-center gap-2 font-mono text-alpine-900'>
      <span>{trade.side}</span>
      <span>{trade.quantity}</span>
      <span>@ {trade.price}</span>
    </div>
    <div className='mt-1 flex items-center gap-2 text-xs'>
      <span>
        A pnl <span className={pnlColor(trade.pnl_a)}>{formatPnl(trade.pnl_a)}</span>
      </span>
      <span>
        B pnl <span className={pnlColor(trade.pnl_b)}>{formatPnl(trade.pnl_b)}</span>
      </span>
      <span>
        Δ <span className={pnlColor(trade.pnl_delta)}>{formatPnl(trade.pnl_delta)}</span>
      </span>
    </div>
  </div>
)

interface ColumnProps {
  title: string
  entries: TradeDiffEntry[]
  testId: string
}

const Column: React.FC<ColumnProps> = ({ title, entries, testId }) => (
  <div className='space-y-2' data-testid={testId}>
    <h4 className={clsx('text-sm font-semibold text-alpine-900')}>
      {title} <span className='text-muted-500'>({entries.length})</span>
    </h4>
    {entries.length === 0 ? (
      <div className='text-xs text-muted-500'>—</div>
    ) : (
      entries.map(t => <TradeCard key={`${t.instrument}-${t.executed_at}-${t.leg}`} trade={t} />)
    )}
  </div>
)

/**
 * Trades diff list — three columns split by `entry.leg`
 * (`common`, `a` ↔ only_in_a, `b` ↔ only_in_b). Counts in headers.
 */
export const TradesDiffList: React.FC<Props> = ({ entries }) => {
  const { common, onlyA, onlyB } = useMemo(() => {
    const c: TradeDiffEntry[] = []
    const a: TradeDiffEntry[] = []
    const b: TradeDiffEntry[] = []

    for (const entry of entries) {
      if (entry.leg === 'common') c.push(entry)
      else if (entry.leg === 'a') a.push(entry)
      else b.push(entry)
    }

    return { common: c, onlyA: a, onlyB: b }
  }, [entries])

  return (
    <div className='grid grid-cols-1 gap-4 md:grid-cols-3' data-testid='trades-diff-list'>
      <Column title='Common' entries={common} testId='trades-diff-common' />
      <Column title='Only in A' entries={onlyA} testId='trades-diff-only-a' />
      <Column title='Only in B' entries={onlyB} testId='trades-diff-only-b' />
    </div>
  )
}
