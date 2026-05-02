import React from 'react'
import type { BacktestProgressSnapshot } from './hooks/useBacktestProgressSubscription'

interface Props {
  snapshot: BacktestProgressSnapshot | null
}

const MILESTONE_LABELS: Record<string, string> = {
  '25pct': '25 %',
  '50pct': '50 %',
  '75pct': '75 %',
}

/**
 * Progress bar — renders a 0..100 % bar plus milestone chips and
 * current candle / equity counts. Falls back to "no progress yet"
 * when no event has arrived.
 */
export const BacktestProgressBar: React.FC<Props> = ({ snapshot }) => {
  if (!snapshot) {
    return <div className='text-sm opacity-60'>Waiting for progress…</div>
  }

  const pct = Math.round((snapshot.progress_pct ?? 0) * 100)
  const milestoneLabel =
    snapshot.event === 'milestone' && snapshot.milestone
      ? MILESTONE_LABELS[snapshot.milestone]
      : null

  return (
    <div className='space-y-1'>
      <div className='w-full bg-gray-200 dark:bg-gray-700 rounded h-2 overflow-hidden'>
        <div
          className='bg-indigo-500 h-2 transition-all'
          style={{ width: `${pct}%` }}
          data-testid='bt-progress-fill'
        />
      </div>
      <div className='text-xs flex justify-between'>
        <span>
          {snapshot.candles_done}
          {snapshot.total_candles ? ` / ${snapshot.total_candles}` : ''} candles
        </span>
        <span>equity {snapshot.equity?.toFixed(2)}</span>
        <span>
          {snapshot.signals_count} sig / {snapshot.trades_count} trades
        </span>
      </div>
      {milestoneLabel ? (
        <div className='text-xs italic opacity-80'>milestone {milestoneLabel}</div>
      ) : null}
    </div>
  )
}
