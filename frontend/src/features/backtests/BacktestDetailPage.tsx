import React from 'react'
import { useBacktestProgressSubscription } from './hooks/useBacktestProgressSubscription'
import { BacktestProgressBar } from './BacktestProgressBar'
import { isUuid7 } from '../../lib/ids'

interface Props {
  runPublicId: string
}

/**
 * Phase 2c backtest detail page — minimal scaffold.
 *
 * Hosts the live progress subscription + bar. Future iterations will
 * embed the equity curve, trades table, and CompareLauncher
 * (CompareLauncher lands in Step 4).
 */
export const BacktestDetailPage: React.FC<Props> = ({ runPublicId }) => {
  const validRun = isUuid7(runPublicId)
  const snapshot = useBacktestProgressSubscription(validRun ? runPublicId : null)

  if (!validRun) {
    return (
      <div className='p-4 text-sm text-red-600'>
        Invalid run id — must be a UUID7 (got &quot;{runPublicId}&quot;).
      </div>
    )
  }

  return (
    <div className='p-4 space-y-4'>
      <h2 className='text-lg font-semibold'>Backtest run</h2>
      <code className='text-xs opacity-70'>{runPublicId}</code>
      <BacktestProgressBar snapshot={snapshot} />
    </div>
  )
}
