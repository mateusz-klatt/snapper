import React from 'react'
import type { BacktestRunData } from '../../types/api'

interface Props {
  currentRun: BacktestRunData
}

/**
 * Phase 2c CompareLauncher — Step 4 stub. Step 5 replaces with the
 * full implementation (terminal gate, auto/manual, "show all runs",
 * current-run exclusion, terminal-only filter).
 */
export const CompareLauncher: React.FC<Props> = ({ currentRun }) => {
  return <div data-testid='compare-launcher-stub'>{currentRun.public_id}</div>
}
