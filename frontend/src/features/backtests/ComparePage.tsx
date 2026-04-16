import React from 'react'

interface Props {
  comparisonPublicId: string
}

/**
 * Phase 2c Compare page — Step 3 stub. Step 6 replaces with the full
 * implementation (4 sub-components + wallet-scope error handling).
 */
export const ComparePage: React.FC<Props> = ({ comparisonPublicId }) => {
  return <div data-testid='compare-page'>{comparisonPublicId}</div>
}
