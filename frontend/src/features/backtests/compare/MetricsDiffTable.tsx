import React from 'react'
import clsx from 'clsx'
import type { MetricDiffRow } from '../../../types/api'

interface Props {
  rows: MetricDiffRow[]
}

const formatNumber = (v: number | null | undefined): string => {
  if (v === null || v === undefined) return '—'

  return Number.isFinite(v) ? v.toFixed(4) : String(v)
}

const formatPct = (v: number | null | undefined): string => {
  if (v === null || v === undefined) return '—'

  return `${(v * 100).toFixed(2)} %`
}

const deltaColor = (v: number | null | undefined): string => {
  if (v === null || v === undefined || v === 0) return 'text-muted-400'

  return v > 0 ? 'text-gain-400' : 'text-loss-400'
}

/**
 * Metrics diff table — one row per metric name, columns for run A
 * value, run B value, absolute delta, percent delta. Renders an
 * empty state when the diff is empty (e.g. both runs had no equity
 * points).
 */
export const MetricsDiffTable: React.FC<Props> = ({ rows }) => {
  if (rows.length === 0) {
    return (
      <div className='text-sm text-muted-500' data-testid='metrics-diff-empty'>
        No comparable metrics — both runs had no recorded values.
      </div>
    )
  }

  return (
    <table className='w-full text-sm' data-testid='metrics-diff-table'>
      <thead>
        <tr className='text-left text-muted-500'>
          <th className='py-1'>Metric</th>
          <th className='py-1 text-right'>Run A</th>
          <th className='py-1 text-right'>Run B</th>
          <th className='py-1 text-right'>Δ</th>
          <th className='py-1 text-right'>Δ %</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(row => (
          <tr key={row.name} className='border-t border-dark-600'>
            <td className='py-1 text-alpine-900'>{row.name}</td>
            <td className='py-1 text-right font-mono text-alpine-900'>{formatNumber(row.run_a)}</td>
            <td className='py-1 text-right font-mono text-alpine-900'>{formatNumber(row.run_b)}</td>
            <td className={clsx('py-1 text-right font-mono', deltaColor(row.delta))}>
              {formatNumber(row.delta)}
            </td>
            <td className={clsx('py-1 text-right font-mono', deltaColor(row.pct))}>
              {formatPct(row.pct)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
