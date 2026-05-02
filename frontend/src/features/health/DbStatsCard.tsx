import React, { useMemo } from 'react'

import { useDbStats } from '../../hooks/queries'
import { formatNumber } from '../../lib/utils'

import type { TableStatsItem } from '../../types/api'

function formatCount(value: number | null): string {
  if (value === null) return '—'

  return formatNumber(value)
}

function formatRelativeAge(busTime: string, now: Date): string {
  const ageMs = now.getTime() - new Date(busTime).getTime()
  const ageS = ageMs / 1000

  if (ageS < 1) return 'just now'
  if (ageS < 60) return `${ageS.toFixed(0)}s ago`
  if (ageS < 3600) return `${(ageS / 60).toFixed(1)}min ago`

  return `${(ageS / 3600).toFixed(1)}h ago`
}

interface TableRowProps {
  readonly row: TableStatsItem
}

const TableRow: React.FC<TableRowProps> = ({ row }) => (
  <tr className='border-t border-dark-600'>
    <td className='px-3 py-2 font-mono text-xs text-alpine-900'>{row.table}</td>
    <td className='px-3 py-2 text-xs text-muted-600'>{row.table_kind}</td>
    <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
      {formatCount(row.total)}
    </td>
    <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
      {formatCount(row.current)}
    </td>
    <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
      {formatCount(row.closed)}
    </td>
    <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
      {formatCount(row.archivable)}
    </td>
    <td className='px-3 py-2 text-center text-xs'>
      {row.is_stale ? <span className='text-loss-600'>stale</span> : '—'}
    </td>
  </tr>
)

export const DbStatsCard: React.FC = () => {
  const { data, isLoading, error } = useDbStats()

  const errorMessage = useMemo(() => {
    if (!error) return null
    if (error instanceof Error) return error.message

    return 'Failed to load database statistics.'
  }, [error])

  return (
    <section className='space-y-3 rounded-lg border border-dark-600 bg-white p-4'>
      <header className='flex items-center justify-between'>
        <h3 className='text-lg font-medium text-alpine-900'>Database Statistics</h3>
        {data?.payload && (
          <span className='text-xs text-muted-600'>
            {`sampled ${formatRelativeAge(data.payload.snapshot_completed_at, new Date())}`}
          </span>
        )}
      </header>
      {isLoading && <p className='text-sm text-muted-600'>Loading database stats…</p>}
      {errorMessage !== null && (
        <p className='text-sm text-loss-600'>Database stats unavailable: {errorMessage}</p>
      )}
      {data?.payload && (
        <div className='overflow-x-auto'>
          <table className='w-full border-collapse text-sm'>
            <thead>
              <tr className='bg-alpine-50 text-left text-xs uppercase tracking-wide text-muted-600'>
                <th className='px-3 py-2'>Table</th>
                <th className='px-3 py-2'>Kind</th>
                <th className='px-3 py-2 text-right'>Total</th>
                <th className='px-3 py-2 text-right'>Current</th>
                <th className='px-3 py-2 text-right'>Closed</th>
                <th className='px-3 py-2 text-right'>Archivable</th>
                <th className='px-3 py-2 text-center'>Status</th>
              </tr>
            </thead>
            <tbody>
              {data.payload.tables.map(row => (
                <TableRow key={row.table} row={row} />
              ))}
            </tbody>
          </table>
        </div>
      )}
      {data?.payload && (
        <p className='text-xs text-muted-600'>
          {`Sampler interval: ${data.payload.interval_seconds.toString()}s · ${data.payload.tables.length.toString()} tables`}
        </p>
      )}
    </section>
  )
}
