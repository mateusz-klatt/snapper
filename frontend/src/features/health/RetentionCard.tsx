import React, { useMemo } from 'react'

import { useRetentionRun } from '../../hooks/queries'
import { formatNumber } from '../../lib/utils'

import type { RetentionPolicyResult } from '../../types/api'

function formatRelativeAge(busTime: string, now: Date): string {
  const ageMs = now.getTime() - new Date(busTime).getTime()
  const ageS = ageMs / 1000

  if (ageS < 1) return 'just now'
  if (ageS < 60) return `${ageS.toFixed(0)}s ago`
  if (ageS < 3600) return `${(ageS / 60).toFixed(1)}min ago`
  if (ageS < 86400) return `${(ageS / 3600).toFixed(1)}h ago`

  return `${(ageS / 86400).toFixed(1)}d ago`
}

interface PolicyRowProps {
  readonly policy: RetentionPolicyResult
}

const PolicyRow: React.FC<PolicyRowProps> = ({ policy }) => {
  const window =
    policy.day_start === null || policy.day_end === null
      ? '—'
      : `${policy.day_start} → ${policy.day_end}`

  return (
    <tr className='border-t border-dark-600'>
      <td className='px-3 py-2 font-mono text-xs text-alpine-900'>{policy.table}</td>
      <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
        {policy.retain_days.toString()}d
      </td>
      <td className='px-3 py-2 font-mono text-xs text-muted-600'>{window}</td>
      <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
        {formatNumber(policy.archived_rows)}
      </td>
      <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
        {formatNumber(policy.purged_rows)}
      </td>
      <td className='px-3 py-2 text-right font-mono text-xs text-alpine-900'>
        {policy.files_written.toString()}
      </td>
      <td className='px-3 py-2 text-xs'>
        {policy.error === null ? (
          <span className='text-gain-600'>ok</span>
        ) : (
          <span className='text-loss-600' title={policy.error}>
            error
          </span>
        )}
      </td>
    </tr>
  )
}

export const RetentionCard: React.FC = () => {
  const { data, isLoading, error } = useRetentionRun()

  const errorMessage = useMemo(() => {
    if (!error) return null
    if (error instanceof Error) return error.message

    return 'Failed to load retention run.'
  }, [error])

  return (
    <section className='space-y-3 rounded-lg border border-dark-600 bg-white p-4'>
      <header className='flex items-center justify-between'>
        <h3 className='text-lg font-medium text-alpine-900'>Retention Scheduler</h3>
        {data?.payload && (
          <span className='text-xs text-muted-600'>
            {`last run ${formatRelativeAge(data.payload.run_completed_at, new Date())}`}
          </span>
        )}
      </header>
      {isLoading && <p className='text-sm text-muted-600'>Loading retention run…</p>}
      {errorMessage !== null && (
        <p className='text-sm text-loss-600'>Retention run unavailable: {errorMessage}</p>
      )}
      {data?.payload && (
        <>
          {data.payload.dry_run && (
            <p className='rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-xs text-warning-700'>
              Dry-run mode: rows are archived but not purged from the DB.
            </p>
          )}
          <div className='overflow-x-auto'>
            <table className='w-full border-collapse text-sm'>
              <thead>
                <tr className='bg-alpine-50 text-left text-xs uppercase tracking-wide text-muted-600'>
                  <th className='px-3 py-2'>Table</th>
                  <th className='px-3 py-2 text-right'>Retain</th>
                  <th className='px-3 py-2'>Archived window</th>
                  <th className='px-3 py-2 text-right'>Archived</th>
                  <th className='px-3 py-2 text-right'>Purged</th>
                  <th className='px-3 py-2 text-right'>Files</th>
                  <th className='px-3 py-2'>Status</th>
                </tr>
              </thead>
              <tbody>
                {data.payload.results.map(row => (
                  <PolicyRow key={row.table} policy={row} />
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  )
}
