import React, { useMemo, useState } from 'react'

import { useSystemMetrics } from '../../hooks/queries'
import { formatNumber } from '../../lib/utils'

import type { SystemMetricsData } from '../../types/api'

const BYTES_PER_MB = 1024 * 1024

function formatBytes(bytes: number): string {
  const mb = bytes / BYTES_PER_MB

  if (mb < 1024) return `${mb.toFixed(1)} MB`

  return `${(mb / 1024).toFixed(2)} GB`
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(0)}s`
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)}min`
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`

  return `${(seconds / 86400).toFixed(1)}d`
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—'

  return `${(value * 100).toFixed(1)}%`
}

function formatRelativeAge(busTime: string, now: Date): string {
  const ageMs = now.getTime() - new Date(busTime).getTime()
  const ageS = ageMs / 1000

  if (ageS < 1) return 'just now'
  if (ageS < 60) return `${ageS.toFixed(0)}s ago`
  if (ageS < 3600) return `${(ageS / 60).toFixed(1)}min ago`

  return `${(ageS / 3600).toFixed(1)}h ago`
}

interface MetricCellProps {
  readonly label: string
  readonly value: string
  readonly description?: string
}

const MetricCell: React.FC<MetricCellProps> = ({ label, value, description }) => (
  <div className='flex flex-col rounded-md border border-dark-600 bg-alpine-50 p-3'>
    <span className='text-xs uppercase tracking-wide text-muted-600'>{label}</span>
    <span className='mt-1 text-lg font-semibold text-alpine-900'>{value}</span>
    {description !== undefined && (
      <span className='mt-1 text-xs text-muted-600'>{description}</span>
    )}
  </div>
)

interface ProcessHealthGridProps {
  readonly snapshot: SystemMetricsData
}

const ProcessHealthGrid: React.FC<ProcessHealthGridProps> = ({ snapshot }) => {
  const { process, cpu, memory, asyncio, gc, db_internal, saturation, tracemalloc_active } =
    snapshot
  const memorySaturation = memory.saturation_pct
  const dbPoolDepth =
    db_internal.pool_size === null || db_internal.pool_checked_out === null
      ? '—'
      : `${db_internal.pool_checked_out} / ${db_internal.pool_size}`

  return (
    <div className='grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4'>
      <MetricCell
        label='PID'
        value={process.pid.toString()}
        description={`Status: ${process.status}`}
      />
      <MetricCell label='Uptime' value={formatUptime(process.uptime_seconds)} />
      <MetricCell label='Threads' value={process.num_threads.toString()} />
      <MetricCell label='Open FDs' value={process.num_fds.toString()} />
      <MetricCell
        label='CPU'
        value={`${cpu.process_percent.toFixed(1)}%`}
        description={
          cpu.cgroup_throttled_count !== null && cpu.cgroup_throttled_count > 0
            ? `Throttled ${cpu.cgroup_throttled_count.toString()}×`
            : undefined
        }
      />
      <MetricCell
        label='Memory'
        value={formatBytes(memory.rss_bytes)}
        description={
          memorySaturation === null ? undefined : `Saturation ${formatPercent(memorySaturation)}`
        }
      />
      <MetricCell
        label='Asyncio Tasks'
        value={`${asyncio.active_tasks.toString()} active`}
        description={`+ ${asyncio.pending_tasks.toString()} pending`}
      />
      <MetricCell
        label='DB Pool'
        value={dbPoolDepth}
        description={`Live conns: ${db_internal.aiosqlite_live_connections.toString()}`}
      />
      <MetricCell
        label='GC Gen0/1/2'
        value={`${gc.collections_gen0.toString()} / ${gc.collections_gen1.toString()} / ${gc.collections_gen2.toString()}`}
        description={`Objects: ${formatNumber(gc.current_objects)}`}
      />
      <MetricCell label='Threads %' value={formatPercent(saturation.threads_pct)} />
      <MetricCell label='Network conns' value={process.num_connections.toString()} />
      <MetricCell label='Tracemalloc' value={tracemalloc_active ? 'Active' : 'Idle'} />
    </div>
  )
}

export const SystemMetricsCard: React.FC = () => {
  const { data, isLoading, error } = useSystemMetrics()
  const [debugOpen, setDebugOpen] = useState(false)

  const errorMessage = useMemo(() => {
    if (!error) return null
    if (error instanceof Error) return error.message

    return 'Failed to load process health metrics.'
  }, [error])

  return (
    <section className='space-y-3 rounded-lg border border-dark-600 bg-white p-4'>
      <header className='flex items-center justify-between'>
        <h3 className='text-lg font-medium text-alpine-900'>Process Health</h3>
        {data?.payload && (
          <span className='text-xs text-muted-600'>
            {formatRelativeAge(data.payload.bus_time, new Date())}
          </span>
        )}
      </header>
      {isLoading && <p className='text-sm text-muted-600'>Loading process metrics…</p>}
      {errorMessage !== null && (
        <p className='text-sm text-loss-600'>Process metrics unavailable: {errorMessage}</p>
      )}
      {data?.payload && <ProcessHealthGrid snapshot={data.payload} />}
      {data?.payload && (
        <details
          className='text-xs text-muted-600'
          open={debugOpen}
          onToggle={event => {
            setDebugOpen((event.target as HTMLDetailsElement).open)
          }}
        >
          <summary className='cursor-pointer select-none'>Raw JSON</summary>
          <pre className='mt-2 max-h-64 overflow-auto rounded-md bg-muted-100 p-3 font-mono text-xs'>
            {JSON.stringify(data.payload, null, 2)}
          </pre>
        </details>
      )}
    </section>
  )
}
