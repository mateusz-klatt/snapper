import React, { useMemo, useState } from 'react'
import { useBacktestRunsByConfigHash, useCreateBacktestComparison } from '../../hooks/queries'
import { useAllTerminalRuns } from './hooks/useAllTerminalRuns'
import type { BacktestCompareBody, BacktestRunData } from '../../types/api'

interface Props {
  currentRun: BacktestRunData
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled'])
const SAME_CONFIG_LIMIT = 20

const formatLabel = (run: BacktestRunData): string =>
  `${run.public_id.slice(0, 8)} · ${run.strategy_name} · ${run.status}`

/**
 * CompareLauncher.
 *
 * Two source modes for the manual combobox: same-config (default) and
 * "all runs" (toggle). Auto-pair fires when at least one same-config
 * sibling exists. Self-compare is blocked client-side. Sources never
 * mix — chip shows which is active.
 */
export const CompareLauncher: React.FC<Props> = ({ currentRun }) => {
  const isTerminal = TERMINAL_STATUSES.has(currentRun.status)
  const configHash = currentRun.config_hash ?? null
  const sameConfigQuery = useBacktestRunsByConfigHash(
    isTerminal ? configHash : null,
    SAME_CONFIG_LIMIT
  )
  const [showAllToggled, setShowAllToggled] = useState(false)
  const showAll = configHash === null ? true : showAllToggled
  const allRunsQuery = useAllTerminalRuns({ enabled: isTerminal && showAll })
  const createCompare = useCreateBacktestComparison()
  const [pickedOther, setPickedOther] = useState<string>('')
  const [clientError, setClientError] = useState<string | null>(null)

  const sameConfigCandidates = useMemo<BacktestRunData[]>(() => {
    const raw = sameConfigQuery.data?.payload ?? []

    return raw.filter(r => TERMINAL_STATUSES.has(r.status) && r.public_id !== currentRun.public_id)
  }, [sameConfigQuery.data, currentRun.public_id])
  const allRunsCandidates = useMemo<BacktestRunData[]>(() => {
    const raw = allRunsQuery.data ?? []

    return raw.filter(r => r.public_id !== currentRun.public_id)
  }, [allRunsQuery.data, currentRun.public_id])
  const comboboxSource = showAll ? allRunsCandidates : sameConfigCandidates
  const comboboxLabel = showAll ? 'all runs' : 'same config'

  if (!isTerminal) {
    return (
      <div className='rounded-lg border border-dark-600 bg-alpine-50 p-3 text-sm text-muted-500'>
        compare available once this run reaches a terminal status
      </div>
    )
  }

  const submit = (mode: 'auto' | 'manual', other: BacktestRunData) => {
    setClientError(null)
    const body: BacktestCompareBody =
      mode === 'auto'
        ? {
            mode: 'auto',
            config_hash: configHash as string,
            anchor_run_public_id: currentRun.public_id,
          }
        : {
            mode: 'manual',
            run_a_public_id: currentRun.public_id,
            run_b_public_id: other.public_id,
          }

    createCompare.mutate(body, {
      onSuccess: response => {
        globalThis.location.hash = `#backtests/compare/${response.payload.public_id}`
      },
    })
  }

  const handleManual = () => {
    const picked = comboboxSource.find(r => r.public_id === pickedOther)

    if (!picked) {
      setClientError('select a run from the list')

      return
    }

    submit('manual', picked)
  }

  return (
    <div className='space-y-3 rounded-lg border border-dark-600 bg-alpine-50 p-3 text-sm'>
      <div className='flex items-center justify-between'>
        <h3 className='font-semibold text-alpine-900'>Compare</h3>
        <span
          className='rounded bg-dark-600 px-2 py-0.5 text-xs text-muted-400'
          data-testid='compare-source-chip'
        >
          {comboboxLabel}
        </span>
      </div>
      {configHash !== null && sameConfigCandidates.length >= 1 && (
        <button
          type='button'
          onClick={() => submit('auto', sameConfigCandidates[0])}
          className='rounded-lg border border-brand-500 px-3 py-1 text-xs font-medium text-brand-500 transition-colors hover:bg-brand-900/20'
          data-testid='compare-auto-pair'
        >
          Compare with most recent
        </button>
      )}
      {configHash !== null && sameConfigCandidates.length === 0 && (
        <div className='text-xs text-muted-500' data-testid='compare-no-siblings'>
          no other runs with this config
        </div>
      )}
      {sameConfigCandidates.length >= SAME_CONFIG_LIMIT && (
        <div className='text-xs text-muted-500'>
          showing {SAME_CONFIG_LIMIT} most recent — use manual pair for older runs
        </div>
      )}
      <div className='flex items-center gap-2'>
        <select
          className='flex-1 rounded-lg border border-dark-600 bg-alpine-50 px-2 py-1 text-xs text-alpine-900'
          value={pickedOther}
          onChange={e => setPickedOther(e.target.value)}
          data-testid='compare-combobox'
        >
          <option value=''>— pick a run to compare —</option>
          {comboboxSource.map(r => (
            <option key={r.public_id} value={r.public_id}>
              {formatLabel(r)}
            </option>
          ))}
        </select>
        <button
          type='button'
          onClick={handleManual}
          className='rounded-lg border border-brand-500 px-3 py-1 text-xs font-medium text-brand-500 transition-colors hover:bg-brand-900/20'
          data-testid='compare-manual-submit'
        >
          Compare
        </button>
      </div>
      {configHash !== null && (
        <label className='flex items-center gap-2 text-xs text-muted-500'>
          <input
            type='checkbox'
            checked={showAllToggled}
            onChange={e => {
              setShowAllToggled(e.target.checked)
              setPickedOther('')
            }}
            data-testid='compare-show-all'
          />
          <span>Show all runs (not just same config)</span>
        </label>
      )}
      {clientError && (
        <div className='text-xs text-loss-400' data-testid='compare-client-error'>
          {clientError}
        </div>
      )}
    </div>
  )
}
