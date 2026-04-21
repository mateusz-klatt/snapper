import React, { useState } from 'react'
import { ChevronRight, Info, Loader2, ShieldAlert } from 'lucide-react'
import { LiveOnlyNotice } from '../../components/LiveOnlyNotice'
import { Button, EmptyState } from '../../components/ui'
import { CreateDelegateWizard } from './CreateDelegateWizard'
import { DelegateDetailView } from './DelegateDetailView'
import { useAiDelegates, useFeatureFlags } from '../../hooks/queries'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'
import type { DelegateRead } from '../../types/api'

export function AIIntegration(): React.ReactElement {
  const { isEnabled, isLoading } = useFeatureFlags()

  if (isLoading) return <LoadingShell />

  if (!isEnabled) return <DisabledPanel />

  return <EnabledShell />
}

function LoadingShell(): React.ReactElement {
  return (
    <div className='flex items-center justify-center p-8 text-muted-500'>
      <Loader2 className='w-5 h-5 mr-2 animate-spin' />
      <span>Loading AI integration…</span>
    </div>
  )
}

function DisabledPanel(): React.ReactElement {
  return (
    <div className='p-6'>
      <div className='flex items-start gap-3 p-4 rounded-lg bg-warning-50 border border-warning-200 text-warning-800'>
        <ShieldAlert className='w-5 h-5 shrink-0 mt-0.5' />
        <div>
          <h2 className='font-semibold mb-1'>AI Integration is disabled</h2>
          <p className='text-sm'>
            Enable the feature flag in Settings → Feature Flags to manage AI delegates.
          </p>
        </div>
      </div>
    </div>
  )
}

function EnabledShell(): React.ReactElement {
  const delegatesQuery = useAiDelegates()
  const readOnly = useIsReadOnly()
  const [wizardOpen, setWizardOpen] = useState(false)
  const [detailPublicId, setDetailPublicId] = useState<string | null>(null)

  if (detailPublicId !== null) {
    return <DelegateDetailView publicId={detailPublicId} onBack={() => setDetailPublicId(null)} />
  }

  return (
    <div className='p-6 space-y-4'>
      <LiveOnlyNotice />
      <header className='flex items-center justify-between'>
        <div>
          <h1 className='text-2xl font-bold'>AI Integration</h1>
          <p className='text-sm text-muted-500'>
            Manage AI delegates that connect to Snapper via MCP.
          </p>
        </div>
        <Button variant='primary' onClick={() => setWizardOpen(true)} disabled={readOnly}>
          Create delegate
        </Button>
      </header>

      <DelegateList
        delegates={delegatesQuery.data?.payload ?? []}
        isLoading={delegatesQuery.isLoading}
        onSelect={setDetailPublicId}
      />

      <CreateDelegateWizard open={wizardOpen} onClose={() => setWizardOpen(false)} />
    </div>
  )
}

function DelegateList({
  delegates,
  isLoading,
  onSelect,
}: Readonly<{
  delegates: readonly DelegateRead[]
  isLoading: boolean
  onSelect: (publicId: string) => void
}>): React.ReactElement {
  if (isLoading) {
    return (
      <div className='flex items-center justify-center p-8 text-muted-500'>
        <Loader2 className='w-5 h-5 mr-2 animate-spin' />
        <span>Loading delegates…</span>
      </div>
    )
  }

  if (delegates.length === 0) {
    return (
      <EmptyState
        icon={<Info className='w-5 h-5' />}
        title='No AI delegates yet'
        message='Click “Create delegate” to mint one and generate its MCP config snippet.'
      />
    )
  }

  return (
    <div className='overflow-x-auto border border-dark-600 rounded-lg'>
      <table className='min-w-full text-sm'>
        <thead className='bg-dark-700 text-muted-700 text-left'>
          <tr>
            <th className='px-4 py-2 font-semibold'>Label</th>
            <th className='px-4 py-2 font-semibold'>Username</th>
            <th className='px-4 py-2 font-semibold'>Created</th>
            <th className='px-4 py-2 font-semibold'>Max open orders</th>
            <th className='px-4 py-2 font-semibold'>Max daily USD</th>
            <th className='px-4 py-2 font-semibold'>Status</th>
            <th className='px-4 py-2 font-semibold text-right'>Actions</th>
          </tr>
        </thead>
        <tbody className='divide-y divide-dark-600'>
          {delegates.map(d => (
            <tr key={d.public_id} data-testid={`delegate-row-${d.public_id}`}>
              <td className='px-4 py-2'>{d.label}</td>
              <td className='px-4 py-2 font-mono text-xs'>{d.username}</td>
              <td className='px-4 py-2 text-muted-600'>
                {new Date(d.created_at).toLocaleString()}
              </td>
              <td className='px-4 py-2'>{d.caps.max_open_orders ?? 'default'}</td>
              <td className='px-4 py-2'>{d.caps.max_daily_notional_usd ?? 'default'}</td>
              <td className='px-4 py-2'>
                <span
                  className={
                    d.is_active
                      ? 'inline-flex px-2 py-0.5 rounded-full text-xs bg-gain-50 text-gain-700'
                      : 'inline-flex px-2 py-0.5 rounded-full text-xs bg-dark-700 text-muted-600'
                  }
                >
                  {d.is_active ? 'Active' : 'Revoked'}
                </span>
              </td>
              <td className='px-4 py-2 text-right'>
                <button
                  type='button'
                  onClick={() => onSelect(d.public_id)}
                  className='text-brand-600 hover:text-brand-700 text-sm font-medium inline-flex items-center'
                  aria-label={`View delegate ${d.label}`}
                >
                  Details
                  <ChevronRight className='w-4 h-4' />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
