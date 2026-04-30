import React, { useState } from 'react'
import { toast } from 'react-hot-toast'
import { ChevronLeft, Edit3, PowerOff, Save } from 'lucide-react'
import { Button } from '../../components/ui'
import { useAiDelegate, useUpdateAiDelegateCaps } from '../../hooks/queries'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'
import { RevokeConfirmDialog } from './RevokeConfirmDialog'
import type { DelegateCapsBody } from '../../types/api'

export function DelegateDetailView({
  publicId,
  onBack,
}: Readonly<{ publicId: string; onBack: () => void }>): React.ReactElement {
  const detailQuery = useAiDelegate(publicId)
  const readOnly = useIsReadOnly()
  const updateMutation = useUpdateAiDelegateCaps()
  const [editing, setEditing] = useState(false)
  const [revokeOpen, setRevokeOpen] = useState(false)
  const [draft, setDraft] = useState<{
    maxOpenOrders: string
    maxDailyNotionalUsd: string
    maxCancelsPerMinute: string
  } | null>(null)

  const delegate = detailQuery.data?.payload

  if (detailQuery.isLoading) {
    return <div className='p-6 text-muted-500'>Loading delegate…</div>
  }

  if (delegate === undefined) {
    return (
      <div className='p-6 space-y-3'>
        <Button variant='secondary' onClick={onBack}>
          <ChevronLeft className='w-4 h-4 mr-1 inline' />
          Back
        </Button>
        <p className='text-sm text-muted-600'>Delegate not found or unavailable.</p>
      </div>
    )
  }

  const startEditing = (): void => {
    setDraft({
      maxOpenOrders: delegate.caps.max_open_orders?.toString() ?? '',
      maxDailyNotionalUsd: delegate.caps.max_daily_notional_usd?.toString() ?? '',
      maxCancelsPerMinute: delegate.caps.max_cancels_per_minute?.toString() ?? '',
    })
    setEditing(true)
  }

  const cancelEditing = (): void => {
    setDraft(null)
    setEditing(false)
  }

  const submitCapsUpdate = async (activeDraft: NonNullable<typeof draft>): Promise<void> => {
    const parsedOpen = parseNullableInt(activeDraft.maxOpenOrders)
    const parsedDaily = parseNullableInt(activeDraft.maxDailyNotionalUsd)
    const parsedCancels = parseNullableInt(activeDraft.maxCancelsPerMinute)

    if (parsedOpen === 'error' || parsedDaily === 'error' || parsedCancels === 'error') {
      toast.error('Caps must be non-negative integers or empty.')

      return
    }

    const body: { caps: DelegateCapsBody } = {
      caps: {
        max_open_orders: parsedOpen,
        max_daily_notional_usd: parsedDaily,
        max_cancels_per_minute: parsedCancels,
        max_order_quantity_per_instrument: delegate.caps.max_order_quantity_per_instrument,
      },
    }

    try {
      await updateMutation.mutateAsync({ publicId, body })
      toast.success('Caps updated')
      setEditing(false)
      setDraft(null)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update caps')
    }
  }

  return (
    <div className='p-6 space-y-4'>
      <div className='flex items-center justify-between'>
        <Button variant='secondary' onClick={onBack}>
          <ChevronLeft className='w-4 h-4 mr-1 inline' />
          Back to list
        </Button>
        <div className='flex items-center gap-2'>
          {!editing && (
            <Button
              variant='secondary'
              onClick={startEditing}
              disabled={readOnly || !delegate.is_active}
            >
              <Edit3 className='w-4 h-4 mr-1 inline' />
              Update caps
            </Button>
          )}
          {delegate.is_active && (
            <Button variant='danger' onClick={() => setRevokeOpen(true)} disabled={readOnly}>
              <PowerOff className='w-4 h-4 mr-1 inline' />
              Revoke
            </Button>
          )}
        </div>
      </div>

      <section>
        <h1 className='text-2xl font-bold'>{delegate.label}</h1>
        <p className='text-sm text-muted-500 font-mono'>{delegate.username}</p>
      </section>

      <dl className='grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-2 text-sm'>
        <div>
          <dt className='text-muted-500'>Status</dt>
          <dd className={delegate.is_active ? 'text-gain-700' : 'text-muted-600'}>
            {delegate.is_active ? 'Active' : 'Revoked'}
          </dd>
        </div>
        <div>
          <dt className='text-muted-500'>Created at</dt>
          <dd>{new Date(delegate.created_at).toLocaleString()}</dd>
        </div>
        <div>
          <dt className='text-muted-500'>Created by (user public_id)</dt>
          <dd className='font-mono text-xs'>{delegate.created_by_user_public_id}</dd>
        </div>
        <div>
          <dt className='text-muted-500'>Public ID</dt>
          <dd className='font-mono text-xs'>{delegate.public_id}</dd>
        </div>
      </dl>

      <section className='border-t border-dark-600 pt-4'>
        <h2 className='text-sm font-semibold text-muted-800 mb-2'>Trading caps</h2>
        {editing ? (
          draft !== null && (
            <div className='grid grid-cols-1 sm:grid-cols-2 gap-3'>
              <CapEditor
                id='edit-max-open'
                label='Max open orders'
                value={draft.maxOpenOrders}
                onChange={v => setDraft({ ...draft, maxOpenOrders: v })}
              />
              <CapEditor
                id='edit-max-daily'
                label='Max daily notional USD'
                value={draft.maxDailyNotionalUsd}
                onChange={v => setDraft({ ...draft, maxDailyNotionalUsd: v })}
              />
              <CapEditor
                id='edit-max-cancels'
                label='Max cancels per minute'
                value={draft.maxCancelsPerMinute}
                onChange={v => setDraft({ ...draft, maxCancelsPerMinute: v })}
              />
              <div className='col-span-full flex justify-end gap-2'>
                <Button
                  variant='secondary'
                  onClick={cancelEditing}
                  disabled={updateMutation.isPending}
                >
                  Cancel
                </Button>
                <Button
                  variant='primary'
                  onClick={() => submitCapsUpdate(draft)}
                  disabled={readOnly || updateMutation.isPending}
                  loading={updateMutation.isPending}
                >
                  <Save className='w-4 h-4 mr-1 inline' />
                  Save
                </Button>
              </div>
            </div>
          )
        ) : (
          <dl className='grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-2 text-sm'>
            <div>
              <dt className='text-muted-500'>Max open orders</dt>
              <dd>{delegate.caps.max_open_orders ?? '(default)'}</dd>
            </div>
            <div>
              <dt className='text-muted-500'>Max daily notional USD</dt>
              <dd>{delegate.caps.max_daily_notional_usd ?? '(default)'}</dd>
            </div>
            <div>
              <dt className='text-muted-500'>Max cancels per minute</dt>
              <dd>{delegate.caps.max_cancels_per_minute ?? '(default)'}</dd>
            </div>
            <div>
              <dt className='text-muted-500'>Per-instrument max quantity</dt>
              <dd className='font-mono text-xs'>
                {delegate.caps.max_order_quantity_per_instrument
                  ? JSON.stringify(delegate.caps.max_order_quantity_per_instrument)
                  : '(default)'}
              </dd>
            </div>
          </dl>
        )}
      </section>

      <RevokeConfirmDialog
        delegate={delegate}
        open={revokeOpen}
        onClose={() => setRevokeOpen(false)}
      />
    </div>
  )
}

function CapEditor({
  id,
  label,
  value,
  onChange,
}: Readonly<{
  id: string
  label: string
  value: string
  onChange: (v: string) => void
}>): React.ReactElement {
  return (
    <div>
      <label htmlFor={id} className='block text-sm mb-1'>
        {label}
      </label>
      <input
        id={id}
        type='number'
        min={0}
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder='(default)'
        className='w-full px-3 py-2 rounded-lg border border-dark-600 bg-alpine-50'
      />
    </div>
  )
}

function parseNullableInt(raw: string): number | null | 'error' {
  const trimmed = raw.trim()

  if (trimmed === '') return null

  const parsed = Number(trimmed)

  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) return 'error'

  return parsed
}
