import React, { useState } from 'react'
import { Modal } from '../../components/ui/Modal'
import { useCreateTrailingStop } from '../../hooks/queries'
import { validateTrailingStopParams } from './validation'

interface AttachTrailingStopModalProps {
  open: boolean
  onClose: () => void
  positionCyclePublicId: string
  instrument: string
  side: 'LONG' | 'SHORT'
  averagePrice: number
}

export const AttachTrailingStopModal: React.FC<AttachTrailingStopModalProps> = ({
  open,
  onClose,
  positionCyclePublicId,
  instrument,
  side,
  averagePrice,
}) => {
  const createTrailingStop = useCreateTrailingStop()
  const [trailingPct, setTrailingPct] = useState('')
  const [minLockPct, setMinLockPct] = useState('')
  const [error, setError] = useState('')
  const [confirming, setConfirming] = useState(false)

  const handleClose = () => {
    setTrailingPct('')
    setMinLockPct('')
    setError('')
    setConfirming(false)
    createTrailingStop.reset()
    onClose()
  }

  const handleSubmit = () => {
    setError('')
    const pct = trailingPct ? Number.parseFloat(trailingPct) : null
    const lock = minLockPct ? Number.parseFloat(minLockPct) : null
    const validationError = validateTrailingStopParams(pct, lock)

    if (validationError) {
      setError(validationError)

      return
    }

    setConfirming(true)
  }

  const handleConfirm = () => {
    const pct = Number.parseFloat(trailingPct)
    const lock = Number.parseFloat(minLockPct || '0')

    createTrailingStop.mutate(
      {
        position_cycle_public_id: positionCyclePublicId,
        trailing_pct: pct,
        min_lock_pct: lock,
      },
      {
        onSuccess: () => {
          handleClose()
        },
        onError: (err: Error) => {
          setError(err.message)
          setConfirming(false)
        },
      }
    )
  }

  const isEditingStep = !confirming

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title={`Attach Trailing Stop — ${instrument}`}
      size='sm'
    >
      {isEditingStep ? (
        <div className='space-y-4'>
          <div className='rounded-lg border border-dark-600 bg-dark-700 p-3 text-sm'>
            <div className='flex justify-between'>
              <span className='text-muted-500'>Position</span>
              <span className='text-alpine-900'>
                {side} {instrument}
              </span>
            </div>
            <div className='mt-1 flex justify-between'>
              <span className='text-muted-500'>Entry Price</span>
              <span className='font-mono text-alpine-900'>${averagePrice.toFixed(2)}</span>
            </div>
          </div>
          <div>
            <label htmlFor='trailing-pct' className='mb-1 block text-sm text-muted-500'>
              Trailing Distance (%)
            </label>
            <input
              id='trailing-pct'
              type='number'
              step='0.1'
              min='0.1'
              max='99'
              value={trailingPct}
              onChange={e => setTrailingPct(e.target.value)}
              placeholder='e.g. 5.0'
              className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 placeholder-muted-500 focus:border-brand-500 focus:outline-none'
              data-testid='trailing-pct-input'
            />
          </div>
          <div>
            <label htmlFor='min-lock-pct' className='mb-1 block text-sm text-muted-500'>
              Min Lock Profit (%) — optional
            </label>
            <input
              id='min-lock-pct'
              type='number'
              step='0.1'
              min='0'
              max='99'
              value={minLockPct}
              onChange={e => setMinLockPct(e.target.value)}
              placeholder='0 = start immediately'
              className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 placeholder-muted-500 focus:border-brand-500 focus:outline-none'
              data-testid='min-lock-pct-input'
            />
          </div>
          {error ? (
            <div
              className='rounded-lg bg-loss-900/20 p-3 text-sm text-loss-400'
              data-testid='trailing-stop-error'
            >
              {error}
            </div>
          ) : null}
          <div className='flex justify-end gap-2'>
            <button
              type='button'
              onClick={handleClose}
              className='rounded-lg border border-dark-600 px-4 py-2 text-sm text-muted-500 transition-colors hover:bg-muted-200'
            >
              Cancel
            </button>
            <button
              type='button'
              onClick={handleSubmit}
              className='rounded-lg bg-brand-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-brand-500'
              data-testid='trailing-stop-submit'
            >
              Review
            </button>
          </div>
        </div>
      ) : (
        <div className='space-y-4'>
          <div className='rounded-lg border border-dark-600 bg-dark-700 p-3 text-sm'>
            <div className='font-medium text-alpine-900'>Confirm Trailing Stop</div>
            <div className='mt-2 space-y-1'>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Position</span>
                <span className='text-alpine-900'>
                  {side} {instrument}
                </span>
              </div>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Trailing Distance</span>
                <span className='font-mono text-brand-400'>
                  {Number.parseFloat(trailingPct).toFixed(1)}%
                </span>
              </div>
              {minLockPct && Number.parseFloat(minLockPct) > 0 ? (
                <div className='flex justify-between'>
                  <span className='text-muted-500'>Min Lock Profit</span>
                  <span className='font-mono text-gain-400'>
                    {Number.parseFloat(minLockPct).toFixed(1)}%
                  </span>
                </div>
              ) : null}
            </div>
          </div>
          {minLockPct && Number.parseFloat(minLockPct) > 0 ? (
            <div className='rounded-lg bg-yellow-900/20 p-3 text-sm text-yellow-400'>
              Trailing stop will not activate until price moves {minLockPct}% in your favor.
            </div>
          ) : null}
          <div className='flex justify-end gap-2'>
            <button
              type='button'
              onClick={() => setConfirming(false)}
              disabled={createTrailingStop.isPending}
              className='rounded-lg border border-dark-600 px-4 py-2 text-sm text-muted-500 transition-colors hover:bg-muted-200 disabled:opacity-50'
            >
              Back
            </button>
            <button
              type='button'
              onClick={handleConfirm}
              disabled={createTrailingStop.isPending}
              className='rounded-lg bg-brand-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-brand-500 disabled:opacity-50'
              data-testid='trailing-stop-confirm'
            >
              {createTrailingStop.isPending ? 'Creating...' : 'Confirm'}
            </button>
          </div>
        </div>
      )}
    </Modal>
  )
}
