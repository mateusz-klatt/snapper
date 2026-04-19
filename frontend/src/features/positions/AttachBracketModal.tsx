import React, { useState } from 'react'
import { Modal } from '../../components/ui/Modal'
import { useCreateBracket } from '../../hooks/queries'
import { validateBracketPrices } from './validation'

interface AttachBracketModalProps {
  open: boolean
  onClose: () => void
  positionCyclePublicId: string
  instrument: string
  side: 'LONG' | 'SHORT'
  averagePrice: number
}

export const AttachBracketModal: React.FC<AttachBracketModalProps> = ({
  open,
  onClose,
  positionCyclePublicId,
  instrument,
  side,
  averagePrice,
}) => {
  const createBracket = useCreateBracket()
  const [slPrice, setSlPrice] = useState('')
  const [tpPrice, setTpPrice] = useState('')
  const [error, setError] = useState('')
  const [confirming, setConfirming] = useState(false)

  const handleClose = () => {
    setSlPrice('')
    setTpPrice('')
    setError('')
    setConfirming(false)
    createBracket.reset()
    onClose()
  }

  const handleSubmit = () => {
    setError('')
    const sl = slPrice ? Number.parseFloat(slPrice) : null
    const tp = tpPrice ? Number.parseFloat(tpPrice) : null
    const validationError = validateBracketPrices(sl, tp, side, averagePrice)

    if (validationError) {
      setError(validationError)

      return
    }

    setConfirming(true)
  }

  const handleConfirm = () => {
    const sl = slPrice ? Number.parseFloat(slPrice) : null
    const tp = tpPrice ? Number.parseFloat(tpPrice) : null

    createBracket.mutate(
      {
        position_cycle_public_id: positionCyclePublicId,
        sl_price: sl,
        tp_price: tp,
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
    <Modal open={open} onClose={handleClose} title={`Attach SL/TP — ${instrument}`} size='sm'>
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
            <label htmlFor='sl-price' className='mb-1 block text-sm text-muted-500'>
              Stop-Loss Price
            </label>
            <input
              id='sl-price'
              type='number'
              step='any'
              value={slPrice}
              onChange={e => setSlPrice(e.target.value)}
              placeholder={
                side === 'LONG'
                  ? `Below $${averagePrice.toFixed(2)}`
                  : `Above $${averagePrice.toFixed(2)}`
              }
              className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 placeholder-muted-500 focus:border-brand-500 focus:outline-none'
              data-testid='sl-price-input'
            />
          </div>
          <div>
            <label htmlFor='tp-price' className='mb-1 block text-sm text-muted-500'>
              Take-Profit Price
            </label>
            <input
              id='tp-price'
              type='number'
              step='any'
              value={tpPrice}
              onChange={e => setTpPrice(e.target.value)}
              placeholder={
                side === 'LONG'
                  ? `Above $${averagePrice.toFixed(2)}`
                  : `Below $${averagePrice.toFixed(2)}`
              }
              className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 placeholder-muted-500 focus:border-brand-500 focus:outline-none'
              data-testid='tp-price-input'
            />
          </div>
          {error ? (
            <div
              className='rounded-lg bg-loss-900/20 p-3 text-sm text-loss-400'
              data-testid='bracket-error'
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
              data-testid='bracket-submit'
            >
              Review
            </button>
          </div>
        </div>
      ) : (
        <div className='space-y-4'>
          <div className='rounded-lg border border-dark-600 bg-dark-700 p-3 text-sm'>
            <div className='font-medium text-alpine-900'>Confirm Bracket</div>
            <div className='mt-2 space-y-1'>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Position</span>
                <span className='text-alpine-900'>
                  {side} {instrument}
                </span>
              </div>
              {slPrice && (
                <div className='flex justify-between'>
                  <span className='text-muted-500'>Stop-Loss</span>
                  <span className='font-mono text-loss-400'>
                    ${Number.parseFloat(slPrice).toFixed(2)}
                  </span>
                </div>
              )}
              {tpPrice && (
                <div className='flex justify-between'>
                  <span className='text-muted-500'>Take-Profit</span>
                  <span className='font-mono text-gain-400'>
                    ${Number.parseFloat(tpPrice).toFixed(2)}
                  </span>
                </div>
              )}
            </div>
          </div>
          <div className='flex justify-end gap-2'>
            <button
              type='button'
              onClick={() => setConfirming(false)}
              disabled={createBracket.isPending}
              className='rounded-lg border border-dark-600 px-4 py-2 text-sm text-muted-500 transition-colors hover:bg-muted-200 disabled:opacity-50'
            >
              Back
            </button>
            <button
              type='button'
              onClick={handleConfirm}
              disabled={createBracket.isPending}
              className='rounded-lg bg-brand-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-brand-500 disabled:opacity-50'
              data-testid='bracket-confirm'
            >
              {createBracket.isPending ? 'Creating…' : 'Confirm'}
            </button>
          </div>
        </div>
      )}
    </Modal>
  )
}
