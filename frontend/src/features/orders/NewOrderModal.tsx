import React, { useState, useEffect } from 'react'
import { v7 as uuid7 } from 'uuid'
import { Modal } from '../../components/ui/Modal'
import { ThemeSelect } from '../../components/ThemeSelect'
import {
  useExchanges,
  useExchangeInstruments,
  useWallets,
  useCreateOrder,
} from '../../hooks/queries'

interface NewOrderModalProps {
  open: boolean
  onClose: () => void
}

const SIDE_OPTIONS = [
  { value: 'buy', label: 'Buy' },
  { value: 'sell', label: 'Sell' },
]

const ORDER_TYPE_OPTIONS = [
  { value: 'market', label: 'Market' },
  { value: 'limit', label: 'Limit' },
  { value: 'stop', label: 'Stop' },
  { value: 'stop_limit', label: 'Stop Limit' },
]

const MODE_OPTIONS = [
  { value: 'live', label: 'Live' },
  { value: 'paper', label: 'Paper' },
]

export const NewOrderModal: React.FC<NewOrderModalProps> = ({ open, onClose }) => {
  const { data: exchanges } = useExchanges()
  const { data: walletsResponse } = useWallets()
  const createOrder = useCreateOrder()

  const [exchange, setExchange] = useState('')
  const [instrument, setInstrument] = useState('')
  const [instrumentPublicId, setInstrumentPublicId] = useState('')
  const [side, setSide] = useState('buy')
  const [orderType, setOrderType] = useState('limit')
  const [quantity, setQuantity] = useState('')
  const [price, setPrice] = useState('')
  const [stopPrice, setStopPrice] = useState('')
  const [mode, setMode] = useState('live')
  const [walletPublicId, setWalletPublicId] = useState('')
  const [error, setError] = useState('')
  const [confirming, setConfirming] = useState(false)

  const { data: instruments } = useExchangeInstruments(exchange || null)

  useEffect(() => {
    if (exchanges?.payload && exchanges.payload.length > 0 && !exchange) {
      setExchange(exchanges.payload[0])
    }
  }, [exchanges, exchange])

  const wallets = walletsResponse?.payload

  useEffect(() => {
    if (wallets && wallets.length > 0 && !walletPublicId) {
      setWalletPublicId(wallets[0].public_id)
    }
  }, [wallets, walletPublicId])

  useEffect(() => {
    setInstrument('')
    setInstrumentPublicId('')
  }, [exchange])

  useEffect(() => {
    if (instruments?.payload && instruments.payload.length > 0 && !instrument) {
      const first = instruments.payload[0] as string

      setInstrument(first)
      setInstrumentPublicId(first)
    }
  }, [instruments, instrument])

  const exchangeOptions = (exchanges?.payload ?? []).map(e => ({ value: e, label: e }))
  const instrumentOptions = (instruments?.payload ?? []).map((i: string) => ({
    value: i,
    label: i,
  }))
  const walletOptions = (wallets ?? []).map(w => ({
    value: w.public_id,
    label: `${w.label}${w.is_paper ? ' (paper)' : ''}`,
  }))

  const needsPrice = orderType === 'limit' || orderType === 'stop_limit'
  const needsStopPrice = orderType === 'stop' || orderType === 'stop_limit'

  const handleInstrumentChange = (val: string) => {
    setInstrument(val)
    setInstrumentPublicId(val)
  }

  const handleSubmit = () => {
    setError('')

    if (!exchange || !instrument || !quantity || !walletPublicId) {
      setError('All required fields must be filled')

      return
    }

    if (needsPrice && !price) {
      setError('Price is required for this order type')

      return
    }

    if (needsStopPrice && !stopPrice) {
      setError('Stop price is required for this order type')

      return
    }

    setConfirming(true)
  }

  const handleConfirm = async () => {
    const now = new Date().toISOString()
    const publicId = uuid7()

    const body = {
      type: 'create_order_command',
      session_id: 'ui',
      sequence_id: 0,
      public_id: publicId,
      timestamp: now,
      payload: {
        instrument,
        instrument_public_id: instrumentPublicId,
        exchange,
        mode,
        side,
        order_type: orderType,
        quantity: parseFloat(quantity),
        price: needsPrice ? parseFloat(price) : null,
        stop_price: needsStopPrice ? parseFloat(stopPrice) : null,
        wallet_public_id: walletPublicId,
      },
    }

    try {
      await createOrder.mutateAsync(body)
      handleClose()
    } catch (err) {
      setConfirming(false)
      setError(err instanceof Error ? err.message : 'Order creation failed')
    }
  }

  const handleClose = () => {
    setExchange('')
    setInstrument('')
    setInstrumentPublicId('')
    setSide('buy')
    setOrderType('limit')
    setQuantity('')
    setPrice('')
    setStopPrice('')
    setMode('live')
    setWalletPublicId('')
    setError('')
    setConfirming(false)
    onClose()
  }

  return (
    <Modal open={open} onClose={handleClose}>
      <div className='p-6 max-w-lg w-full'>
        <h3 className='text-lg font-semibold text-alpine-900 mb-4'>
          {confirming ? 'Confirm Order' : 'New Manual Order'}
        </h3>

        {error && (
          <div className='mb-4 rounded-lg bg-loss-900/20 border border-loss-600 px-4 py-2 text-sm text-loss-400'>
            {error}
          </div>
        )}

        {confirming ? (
          <div className='space-y-3'>
            <div className='rounded-xl border border-dark-600 bg-dark-700 p-4 space-y-2 text-sm'>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Instrument</span>
                <span className='text-alpine-900 font-medium'>{instrument}</span>
              </div>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Exchange</span>
                <span className='text-alpine-900'>{exchange}</span>
              </div>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Side</span>
                <span
                  className={
                    side === 'buy' ? 'text-gain-400 font-medium' : 'text-loss-400 font-medium'
                  }
                >
                  {side.toUpperCase()}
                </span>
              </div>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Type</span>
                <span className='text-alpine-900'>{orderType}</span>
              </div>
              <div className='flex justify-between'>
                <span className='text-muted-500'>Quantity</span>
                <span className='text-alpine-900 font-medium'>{quantity}</span>
              </div>
              {needsPrice && (
                <div className='flex justify-between'>
                  <span className='text-muted-500'>Price</span>
                  <span className='text-alpine-900'>${price}</span>
                </div>
              )}
              {needsStopPrice && (
                <div className='flex justify-between'>
                  <span className='text-muted-500'>Stop Price</span>
                  <span className='text-alpine-900'>${stopPrice}</span>
                </div>
              )}
              <div className='flex justify-between'>
                <span className='text-muted-500'>Mode</span>
                <span className='text-alpine-900'>{mode}</span>
              </div>
            </div>
            <div className='flex justify-end gap-3 pt-2'>
              <button
                onClick={() => setConfirming(false)}
                className='px-4 py-2 text-sm text-muted-600 hover:text-alpine-900 transition-colors'
              >
                Back
              </button>
              <button
                onClick={handleConfirm}
                disabled={createOrder.isPending}
                className='px-4 py-2 text-sm font-medium rounded-lg bg-brand-600 text-white hover:bg-brand-500 disabled:opacity-50 transition-colors'
              >
                {createOrder.isPending ? 'Submitting...' : 'Confirm Order'}
              </button>
            </div>
          </div>
        ) : (
          <div className='space-y-4'>
            <div className='grid grid-cols-2 gap-4'>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Exchange</label>
                <ThemeSelect value={exchange} onChange={setExchange} options={exchangeOptions} />
              </div>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Instrument</label>
                <ThemeSelect
                  value={instrument}
                  onChange={handleInstrumentChange}
                  options={instrumentOptions}
                />
              </div>
            </div>
            <div className='grid grid-cols-2 gap-4'>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Side</label>
                <ThemeSelect value={side} onChange={setSide} options={SIDE_OPTIONS} />
              </div>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Order Type</label>
                <ThemeSelect
                  value={orderType}
                  onChange={setOrderType}
                  options={ORDER_TYPE_OPTIONS}
                />
              </div>
            </div>
            <div>
              <label className='block text-xs text-muted-500 mb-1'>Quantity</label>
              <input
                type='number'
                step='any'
                min='0'
                value={quantity}
                onChange={e => setQuantity(e.target.value)}
                className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 focus:border-brand-500 focus:outline-none'
                placeholder='0.00'
              />
            </div>
            {needsPrice && (
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Price</label>
                <input
                  type='number'
                  step='any'
                  min='0'
                  value={price}
                  onChange={e => setPrice(e.target.value)}
                  className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 focus:border-brand-500 focus:outline-none'
                  placeholder='0.00'
                />
              </div>
            )}
            {needsStopPrice && (
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Stop Price</label>
                <input
                  type='number'
                  step='any'
                  min='0'
                  value={stopPrice}
                  onChange={e => setStopPrice(e.target.value)}
                  className='w-full rounded-lg border border-dark-600 bg-dark-700 px-3 py-2 text-sm text-alpine-900 focus:border-brand-500 focus:outline-none'
                  placeholder='0.00'
                />
              </div>
            )}
            <div className='grid grid-cols-2 gap-4'>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Mode</label>
                <ThemeSelect value={mode} onChange={setMode} options={MODE_OPTIONS} />
              </div>
              <div>
                <label className='block text-xs text-muted-500 mb-1'>Wallet</label>
                <ThemeSelect
                  value={walletPublicId}
                  onChange={setWalletPublicId}
                  options={walletOptions}
                />
              </div>
            </div>
            <div className='flex justify-end gap-3 pt-2'>
              <button
                onClick={handleClose}
                className='px-4 py-2 text-sm text-muted-600 hover:text-alpine-900 transition-colors'
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                className='px-4 py-2 text-sm font-medium rounded-lg bg-brand-600 text-white hover:bg-brand-500 transition-colors'
              >
                Review Order
              </button>
            </div>
          </div>
        )}
      </div>
    </Modal>
  )
}
