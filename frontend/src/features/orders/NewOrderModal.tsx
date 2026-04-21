import React, { useState, useEffect, useMemo } from 'react'
import { v7 as uuid7 } from 'uuid'
import { Modal } from '../../components/ui/Modal'
import { ThemeSelect } from '../../components/ThemeSelect'
import { MarketDataOnlyBadge } from '../../components/MarketDataOnlyBadge'
import {
  useExchanges,
  useExchangeInstrumentsDetail,
  useWallets,
  useCreateOrder,
} from '../../hooks/queries'
import { APIError } from '../../lib/apiClient'
import { lookupOrderErrorMessage } from './errorMessages'

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
  const exchangeSelectId = 'new-order-exchange'
  const instrumentSelectId = 'new-order-instrument'
  const sideSelectId = 'new-order-side'
  const orderTypeSelectId = 'new-order-type'
  const quantityInputId = 'new-order-quantity'
  const priceInputId = 'new-order-price'
  const stopPriceInputId = 'new-order-stop-price'
  const modeSelectId = 'new-order-mode'
  const walletSelectId = 'new-order-wallet'
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

  const { data: instruments } = useExchangeInstrumentsDetail(exchange || null)
  const capabilityMap = useMemo(() => {
    const map = new Map<string, boolean>()

    for (const row of instruments?.payload ?? []) {
      map.set(row.symbol, row.can_trade)
    }

    return map
  }, [instruments])
  const selectedIsMarketDataOnly = instrument.length > 0 && capabilityMap.get(instrument) === false

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
      const first = instruments.payload[0]

      setInstrument(first.symbol)
      setInstrumentPublicId(first.symbol)
    }
  }, [instruments, instrument])

  const exchangeOptions = (exchanges?.payload ?? []).map(e => ({ value: e, label: e }))
  const instrumentOptions = (instruments?.payload ?? []).map(row => ({
    value: row.symbol,
    label: row.can_trade ? row.symbol : `${row.symbol} — market-data only`,
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

    const qty = Number.parseFloat(quantity)

    if (Number.isNaN(qty) || qty <= 0) {
      setError('Quantity must be a positive number')

      return
    }

    if (needsPrice && !price) {
      setError('Price is required for this order type')

      return
    }

    if (needsPrice && (Number.isNaN(Number.parseFloat(price)) || Number.parseFloat(price) <= 0)) {
      setError('Price must be a positive number')

      return
    }

    if (needsStopPrice && !stopPrice) {
      setError('Stop price is required for this order type')

      return
    }

    if (
      needsStopPrice &&
      (Number.isNaN(Number.parseFloat(stopPrice)) || Number.parseFloat(stopPrice) <= 0)
    ) {
      setError('Stop price must be a positive number')

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
        quantity: Number.parseFloat(quantity),
        price: needsPrice ? Number.parseFloat(price) : null,
        stop_price: needsStopPrice ? Number.parseFloat(stopPrice) : null,
        wallet_public_id: walletPublicId,
        idempotency_key: publicId,
      },
    }

    try {
      await createOrder.mutateAsync(body)
      handleClose()
    } catch (err) {
      setConfirming(false)

      if (err instanceof APIError) {
        const mapped = lookupOrderErrorMessage(err.details)

        setError(mapped ?? err.message)

        return
      }

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
                <label htmlFor={exchangeSelectId} className='block text-xs text-muted-500 mb-1'>
                  Exchange
                </label>
                <ThemeSelect
                  id={exchangeSelectId}
                  value={exchange}
                  onChange={setExchange}
                  options={exchangeOptions}
                />
              </div>
              <div>
                <label
                  htmlFor={instrumentSelectId}
                  className='flex items-center gap-2 text-xs text-muted-500 mb-1'
                >
                  <span>Instrument</span>
                  {selectedIsMarketDataOnly && <MarketDataOnlyBadge size='sm' />}
                </label>
                <ThemeSelect
                  id={instrumentSelectId}
                  value={instrument}
                  onChange={handleInstrumentChange}
                  options={instrumentOptions}
                />
              </div>
            </div>
            {selectedIsMarketDataOnly && (
              <div className='rounded-lg border border-warning-500/40 bg-warning-500/10 px-3 py-2 text-xs text-warning-600'>
                {lookupOrderErrorMessage({ error_code: 'instrument_market_data_only' })}
              </div>
            )}
            <div className='grid grid-cols-2 gap-4'>
              <div>
                <label htmlFor={sideSelectId} className='block text-xs text-muted-500 mb-1'>
                  Side
                </label>
                <ThemeSelect
                  id={sideSelectId}
                  value={side}
                  onChange={setSide}
                  options={SIDE_OPTIONS}
                />
              </div>
              <div>
                <label htmlFor={orderTypeSelectId} className='block text-xs text-muted-500 mb-1'>
                  Order Type
                </label>
                <ThemeSelect
                  id={orderTypeSelectId}
                  value={orderType}
                  onChange={setOrderType}
                  options={ORDER_TYPE_OPTIONS}
                />
              </div>
            </div>
            <div>
              <label htmlFor={quantityInputId} className='block text-xs text-muted-500 mb-1'>
                Quantity
              </label>
              <input
                id={quantityInputId}
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
                <label htmlFor={priceInputId} className='block text-xs text-muted-500 mb-1'>
                  Price
                </label>
                <input
                  id={priceInputId}
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
                <label htmlFor={stopPriceInputId} className='block text-xs text-muted-500 mb-1'>
                  Stop Price
                </label>
                <input
                  id={stopPriceInputId}
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
                <label htmlFor={modeSelectId} className='block text-xs text-muted-500 mb-1'>
                  Mode
                </label>
                <ThemeSelect
                  id={modeSelectId}
                  value={mode}
                  onChange={setMode}
                  options={MODE_OPTIONS}
                />
              </div>
              <div>
                <label htmlFor={walletSelectId} className='block text-xs text-muted-500 mb-1'>
                  Wallet
                </label>
                <ThemeSelect
                  id={walletSelectId}
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
                disabled={selectedIsMarketDataOnly}
                className='px-4 py-2 text-sm font-medium rounded-lg bg-brand-600 text-white hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors'
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
