import React from 'react'
import clsx from 'clsx'
import { usePositions } from '../../hooks/queries'
import { OrderCardSkeleton } from '../../components/Skeleton'
import { EmptyState } from '../../components/ui'
import type { Position } from '../../types/entities'

type PositionSide = 'LONG' | 'SHORT' | 'FLAT'

const getPositionSide = (quantity: number): PositionSide => {
  if (quantity > 0) return 'LONG'
  if (quantity < 0) return 'SHORT'

  return 'FLAT'
}

const getSideBadgeClass = (side: PositionSide): string => {
  switch (side) {
    case 'LONG':
      return 'text-gain-400 bg-gain-900/20'
    case 'SHORT':
      return 'text-loss-400 bg-loss-900/20'
    case 'FLAT':
    default:
      return 'text-muted-400 bg-muted-900/20'
  }
}

const getPnlClass = (value: number): string => {
  if (value > 0) return 'text-gain-400'
  if (value < 0) return 'text-loss-400'

  return 'text-muted-400'
}

const formatPrice = (value: number): string => `$${value.toFixed(2)}`

const formatPnl = (value: number): string => {
  const abs = Math.abs(value).toFixed(2)

  if (value > 0) return `+$${abs}`
  if (value < 0) return `-$${abs}`

  return `$${abs}`
}

const PositionRow: React.FC<{ position: Position }> = ({ position }) => {
  const side = getPositionSide(position.quantity)
  const absQuantity = Math.abs(position.quantity)

  return (
    <div
      className='rounded-2xl border border-dark-600 bg-alpine-50 p-5 transition-colors hover:border-muted-400'
      data-testid={`position-${position.instrument}-${position.exchange}`}
    >
      <div className='mb-3 flex items-center justify-between'>
        <div className='flex items-center space-x-3'>
          <span className='font-semibold text-alpine-900'>{position.instrument}</span>
          <span className='text-sm text-muted-500'>{position.exchange}</span>
          <span
            className={clsx('rounded-full px-2 py-1 text-xs font-medium', getSideBadgeClass(side))}
            data-testid={`position-side-${position.instrument}`}
          >
            {side}
          </span>
        </div>
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm md:grid-cols-4'>
        <div>
          <div className='text-muted-500'>Quantity</div>
          <div className='font-mono text-alpine-900'>{absQuantity.toFixed(4)}</div>
        </div>
        <div>
          <div className='text-muted-500'>Avg Entry</div>
          <div className='font-mono text-alpine-900'>{formatPrice(position.averagePrice)}</div>
        </div>
        <div>
          <div className='text-muted-500'>Unrealized P&amp;L</div>
          <div
            className={clsx('font-mono', getPnlClass(position.unrealizedPnl))}
            data-testid={`position-unrealized-${position.instrument}`}
          >
            {formatPnl(position.unrealizedPnl)}
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Realized P&amp;L</div>
          <div
            className={clsx('font-mono', getPnlClass(position.realizedPnl))}
            data-testid={`position-realized-${position.instrument}`}
          >
            {formatPnl(position.realizedPnl)}
          </div>
        </div>
      </div>
      {position.timestamp && (
        <div className='mt-3 text-xs text-muted-500'>
          Updated {position.timestamp.toLocaleString()}
        </div>
      )}
    </div>
  )
}

export const Positions: React.FC = () => {
  const { data: positions = [], isLoading } = usePositions()

  return (
    <div className='space-y-6'>
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-semibold text-alpine-900'>Positions</h2>
      </div>
      <div className='space-y-4'>
        {isLoading && (
          <div className='space-y-3'>
            <OrderCardSkeleton />
            <OrderCardSkeleton />
            <OrderCardSkeleton />
          </div>
        )}
        {!isLoading && positions.length === 0 && (
          <EmptyState
            icon={
              <svg className='h-6 w-6' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                <path
                  strokeLinecap='round'
                  strokeLinejoin='round'
                  strokeWidth={2}
                  d='M3 12l3-3 4 4 5-5 4 4'
                />
              </svg>
            }
            title='No open positions'
            message='Long, short, and flat positions will appear here once trades are executed.'
          />
        )}
        {!isLoading && positions.length > 0 && (
          <div className='grid gap-4'>
            {positions.map((position: Position) => (
              <PositionRow
                key={`${position.instrument}-${position.exchange}`}
                position={position}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
