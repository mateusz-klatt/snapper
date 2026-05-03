import React, { useState } from 'react'
import clsx from 'clsx'
import { Shield, TrendingDown } from 'lucide-react'
import { usePositions, useTrailingStopForCycle } from '../../hooks/queries'
import { useAppStore } from '../../stores/app'
import { InstrumentIcon } from '../../components/InstrumentIcon'
import { OrderCardSkeleton } from '../../components/Skeleton'
import { EmptyState } from '../../components/ui'
import { AttachBracketModal } from './AttachBracketModal'
import { AttachTrailingStopModal } from './AttachTrailingStopModal'
import type { Position } from '../../types/entities'
import type { TrailingStopByCycleResult } from '../../types/api'

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

const positionIdSuffix = (position: Position): string =>
  `${position.instrument}-${position.exchange}-${position.mode ?? 'live'}`

interface PositionRowProps {
  position: Position
  onAttachBracket: (position: Position) => void
  onAttachTrailingStop: (position: Position) => void
  isTimeTraveling: boolean
}

const TrailingStopBadge: React.FC<{ cyclePublicId: string | undefined }> = ({ cyclePublicId }) => {
  const { data } = useTrailingStopForCycle(cyclePublicId)

  if (!data) return null
  const result = data as TrailingStopByCycleResult

  if (result.type === 'message') return null
  const currentStop = result.payload.current_stop ?? 0

  return (
    <span
      className='rounded-full bg-brand-900/20 px-2 py-1 text-xs font-medium text-brand-400'
      data-testid='trailing-stop-badge'
    >
      {currentStop > 0 ? `TS: $${currentStop.toFixed(2)}` : 'TS: pending'}
    </span>
  )
}

const PositionRow: React.FC<PositionRowProps> = ({
  position,
  onAttachBracket,
  onAttachTrailingStop,
  isTimeTraveling,
}) => {
  const side = getPositionSide(position.quantity)
  const absQuantity = Math.abs(position.quantity)
  const suffix = positionIdSuffix(position)
  const hasCycle = !!position.positionCyclePublicId
  const canAttach = hasCycle && side !== 'FLAT' && !isTimeTraveling

  return (
    <div
      className='rounded-2xl border border-dark-600 bg-alpine-50 p-5 transition-colors hover:border-muted-400'
      data-testid={`position-${suffix}`}
    >
      <div className='mb-3 flex items-center justify-between'>
        <div className='flex items-center space-x-3'>
          <InstrumentIcon symbol={position.instrument} exchange={position.exchange} size={28} />
          <span className='font-semibold text-alpine-900'>{position.instrument}</span>
          <span className='text-sm text-muted-500'>{position.exchange}</span>
          <span
            className={clsx('rounded-full px-2 py-1 text-xs font-medium', getSideBadgeClass(side))}
            data-testid={`position-side-${suffix}`}
          >
            {side}
          </span>
          {canAttach && (
            <TrailingStopBadge cyclePublicId={position.positionCyclePublicId as string} />
          )}
        </div>
        <div className='flex items-center gap-2'>
          {canAttach && (
            <button
              type='button'
              onClick={() => onAttachBracket(position)}
              className='flex items-center gap-1 rounded-lg border border-brand-500 px-2 py-1 text-xs font-medium text-brand-500 transition-colors hover:bg-brand-900/20'
              data-testid={`attach-bracket-${suffix}`}
            >
              <Shield size={12} />
              SL/TP
            </button>
          )}
          {canAttach && (
            <button
              type='button'
              onClick={() => onAttachTrailingStop(position)}
              className='flex items-center gap-1 rounded-lg border border-brand-500 px-2 py-1 text-xs font-medium text-brand-500 transition-colors hover:bg-brand-900/20'
              data-testid={`attach-trailing-stop-${suffix}`}
            >
              <TrendingDown size={12} />
              Trail
            </button>
          )}
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
            data-testid={`position-unrealized-${suffix}`}
          >
            {formatPnl(position.unrealizedPnl)}
          </div>
        </div>
        <div>
          <div className='text-muted-500'>Realized P&amp;L</div>
          <div
            className={clsx('font-mono', getPnlClass(position.realizedPnl))}
            data-testid={`position-realized-${suffix}`}
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
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const [bracketTarget, setBracketTarget] = useState<Position | null>(null)
  const [trailingStopTarget, setTrailingStopTarget] = useState<Position | null>(null)

  const handleAttachBracket = (position: Position) => {
    setBracketTarget(position)
  }

  const handleAttachTrailingStop = (position: Position) => {
    setTrailingStopTarget(position)
  }

  const bracketSide =
    bracketTarget && bracketTarget.quantity > 0 ? ('LONG' as const) : ('SHORT' as const)
  const trailingStopSide =
    trailingStopTarget && trailingStopTarget.quantity > 0 ? ('LONG' as const) : ('SHORT' as const)

  return (
    <div className='space-y-6'>
      {bracketTarget?.positionCyclePublicId && (
        <AttachBracketModal
          open={true}
          onClose={() => setBracketTarget(null)}
          positionCyclePublicId={bracketTarget.positionCyclePublicId}
          instrument={bracketTarget.instrument}
          side={bracketSide}
          averagePrice={bracketTarget.averagePrice}
        />
      )}
      {trailingStopTarget?.positionCyclePublicId && (
        <AttachTrailingStopModal
          open={true}
          onClose={() => setTrailingStopTarget(null)}
          positionCyclePublicId={trailingStopTarget.positionCyclePublicId}
          instrument={trailingStopTarget.instrument}
          side={trailingStopSide}
          averagePrice={trailingStopTarget.averagePrice}
        />
      )}
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
                key={positionIdSuffix(position)}
                position={position}
                onAttachBracket={handleAttachBracket}
                onAttachTrailingStop={handleAttachTrailingStop}
                isTimeTraveling={isTimeTraveling}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
