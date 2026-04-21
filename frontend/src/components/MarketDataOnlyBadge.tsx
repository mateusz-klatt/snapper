import React from 'react'

interface MarketDataOnlyBadgeProps {
  /**
   * Badge size — ``sm`` is tuned for inline use inside instrument-list rows,
   * ``md`` for page-header context where the badge stands alone next to a
   * title.
   */
  size?: 'sm' | 'md'
  /** Optional tooltip override (defaults to the TradFi-specific message). */
  title?: string
  className?: string
}

const SIZE_CLASSES: Readonly<Record<'sm' | 'md', string>> = {
  sm: 'text-[10px] px-1.5 py-0.5 rounded-sm',
  md: 'text-xs px-2 py-1 rounded-md',
}

const DEFAULT_TITLE =
  'Observation-only instrument. Snapper receives market data but cannot place orders for this (symbol, exchange) pair.'

/**
 * Pill marking an instrument whose ``SymbolExchangeCapability.can_trade=false``.
 *
 * Renders next to native-symbol labels in the MarketData instrument
 * dropdown + next to the page title when the currently-selected
 * instrument is market-data-only (TradFi FCM index futures being the
 * primary driver). Order-entry UI is expected to honour the same
 * capability state by disabling the submit button.
 */
export const MarketDataOnlyBadge: React.FC<Readonly<MarketDataOnlyBadgeProps>> = ({
  size = 'sm',
  title = DEFAULT_TITLE,
  className = '',
}) => (
  <span
    title={title}
    aria-label='Market-data only instrument'
    data-testid='market-data-only-badge'
    className={`inline-flex items-center whitespace-nowrap bg-warning-500/20 text-warning-600 border border-warning-500/40 font-medium ${SIZE_CLASSES[size]} ${className}`}
  >
    Market-data only
  </span>
)
