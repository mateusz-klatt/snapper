export const validateBracketPrices = (
  sl: number | null,
  tp: number | null,
  side: 'LONG' | 'SHORT',
  averagePrice: number
): string | null => {
  if (sl === null && tp === null) return 'At least one of SL or TP price is required'
  if (sl !== null && !Number.isFinite(sl)) return 'Invalid stop-loss price'
  if (tp !== null && !Number.isFinite(tp)) return 'Invalid take-profit price'
  if (sl !== null && sl <= 0) return 'Stop-loss price must be positive'
  if (tp !== null && tp <= 0) return 'Take-profit price must be positive'

  const fmt = `$${averagePrice.toFixed(2)}`

  if (side === 'LONG') {
    if (sl !== null && sl >= averagePrice) return `SL price must be below entry price (${fmt})`
    if (tp !== null && tp <= averagePrice) return `TP price must be above entry price (${fmt})`
  } else {
    if (sl !== null && sl <= averagePrice) return `SL price must be above entry price (${fmt})`
    if (tp !== null && tp >= averagePrice) return `TP price must be below entry price (${fmt})`
  }

  return null
}

export const validateTrailingStopParams = (
  trailingPct: number | null,
  minLockPct: number | null
): string | null => {
  if (trailingPct === null) return 'Trailing percentage is required'
  if (!Number.isFinite(trailingPct)) return 'Invalid trailing percentage'
  if (trailingPct <= 0 || trailingPct >= 100) return 'Trailing percentage must be between 0 and 100'

  if (minLockPct !== null) {
    if (!Number.isFinite(minLockPct)) return 'Invalid min lock percentage'
    if (minLockPct < 0 || minLockPct >= 100) return 'Min lock percentage must be between 0 and 100'
  }

  return null
}
