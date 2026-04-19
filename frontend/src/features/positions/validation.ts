const validateBracketField = (
  value: number | null,
  invalidMessage: string,
  nonPositiveMessage: string
): string | null => {
  if (value !== null && !Number.isFinite(value)) return invalidMessage
  if (value !== null && value <= 0) return nonPositiveMessage

  return null
}

const validateLongBracketRelation = (
  sl: number | null,
  tp: number | null,
  averagePrice: number,
  formattedPrice: string
): string | null => {
  if (sl !== null && sl >= averagePrice) {
    return `SL price must be below entry price (${formattedPrice})`
  }

  if (tp !== null && tp <= averagePrice) {
    return `TP price must be above entry price (${formattedPrice})`
  }

  return null
}

const validateShortBracketRelation = (
  sl: number | null,
  tp: number | null,
  averagePrice: number,
  formattedPrice: string
): string | null => {
  if (sl !== null && sl <= averagePrice) {
    return `SL price must be above entry price (${formattedPrice})`
  }

  if (tp !== null && tp >= averagePrice) {
    return `TP price must be below entry price (${formattedPrice})`
  }

  return null
}

export const validateBracketPrices = (
  sl: number | null,
  tp: number | null,
  side: 'LONG' | 'SHORT',
  averagePrice: number
): string | null => {
  if (sl === null && tp === null) return 'At least one of SL or TP price is required'

  const stopLossError = validateBracketField(
    sl,
    'Invalid stop-loss price',
    'Stop-loss price must be positive'
  )

  if (stopLossError !== null) return stopLossError

  const takeProfitError = validateBracketField(
    tp,
    'Invalid take-profit price',
    'Take-profit price must be positive'
  )

  if (takeProfitError !== null) return takeProfitError

  const fmt = `$${averagePrice.toFixed(2)}`

  if (side === 'LONG') {
    return validateLongBracketRelation(sl, tp, averagePrice, fmt)
  }

  return validateShortBracketRelation(sl, tp, averagePrice, fmt)
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
