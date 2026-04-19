import { describe, expect, it } from 'vitest'

import { validateBracketPrices, validateTrailingStopParams } from './validation'

describe('validateBracketPrices', () => {
  it('requires at least one bracket price', () => {
    expect(validateBracketPrices(null, null, 'LONG', 100)).toBe(
      'At least one of SL or TP price is required'
    )
  })

  it('validates long-side bracket relations', () => {
    expect(validateBracketPrices(110, null, 'LONG', 100)).toBe(
      'SL price must be below entry price ($100.00)'
    )
    expect(validateBracketPrices(null, 90, 'LONG', 100)).toBe(
      'TP price must be above entry price ($100.00)'
    )
  })

  it('validates short-side bracket relations', () => {
    expect(validateBracketPrices(90, null, 'SHORT', 100)).toBe(
      'SL price must be above entry price ($100.00)'
    )
    expect(validateBracketPrices(null, 110, 'SHORT', 100)).toBe(
      'TP price must be below entry price ($100.00)'
    )
  })

  it('returns null for valid bracket prices', () => {
    expect(validateBracketPrices(95, 110, 'LONG', 100)).toBeNull()
    expect(validateBracketPrices(105, 90, 'SHORT', 100)).toBeNull()
  })
})

describe('validateTrailingStopParams', () => {
  it('validates trailing stop percentage bounds', () => {
    expect(validateTrailingStopParams(null, null)).toBe('Trailing percentage is required')
    expect(validateTrailingStopParams(0, null)).toBe(
      'Trailing percentage must be between 0 and 100'
    )
  })

  it('validates min lock percentage bounds', () => {
    expect(validateTrailingStopParams(5, 100)).toBe('Min lock percentage must be between 0 and 100')
    expect(validateTrailingStopParams(5, 2)).toBeNull()
  })
})
