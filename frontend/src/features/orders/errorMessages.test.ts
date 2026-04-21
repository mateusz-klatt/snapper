import { describe, it, expect } from 'vitest'
import { ORDER_ERROR_MESSAGES, extractErrorCode, lookupOrderErrorMessage } from './errorMessages'

describe('extractErrorCode', () => {
  it('returns null for null', () => {
    expect(extractErrorCode(null)).toBeNull()
  })

  it('returns null for primitives', () => {
    expect(extractErrorCode('string')).toBeNull()
    expect(extractErrorCode(42)).toBeNull()
    expect(extractErrorCode(undefined)).toBeNull()
  })

  it('returns null for an object missing error_code', () => {
    expect(extractErrorCode({ message: 'x' })).toBeNull()
  })

  it('returns null when error_code is not a string', () => {
    expect(extractErrorCode({ error_code: 42 })).toBeNull()
  })

  it('returns the error_code string for a well-formed detail', () => {
    expect(extractErrorCode({ error_code: 'instrument_market_data_only' })).toBe(
      'instrument_market_data_only'
    )
  })
})

describe('lookupOrderErrorMessage', () => {
  it('maps the TradFi code to the user-facing string', () => {
    expect(lookupOrderErrorMessage({ error_code: 'instrument_market_data_only' })).toBe(
      ORDER_ERROR_MESSAGES.instrument_market_data_only
    )
  })

  it('maps the unknown_instrument code', () => {
    expect(lookupOrderErrorMessage({ error_code: 'unknown_instrument' })).toBe(
      ORDER_ERROR_MESSAGES.unknown_instrument
    )
  })

  it('returns null for unknown codes', () => {
    expect(lookupOrderErrorMessage({ error_code: 'no_such_code' })).toBeNull()
  })

  it('returns null for missing details', () => {
    expect(lookupOrderErrorMessage(null)).toBeNull()
    expect(lookupOrderErrorMessage(undefined)).toBeNull()
  })

  it('exposes a caps_violation fallback for cap-rejection paths', () => {
    expect(lookupOrderErrorMessage({ error_code: 'caps_violation' })).toBe(
      ORDER_ERROR_MESSAGES.caps_violation
    )
  })
})
