import { describe, it, expect, beforeEach } from 'vitest'
import { formatNumber, getCookie } from './utils'

describe('getCookie', () => {
  beforeEach(() => {
    document.cookie.split(';').forEach(c => {
      const name = c.split('=')[0].trim()

      document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/`
    })
  })
  it('returns empty string for non-existent cookie', () => {
    expect(getCookie('non_existent')).toBe('')
  })
  it('returns cookie value when cookie exists', () => {
    document.cookie = 'csrf_token=abc123; path=/'
    expect(getCookie('csrf_token')).toBe('abc123')
  })
  it('returns correct value when multiple cookies exist', () => {
    document.cookie = 'first=value1; path=/'
    document.cookie = 'csrf_token=abc123; path=/'
    document.cookie = 'last=value3; path=/'
    expect(getCookie('csrf_token')).toBe('abc123')
  })
  it('handles cookies with special characters', () => {
    const encodedValue = encodeURIComponent('special=value;')

    document.cookie = `csrf_token=${encodedValue}; path=/`
    expect(getCookie('csrf_token')).toBe(encodedValue)
  })
  it('returns correct value when cookie name is a prefix of another', () => {
    document.cookie = 'test=value1; path=/'
    document.cookie = 'test_long=value2; path=/'
    expect(getCookie('test')).toBe('value1')
  })
})

describe('formatNumber', () => {
  it('uses en-US grouping regardless of host locale', () => {
    expect(formatNumber(1234567)).toBe('1,234,567')
  })
  it('forwards Intl.NumberFormatOptions for currency-style formatting', () => {
    expect(formatNumber(1234.5, { minimumFractionDigits: 2, maximumFractionDigits: 2 })).toBe(
      '1,234.50'
    )
  })
})
