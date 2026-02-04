import { describe, it, expect } from 'vitest'
import {
  isJsonString,
  SENSITIVE_PATTERNS,
  isSensitive,
  isEncrypted,
  CATEGORY_COLORS,
  getCategoryColor,
  getMaskedValue,
} from './settingsUtils'

describe('isJsonString', () => {
  it('returns true for valid JSON object', () => {
    expect(isJsonString('{"key": "value"}')).toBe(true)
  })

  it('returns true for valid JSON array', () => {
    expect(isJsonString('[1, 2, 3]')).toBe(true)
  })

  it('returns true for valid JSON primitive', () => {
    expect(isJsonString('"hello"')).toBe(true)
  })

  it('returns false for invalid JSON', () => {
    expect(isJsonString('not json')).toBe(false)
  })

  it('returns false for empty string', () => {
    expect(isJsonString('')).toBe(false)
  })
})

describe('isSensitive', () => {
  it.each(SENSITIVE_PATTERNS)('detects %s as sensitive', (pattern: string) => {
    expect(isSensitive(pattern)).toBe(true)
  })

  it('is case-insensitive', () => {
    expect(isSensitive('MY_API_KEY')).toBe(true)
  })

  it('returns false for non-sensitive key', () => {
    expect(isSensitive('trading_mode')).toBe(false)
  })
})

describe('isEncrypted', () => {
  it('returns true for encrypted value', () => {
    const encrypted = 'gAAAAAB' + 'x'.repeat(40)

    expect(isEncrypted(encrypted)).toBe(true)
  })

  it('returns false for short value with correct prefix', () => {
    expect(isEncrypted('gAAAAABshort')).toBe(false)
  })

  it('returns false for long value without prefix', () => {
    expect(isEncrypted('x'.repeat(50))).toBe(false)
  })
})

describe('getCategoryColor', () => {
  it.each(Object.entries(CATEGORY_COLORS))(
    'returns correct color for %s category',
    (category: string, expected: string) => {
      expect(getCategoryColor(category)).toBe(expected)
    }
  )

  it('returns fallback color for unknown category', () => {
    expect(getCategoryColor('unknown')).toBe('bg-dark-600 text-dark-200')
  })
})

describe('getMaskedValue', () => {
  it('masks sensitive key value', () => {
    const result = getMaskedValue('my_api_key', 'secret123')

    expect(result).toContain('•')
    expect(result).not.toContain('secret123')
  })

  it('returns value for non-sensitive key', () => {
    expect(getMaskedValue('trading_mode', 'paper')).toBe('paper')
  })

  it('returns (empty) for empty value on non-sensitive key', () => {
    expect(getMaskedValue('trading_mode', '')).toBe('(empty)')
  })

  it('does not mask empty value on sensitive key', () => {
    expect(getMaskedValue('api_key', '')).toBe('(empty)')
  })
})
