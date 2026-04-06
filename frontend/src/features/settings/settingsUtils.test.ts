import { describe, it, expect } from 'vitest'
import {
  isJsonString,
  isBooleanString,
  parseBooleanString,
  SENSITIVE_PATTERNS,
  isSensitive,
  isEncrypted,
  CATEGORY_COLORS,
  getCategoryColor,
  getMaskedValue,
  SENSITIVE_MASK,
  tokenizeJson,
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

describe('isBooleanString', () => {
  it('returns true for "true"', () => {
    expect(isBooleanString('true')).toBe(true)
  })

  it('returns true for "false"', () => {
    expect(isBooleanString('false')).toBe(true)
  })

  it('is case-insensitive', () => {
    expect(isBooleanString('TRUE')).toBe(true)
    expect(isBooleanString('False')).toBe(true)
  })

  it('trims whitespace', () => {
    expect(isBooleanString('  true  ')).toBe(true)
  })

  it('returns false for empty string', () => {
    expect(isBooleanString('')).toBe(false)
  })

  it('returns false for non-boolean values', () => {
    expect(isBooleanString('1')).toBe(false)
    expect(isBooleanString('yes')).toBe(false)
    expect(isBooleanString('truthy')).toBe(false)
    expect(isBooleanString('{"true":true}')).toBe(false)
  })
})

describe('parseBooleanString', () => {
  it('parses "true" as true', () => {
    expect(parseBooleanString('true')).toBe(true)
  })

  it('parses "false" as false', () => {
    expect(parseBooleanString('false')).toBe(false)
  })

  it('is case-insensitive', () => {
    expect(parseBooleanString('TRUE')).toBe(true)
    expect(parseBooleanString('False')).toBe(false)
  })

  it('trims whitespace', () => {
    expect(parseBooleanString('  true  ')).toBe(true)
  })

  it('returns false for non-true strings', () => {
    expect(parseBooleanString('anything-else')).toBe(false)
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
    expect(getCategoryColor('unknown')).toBe('bg-muted-100 text-muted-600')
  })
})

describe('getMaskedValue', () => {
  it('masks sensitive key value with short mask', () => {
    const result = getMaskedValue('my_api_key', 'secret123')

    expect(result).toBe(SENSITIVE_MASK)
    expect(result).toHaveLength(8)
    expect(result).not.toContain('secret123')
  })

  it('returns value for non-sensitive key', () => {
    expect(getMaskedValue('trading_mode', 'paper')).toBe('paper')
  })

  it('pretty-prints JSON object value', () => {
    expect(getMaskedValue('config', '{"a":1,"b":2}')).toBe(JSON.stringify({ a: 1, b: 2 }, null, 2))
  })

  it('pretty-prints JSON array value', () => {
    expect(getMaskedValue('symbols', '["SPY","QQQ"]')).toBe(JSON.stringify(['SPY', 'QQQ'], null, 2))
  })

  it('returns plain string when value is not JSON', () => {
    expect(getMaskedValue('mode', 'live')).toBe('live')
  })

  it('returns JSON primitive string as-is without formatting', () => {
    expect(getMaskedValue('count', '42')).toBe('42')
  })

  it('returns (empty) for empty value on non-sensitive key', () => {
    expect(getMaskedValue('trading_mode', '')).toBe('(empty)')
  })

  it('does not mask empty value on sensitive key', () => {
    expect(getMaskedValue('api_key', '')).toBe('(empty)')
  })
})

describe('tokenizeJson', () => {
  it('tokenizes object keys and string values', () => {
    const tokens = tokenizeJson('{"name": "test"}')

    expect(tokens).toEqual([
      expect.objectContaining({ type: 'punctuation', value: '{' }),
      expect.objectContaining({ type: 'key', value: '"name"' }),
      expect.objectContaining({ type: 'punctuation', value: ':' }),
      expect.objectContaining({ type: 'whitespace', value: ' ' }),
      expect.objectContaining({ type: 'string', value: '"test"' }),
      expect.objectContaining({ type: 'punctuation', value: '}' }),
    ])
  })

  it('tokenizes numbers', () => {
    const tokens = tokenizeJson('{"count": 42}')
    const numberToken = tokens.find(t => t.type === 'number')

    expect(numberToken).toEqual(expect.objectContaining({ type: 'number', value: '42' }))
  })

  it('tokenizes negative and decimal numbers', () => {
    const tokens = tokenizeJson('{"val": -3.14}')
    const numToken = tokens.find(t => t.type === 'number')

    expect(numToken).toEqual(expect.objectContaining({ type: 'number', value: '-3.14' }))
  })

  it('tokenizes booleans', () => {
    const tokens = tokenizeJson('{"enabled": true, "disabled": false}')
    const boolTokens = tokens.filter(t => t.type === 'boolean')

    expect(boolTokens).toEqual([
      expect.objectContaining({ type: 'boolean', value: 'true' }),
      expect.objectContaining({ type: 'boolean', value: 'false' }),
    ])
  })

  it('tokenizes null', () => {
    const tokens = tokenizeJson('{"value": null}')
    const nullToken = tokens.find(t => t.type === 'null')

    expect(nullToken).toEqual(expect.objectContaining({ type: 'null', value: 'null' }))
  })

  it('tokenizes arrays', () => {
    const tokens = tokenizeJson('[1, 2]')

    expect(tokens[0]).toEqual(expect.objectContaining({ type: 'punctuation', value: '[' }))
    expect(tokens[tokens.length - 1]).toEqual(
      expect.objectContaining({ type: 'punctuation', value: ']' })
    )
  })

  it('returns empty array for empty string', () => {
    expect(tokenizeJson('')).toEqual([])
  })

  it('handles pretty-printed JSON with whitespace tokens', () => {
    const json = JSON.stringify({ a: 1 }, null, 2)
    const tokens = tokenizeJson(json)
    const wsTokens = tokens.filter(t => t.type === 'whitespace')

    expect(wsTokens.length).toBeGreaterThan(0)
  })

  it('skips unrecognized characters', () => {
    const tokens = tokenizeJson('~42')

    expect(tokens).toEqual([expect.objectContaining({ type: 'number', value: '42' })])
  })

  it('handles escaped quotes in strings', () => {
    const tokens = tokenizeJson('{"msg": "say \\"hello\\""}')
    const strToken = tokens.find(t => t.type === 'string')

    expect(strToken?.value).toContain('\\"hello\\"')
  })
})
