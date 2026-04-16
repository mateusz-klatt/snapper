import { describe, it, expect } from 'vitest'
import { isUuid7 } from './ids'

describe('isUuid7', () => {
  it('accepts a canonical UUID7 string', () => {
    expect(isUuid7('01948f94-1234-7abc-8def-1234567890ab')).toBe(true)
  })
  it('accepts uppercase hex', () => {
    expect(isUuid7('01948F94-1234-7ABC-8DEF-1234567890AB')).toBe(true)
  })
  it('rejects non-7 version digit', () => {
    expect(isUuid7('01948f94-1234-4abc-8def-1234567890ab')).toBe(false)
  })
  it('rejects bad variant nibble', () => {
    expect(isUuid7('01948f94-1234-7abc-7def-1234567890ab')).toBe(false)
  })
  it('rejects malformed input', () => {
    expect(isUuid7('not-a-uuid')).toBe(false)
  })
  it('rejects the empty string', () => {
    expect(isUuid7('')).toBe(false)
  })
})
