import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createCandle } from '@/test/wsMessageFactories'
import { parseWsMessage } from './ws'

describe('parseWsMessage', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })
  it('returns null for unknown message types', () => {
    const result = parseWsMessage({ type: 'unknown_type', foo: 'bar' })

    expect(result).toBeNull()
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('Unknown message type'))
  })
  it('returns null when type is missing', () => {
    const result = parseWsMessage({ foo: 'bar' })

    expect(result).toBeNull()
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('<no type>'))
  })
  it('returns data for valid messages', () => {
    const message = createCandle()
    const result = parseWsMessage(message)

    expect(result).toMatchObject(message)
  })
  it('logs error for known message types with invalid data', () => {
    const result = parseWsMessage({ type: 'bar', invalid: true })

    expect(result).toBeNull()
    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Schema validation FAILED'),
      expect.any(Array)
    )
  })
})
