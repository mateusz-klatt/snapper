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
    const result = parseWsMessage({ type: 'candle', invalid: true })

    expect(result).toBeNull()
    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Schema validation FAILED'),
      expect.any(Array)
    )
  })

  it('parses ai_review.caps_violation frames published by Phase 2 #2', () => {
    const message = {
      type: 'ai_review.caps_violation',
      sequence_id: 1,
      public_id: '019dcab4-ec08-7fb1-b4b3-3d33233b2c68',
      timestamp: '2026-04-26T18:00:00.000Z',
      session_id: 'sess-xyz',
      review_public_id: 'rev-1',
      user_public_id: 'user-1',
      strategy_public_id: 'strat-1',
      wallet_public_id: 'wal-1',
      instrument_public_id: 'inst-1',
      cap_type: 'max_open_orders',
      attempted: 11,
      limit: 10,
      dispatch_version: 3,
    }
    const result = parseWsMessage(message)

    expect(result).not.toBeNull()
    expect(result?.type).toBe('ai_review.caps_violation')
    expect(result).toMatchObject(message)
  })

  it('parses ai_review_decision bus events published by Phase 2 #3', () => {
    const message = {
      type: 'ai_review_decision',
      sequence_id: 7,
      public_id: '019dcab4-ec08-7fb1-b4b3-3d33233b2c69',
      timestamp: '2026-04-26T18:00:01.000Z',
      session_id: 'sess-xyz',
      review_public_id: 'rev-1',
      responding_delegate_public_id: 'del-1',
      decision: 'approve',
      new_status: 'resolved_approved',
      resolution_mode: 'pick_one_primary',
      dispatch_version: 4,
    }
    const result = parseWsMessage(message)

    expect(result).not.toBeNull()
    expect(result?.type).toBe('ai_review_decision')
    expect(result).toMatchObject(message)
  })
})
