import { describe, it, expect, beforeEach } from 'vitest'
import { storeWsTicket, consumeWsTicket } from './wsTicketCache'

describe('wsTicketCache', () => {
  beforeEach(() => {
    storeWsTicket(null)
  })
  describe('storeWsTicket', () => {
    it('stores valid ticket', () => {
      const ticket = { token: 'abc123', exp: 1000 }

      storeWsTicket(ticket)
      const consumed = consumeWsTicket(0)

      expect(consumed).toEqual(ticket)
    })
    it('clears cache when storing null', () => {
      storeWsTicket({ token: 'abc', exp: 1000 })
      storeWsTicket(null)
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
    it('rejects ticket without token', () => {
      storeWsTicket({ token: undefined as unknown as string, exp: 1000 })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
    it('rejects ticket with non-string token', () => {
      storeWsTicket({ token: 123 as unknown as string, exp: 1000 })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
    it('rejects ticket without exp', () => {
      storeWsTicket({ token: 'abc', exp: undefined as unknown as number })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
    it('rejects ticket with non-number exp', () => {
      storeWsTicket({ token: 'abc', exp: 'invalid' as unknown as number })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
  })
  describe('consumeWsTicket', () => {
    it('returns null when no ticket stored', () => {
      const consumed = consumeWsTicket()

      expect(consumed).toBeNull()
    })
    it('returns ticket when not expired', () => {
      const ticket = { token: 'abc123', exp: 2000 }

      storeWsTicket(ticket)
      const consumed = consumeWsTicket(1000 * 1000)

      expect(consumed).toEqual(ticket)
    })
    it('clears ticket after consumption', () => {
      storeWsTicket({ token: 'abc', exp: 2000 })
      consumeWsTicket(0)
      const second = consumeWsTicket(0)

      expect(second).toBeNull()
    })
    it('returns null when ticket expired', () => {
      storeWsTicket({ token: 'abc', exp: 1000 })
      const consumed = consumeWsTicket(1000 * 1000)

      expect(consumed).toBeNull()
    })
    it('returns null when ticket expired by 1ms', () => {
      storeWsTicket({ token: 'abc', exp: 1000 })
      const consumed = consumeWsTicket(1000 * 1000 + 1)

      expect(consumed).toBeNull()
    })
    it('returns ticket when exactly at expiration time - 1ms', () => {
      storeWsTicket({ token: 'abc', exp: 1000 })
      const consumed = consumeWsTicket(1000 * 1000 - 1)

      expect(consumed).not.toBeNull()
    })
    it('clears expired ticket from cache', () => {
      storeWsTicket({ token: 'abc', exp: 1000 })
      consumeWsTicket(2000 * 1000)
      const second = consumeWsTicket(0)

      expect(second).toBeNull()
    })
    it('uses Date.now() when no timestamp provided', () => {
      const futureExp = Math.floor(Date.now() / 1000) + 3600

      storeWsTicket({ token: 'abc', exp: futureExp })
      const consumed = consumeWsTicket()

      expect(consumed).not.toBeNull()
    })
    it('handles non-finite exp timestamp', () => {
      storeWsTicket({ token: 'abc', exp: Infinity })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
    it('handles NaN exp timestamp', () => {
      storeWsTicket({ token: 'abc', exp: Number.NaN })
      const consumed = consumeWsTicket(0)

      expect(consumed).toBeNull()
    })
  })
})
