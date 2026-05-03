import { describe, it, expect } from 'vitest'
import { resolveIcon } from './resolver'

describe('resolveIcon', () => {
  describe('crypto registry hits', () => {
    it('resolves BTC to crypto icon spec', (): void => {
      const spec = resolveIcon('BTC', 'crypto-spot')

      expect(spec).toEqual({ kind: 'crypto', symbol: 'btc' })
    })

    it('resolves SOL to crypto icon spec', (): void => {
      const spec = resolveIcon('SOL', 'crypto-spot')

      expect(spec).toEqual({ kind: 'crypto', symbol: 'sol' })
    })

    it('resolves USDT to its own crypto icon (not USD flag)', (): void => {
      const spec = resolveIcon('USDT', 'crypto-spot')

      expect(spec).toEqual({ kind: 'crypto', symbol: 'usdt' })
    })

    it('lowercases registry CDN symbol while accepting uppercase ticker input', (): void => {
      const spec = resolveIcon('eth', 'crypto-spot')

      expect(spec).toEqual({ kind: 'crypto', symbol: 'eth' })
    })
  })

  describe('fiat registry hits', () => {
    it('resolves USD to flag spec', (): void => {
      const spec = resolveIcon('USD', 'forex')

      expect(spec).toEqual({ kind: 'flag', country: 'us' })
    })

    it('resolves EUR to flag spec', (): void => {
      const spec = resolveIcon('EUR', 'forex')

      expect(spec).toEqual({ kind: 'flag', country: 'eu' })
    })

    it('resolves PLN to flag spec', (): void => {
      const spec = resolveIcon('PLN', 'forex')

      expect(spec).toEqual({ kind: 'flag', country: 'pl' })
    })
  })

  describe('underlying registry hits', () => {
    it('resolves GOLD to lucide gem with gold color', (): void => {
      const spec = resolveIcon('GOLD', 'commodity-future')

      expect(spec).toEqual({ kind: 'lucide', name: 'gem', color: '#FFD700' })
    })

    it('resolves SPX to lucide trending-up', (): void => {
      const spec = resolveIcon('SPX', 'index')

      expect(spec).toEqual({ kind: 'lucide', name: 'trending-up', color: '#7c3aed' })
    })

    it('resolves US10Y to lucide landmark green', (): void => {
      const spec = resolveIcon('US10Y', 'yield')

      expect(spec).toEqual({ kind: 'lucide', name: 'landmark', color: '#059669' })
    })
  })

  describe('asset-class fallback', () => {
    it('falls back to coins for unknown crypto-spot ticker', (): void => {
      const spec = resolveIcon('UNKNOWNCOIN', 'crypto-spot')

      expect(spec).toEqual({ kind: 'lucide', name: 'coins', color: '#f0883e' })
    })

    it('falls back to building-2 for unknown equity ticker', (): void => {
      const spec = resolveIcon('AAPL', 'equity')

      expect(spec).toEqual({ kind: 'lucide', name: 'building-2', color: '#2563eb' })
    })

    it('falls back to gem for unknown commodity-future', (): void => {
      const spec = resolveIcon('UNKNOWN_COMMODITY', 'commodity-future')

      expect(spec).toEqual({ kind: 'lucide', name: 'gem', color: '#8b949e' })
    })

    it('falls back for unknown asset class', (): void => {
      const spec = resolveIcon('XYZ', 'unknown')

      expect(spec).toEqual({ kind: 'lucide', name: 'coins', color: '#8b949e' })
    })
  })
})
