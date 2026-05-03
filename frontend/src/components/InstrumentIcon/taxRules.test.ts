import { describe, it, expect } from 'vitest'
import {
  isUsdImplicit,
  isStablecoin,
  isFiat,
  USD_EQUIVALENT,
  STABLECOINS,
  FIAT_CURRENCIES,
} from './taxRules'

describe('taxRules', () => {
  describe('USD_EQUIVALENT', () => {
    it('contains only literal USD — stablecoins are deliberately excluded for PL tax visual marker', (): void => {
      expect(USD_EQUIVALENT.has('USD')).toBe(true)
      expect(USD_EQUIVALENT.has('USDT')).toBe(false)
      expect(USD_EQUIVALENT.has('USDC')).toBe(false)
      expect(USD_EQUIVALENT.has('DAI')).toBe(false)
    })
  })

  describe('STABLECOINS', () => {
    it('lists all stablecoins that must always render their own icon', (): void => {
      expect(STABLECOINS.has('USDT')).toBe(true)
      expect(STABLECOINS.has('USDC')).toBe(true)
      expect(STABLECOINS.has('DAI')).toBe(true)
      expect(STABLECOINS.has('PYUSD')).toBe(true)
      expect(STABLECOINS.has('RLUSD')).toBe(true)
      expect(STABLECOINS.has('FDUSD')).toBe(true)
      expect(STABLECOINS.has('TUSD')).toBe(true)
    })

    it('does not include plain USD', (): void => {
      expect(STABLECOINS.has('USD')).toBe(false)
    })
  })

  describe('FIAT_CURRENCIES', () => {
    it('contains common fiat codes used in Snapper', (): void => {
      expect(FIAT_CURRENCIES.has('USD')).toBe(true)
      expect(FIAT_CURRENCIES.has('EUR')).toBe(true)
      expect(FIAT_CURRENCIES.has('PLN')).toBe(true)
      expect(FIAT_CURRENCIES.has('JPY')).toBe(true)
    })

    it('does not include cryptocurrencies', (): void => {
      expect(FIAT_CURRENCIES.has('BTC')).toBe(false)
      expect(FIAT_CURRENCIES.has('USDT')).toBe(false)
    })
  })

  describe('isUsdImplicit', () => {
    it('returns true for plain USD', (): void => {
      expect(isUsdImplicit('USD')).toBe(true)
    })

    it('returns false for stablecoins per PL tax law (crypto↔stablecoin = not tax point)', (): void => {
      expect(isUsdImplicit('USDT')).toBe(false)
      expect(isUsdImplicit('USDC')).toBe(false)
      expect(isUsdImplicit('DAI')).toBe(false)
    })

    it('returns false for non-USD fiat', (): void => {
      expect(isUsdImplicit('EUR')).toBe(false)
      expect(isUsdImplicit('PLN')).toBe(false)
    })

    it('returns false for null quote (single-asset instrument)', (): void => {
      expect(isUsdImplicit(null)).toBe(false)
    })
  })

  describe('isStablecoin', () => {
    it('detects stablecoins', (): void => {
      expect(isStablecoin('USDT')).toBe(true)
      expect(isStablecoin('USDC')).toBe(true)
    })

    it('does not flag fiat USD as stablecoin', (): void => {
      expect(isStablecoin('USD')).toBe(false)
    })
  })

  describe('isFiat', () => {
    it('detects common fiat', (): void => {
      expect(isFiat('USD')).toBe(true)
      expect(isFiat('EUR')).toBe(true)
    })

    it('does not flag stablecoin as fiat', (): void => {
      expect(isFiat('USDT')).toBe(false)
    })

    it('does not flag crypto as fiat', (): void => {
      expect(isFiat('BTC')).toBe(false)
    })
  })
})
