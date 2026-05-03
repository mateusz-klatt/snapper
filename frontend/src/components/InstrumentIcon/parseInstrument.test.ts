import { describe, it, expect } from 'vitest'
import { parseInstrument } from './parseInstrument'

describe('parseInstrument', () => {
  describe('crypto-spot', () => {
    it('parses BTC-USD on kraken', (): void => {
      expect(parseInstrument('BTC-USD', 'kraken')).toEqual({
        base: 'BTC',
        quote: 'USD',
        assetClass: 'crypto-spot',
        underlyingTicker: 'BTC',
      })
    })

    it('parses BTC-EUR on kraken', (): void => {
      expect(parseInstrument('BTC-EUR', 'kraken')).toEqual({
        base: 'BTC',
        quote: 'EUR',
        assetClass: 'crypto-spot',
        underlyingTicker: 'BTC',
      })
    })

    it('parses lowercase symbol by upper-casing', (): void => {
      expect(parseInstrument('eth-usd', 'kraken').base).toBe('ETH')
    })

    it('strips :BTNL suffix from quote', (): void => {
      expect(parseInstrument('AAVE-USD:BTNL', 'kraken').quote).toBe('USD')
    })
  })

  describe('crypto-cross', () => {
    it('parses ETH-BTC as cross-pair', (): void => {
      expect(parseInstrument('ETH-BTC', 'kraken')).toEqual({
        base: 'ETH',
        quote: 'BTC',
        assetClass: 'crypto-cross',
        underlyingTicker: 'ETH',
      })
    })
  })

  describe('crypto-spot with stablecoin quote', () => {
    it('treats BTC-USDT as crypto-spot (stablecoin quote = same class as USD spot)', (): void => {
      expect(parseInstrument('BTC-USDT', 'kraken')).toEqual({
        base: 'BTC',
        quote: 'USDT',
        assetClass: 'crypto-spot',
        underlyingTicker: 'BTC',
      })
    })

    it('treats BTC-USDC as crypto-spot', (): void => {
      expect(parseInstrument('BTC-USDC', 'kraken').assetClass).toBe('crypto-spot')
    })
  })

  describe('crypto-perp', () => {
    it('parses BTC-USD-PERP', (): void => {
      expect(parseInstrument('BTC-USD-PERP', 'kraken_futures')).toEqual({
        base: 'BTC',
        quote: 'USD',
        assetClass: 'crypto-perp',
        underlyingTicker: 'BTC',
      })
    })

    it('parses BTC-USD-PERP-INV (inverse perp)', (): void => {
      expect(parseInstrument('BTC-USD-PERP-INV', 'kraken_futures')).toEqual({
        base: 'BTC',
        quote: 'USD',
        assetClass: 'crypto-perp',
        underlyingTicker: 'BTC',
      })
    })

    it('parses dated future BTC-USD-260925', (): void => {
      expect(parseInstrument('BTC-USD-260925', 'kraken_futures')).toEqual({
        base: 'BTC',
        quote: 'USD',
        assetClass: 'crypto-perp',
        underlyingTicker: 'BTC',
      })
    })

    it('parses inverse dated future BTC-USD-260925-INV', (): void => {
      expect(parseInstrument('BTC-USD-260925-INV', 'kraken_futures').underlyingTicker).toBe('BTC')
    })
  })

  describe('forex', () => {
    it('parses EUR-USD as forex', (): void => {
      expect(parseInstrument('EUR-USD', 'walutomat')).toEqual({
        base: 'EUR',
        quote: 'USD',
        assetClass: 'forex',
        underlyingTicker: 'EURUSD',
      })
    })

    it('parses USD-PLN as forex', (): void => {
      expect(parseInstrument('USD-PLN', 'walutomat')).toEqual({
        base: 'USD',
        quote: 'PLN',
        assetClass: 'forex',
        underlyingTicker: 'USDPLN',
      })
    })
  })

  describe('equity (single asset, no quote)', () => {
    it('parses AAPL as equity', (): void => {
      expect(parseInstrument('AAPL', 'kraken')).toEqual({
        base: 'AAPL',
        quote: null,
        assetClass: 'equity',
        underlyingTicker: null,
      })
    })
  })

  describe('index (single asset)', () => {
    it('parses bare SPX as index', (): void => {
      expect(parseInstrument('SPX', 'polygon')).toEqual({
        base: 'SPX',
        quote: null,
        assetClass: 'index',
        underlyingTicker: 'SPX',
      })
    })
  })

  describe('yield (single asset)', () => {
    it('parses bare US10Y as yield', (): void => {
      expect(parseInstrument('US10Y', 'polygon')).toEqual({
        base: 'US10Y',
        quote: null,
        assetClass: 'yield',
        underlyingTicker: 'US10Y',
      })
    })
  })

  describe('malformed perp/dated inputs fall through to standard parsing', () => {
    it('treats bare -PERP-INV (no base-quote) as standard parse', (): void => {
      expect(parseInstrument('-PERP-INV', 'kraken_futures').assetClass).not.toBe('crypto-perp')
    })

    it('treats bare -PERP (no base-quote) as standard parse', (): void => {
      expect(parseInstrument('-PERP', 'kraken_futures').assetClass).not.toBe('crypto-perp')
    })

    it('treats degenerate -260925 (dash leading, no base before) as fall-through', (): void => {
      expect(parseInstrument('-260925', 'kraken_futures').assetClass).not.toBe('crypto-perp')
    })

    it('treats malformed XYZ-260925 (single-token base, no quote) as cross fall-through', (): void => {
      const result = parseInstrument('XYZ-260925', 'kraken_futures')

      expect(result.assetClass).toBe('crypto-cross')
    })
  })

  describe('kraken_equities futures decoder', () => {
    it('decodes ESM6-CME as SPX-class index future', (): void => {
      expect(parseInstrument('ESM6-CME', 'kraken_equities')).toEqual({
        base: 'ESM6',
        quote: 'CME',
        assetClass: 'index',
        underlyingTicker: 'SPX',
      })
    })

    it('decodes MNQM6-CME as NDX-class index future', (): void => {
      expect(parseInstrument('MNQM6-CME', 'kraken_equities').underlyingTicker).toBe('NDX')
    })

    it('decodes MYMM6-CBOT as DJI-class index future', (): void => {
      expect(parseInstrument('MYMM6-CBOT', 'kraken_equities').underlyingTicker).toBe('DJI')
    })

    it('decodes RTYM6-CME as RUT-class index future', (): void => {
      expect(parseInstrument('RTYM6-CME', 'kraken_equities').underlyingTicker).toBe('RUT')
    })

    it('decodes 10YK6-CBOT as US10Y yield-class', (): void => {
      expect(parseInstrument('10YK6-CBOT', 'kraken_equities')).toEqual({
        base: '10YK6',
        quote: 'CBOT',
        assetClass: 'yield',
        underlyingTicker: 'US10Y',
      })
    })

    it('decodes 2YYK6-CBOT as US2Y yield', (): void => {
      expect(parseInstrument('2YYK6-CBOT', 'kraken_equities').underlyingTicker).toBe('US2Y')
    })

    it('decodes GCM6-COMEX → GOLD underlying (canonical name, registry-resolvable)', (): void => {
      expect(parseInstrument('GCM6-COMEX', 'kraken_equities')).toEqual({
        base: 'GCM6',
        quote: 'COMEX',
        assetClass: 'commodity-future',
        underlyingTicker: 'GOLD',
      })
    })

    it('decodes MGCM6-COMEX → GOLD (Micro Gold)', (): void => {
      expect(parseInstrument('MGCM6-COMEX', 'kraken_equities').underlyingTicker).toBe('GOLD')
    })

    it('decodes CLM6-NYMEX → WTI', (): void => {
      expect(parseInstrument('CLM6-NYMEX', 'kraken_equities').underlyingTicker).toBe('WTI')
    })

    it('decodes BZM6-NYMEX → BRENT', (): void => {
      expect(parseInstrument('BZM6-NYMEX', 'kraken_equities').underlyingTicker).toBe('BRENT')
    })

    it('decodes HGM6-COMEX → COPPER', (): void => {
      expect(parseInstrument('HGM6-COMEX', 'kraken_equities').underlyingTicker).toBe('COPPER')
    })

    it('decodes NGM6-NYMEX → NATGAS', (): void => {
      expect(parseInstrument('NGM6-NYMEX', 'kraken_equities').underlyingTicker).toBe('NATGAS')
    })

    it('decodes ZLM6-CBOT → SOYBEAN_OIL', (): void => {
      expect(parseInstrument('ZLM6-CBOT', 'kraken_equities').underlyingTicker).toBe('SOYBEAN_OIL')
    })

    it('decodes 6EM6-CME → EUR/USD pair (canonical fx legs, not raw prefix/venue)', (): void => {
      expect(parseInstrument('6EM6-CME', 'kraken_equities')).toEqual({
        base: 'EUR',
        quote: 'USD',
        assetClass: 'forex',
        underlyingTicker: 'EURUSD',
      })
    })

    it('decodes 6CM6-CME → USD/CAD pair (CAD/USD inverse maps to USDCAD canonical)', (): void => {
      expect(parseInstrument('6CM6-CME', 'kraken_equities')).toEqual({
        base: 'USD',
        quote: 'CAD',
        assetClass: 'forex',
        underlyingTicker: 'USDCAD',
      })
    })

    it('decodes 6JM6-CME → USD/JPY pair (matches USDJPY underlying)', (): void => {
      expect(parseInstrument('6JM6-CME', 'kraken_equities').underlyingTicker).toBe('USDJPY')
    })

    it('decodes 6AM6-CME → AUD/USD (AUD-base direct quote, AUDUSD canonical)', (): void => {
      const r = parseInstrument('6AM6-CME', 'kraken_equities')

      expect(r.base).toBe('AUD')
      expect(r.quote).toBe('USD')
      expect(r.underlyingTicker).toBe('AUDUSD')
    })

    it('decodes 6SM6-CME → USDCHF', (): void => {
      expect(parseInstrument('6SM6-CME', 'kraken_equities').underlyingTicker).toBe('USDCHF')
    })

    it('decodes E7M6-CME → EURUSD (E-mini EUR/USD half-size)', (): void => {
      expect(parseInstrument('E7M6-CME', 'kraken_equities').underlyingTicker).toBe('EURUSD')
    })

    it('returns unknown for unrecognised kraken_equities prefix', (): void => {
      expect(parseInstrument('ZZZ6-EXOTIC', 'kraken_equities').assetClass).toBe('unknown')
    })

    it('returns unknown for kraken_equities symbol without dash', (): void => {
      expect(parseInstrument('NOTAFUTURE', 'kraken_equities').assetClass).toBe('unknown')
    })
  })
})
