/**
 * Tax-aware quote-currency classification.
 *
 * PL tax law: crypto ↔ stablecoin = not a tax point; crypto ↔ fiat = tax point.
 * Stablecoins MUST always render their own icon — never collapse to USD-implicit.
 * See `proprietary/memory/feedback_usdt_usdc_never_collapse.md`.
 */

export const USD_EQUIVALENT = new Set<string>(['USD'])

export const STABLECOINS = new Set<string>([
  'USDT',
  'USDC',
  'DAI',
  'PYUSD',
  'RLUSD',
  'FDUSD',
  'TUSD',
])

export const FIAT_CURRENCIES = new Set<string>([
  'USD',
  'EUR',
  'GBP',
  'PLN',
  'JPY',
  'CHF',
  'AUD',
  'CAD',
  'NZD',
  'CZK',
  'HUF',
  'SEK',
  'NOK',
  'DKK',
  'TRY',
  'ILS',
  'CNY',
  'ZAR',
  'HKD',
  'SGD',
  'MXN',
  'BRL',
  'BGN',
  'RON',
])

export function isUsdImplicit(quote: string | null): boolean {
  if (quote === null) {
    return false
  }

  return USD_EQUIVALENT.has(quote)
}

export function isStablecoin(symbol: string): boolean {
  return STABLECOINS.has(symbol)
}

export function isFiat(symbol: string): boolean {
  return FIAT_CURRENCIES.has(symbol)
}
