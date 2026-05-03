/**
 * Tax-aware quote-currency classification.
 *
 * Why USDT, USDC, DAI etc. are NOT collapsed into the USD-implicit bucket:
 *
 * Under Polish personal-income-tax practice (Ustawa o PIT, art. 17 ust. 1f
 * pkt 11 — przychody z odpłatnego zbycia waluty wirtualnej), a swap between
 * two virtual currencies (e.g. BTC↔USDT, BTC↔USDC) is treated as
 * non-taxable continuation of the crypto position. A swap between a virtual
 * currency and a fiat currency (BTC↔USD, BTC↔EUR, BTC↔PLN) is the taxable
 * realisation event. See KIS interpretation 0114-KDIP3-1.4011.* lines for
 * the case-law that backs this; rules are similar in several other EU
 * jurisdictions (DE §23 EStG, FR Art. 150 VH bis CGI).
 *
 * The UI consequence is that the operator must be able to tell at a glance
 * whether a position swap moved into fiat (tax event) or stayed in crypto
 * (no tax event). Treating USDT/USDC visually as "USD" (collapsing the
 * quote icon) hides that distinction. The dual icon for stablecoins is
 * therefore a tax-relevance marker, not just a stylistic choice.
 *
 * If you fork this for a different jurisdiction, override the
 * USD_EQUIVALENT / STABLECOINS sets to match local tax-event semantics.
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
