import type { AssetClass, ParsedInstrument } from './types'
import { isFiat, isStablecoin } from './taxRules'

const PERP_SUFFIX = '-PERP'
const PERP_INV_SUFFIX = '-PERP-INV'
const FUTURE_DATE_RE = /-\d{6}(-INV)?$/

const KNOWN_INDICES = new Set(['SPX', 'NDX', 'DJI', 'RUT', 'EMD', 'NK', 'MNK'])
const KNOWN_YIELDS = new Set(['US2Y', 'US5Y', 'US10Y', 'US30Y'])

const COMMODITY_PREFIXES = new Set([
  'GC',
  'MGC',
  '1OZ',
  'SI',
  'SIL',
  'QI',
  'PL',
  'PA',
  'HG',
  'MHG',
  'QC',
  'CL',
  'MCL',
  'QM',
  'BZ',
  'HO',
  'QH',
  'RB',
  'NG',
  'MNG',
  'QG',
  'ZL',
  'MZL',
  'ZM',
  'MZM',
  'ZR',
  'LBR',
  'GF',
  'HE',
])

const INDEX_PREFIXES = new Set(['ES', 'MES', 'NQ', 'MNQ', 'YM', 'MYM', 'RTY', 'EMD', 'NK', 'MNK'])
const YIELD_PREFIXES = new Set(['10Y', '30Y', '2YY', '5YY', 'ZN', 'ZB', 'ZT', 'ZF'])
const FOREX_PREFIXES = new Set(['6A', '6B', '6C', '6E', '6J', '6L', '6M', '6N', '6S', 'E7', 'J7'])

export function parseInstrument(symbol: string, exchange: string): ParsedInstrument {
  const upper = symbol.toUpperCase()

  if (exchange === 'kraken_equities') {
    return parseKrakenEquitiesSymbol(upper)
  }

  const perpMatch = matchPerpStrip(upper)

  if (perpMatch !== null) {
    return { ...perpMatch, assetClass: 'crypto-perp', underlyingTicker: perpMatch.base }
  }

  const dash = upper.indexOf('-')

  if (dash <= 0) {
    if (KNOWN_INDICES.has(upper)) {
      return { base: upper, quote: null, assetClass: 'index', underlyingTicker: upper }
    }

    if (KNOWN_YIELDS.has(upper)) {
      return { base: upper, quote: null, assetClass: 'yield', underlyingTicker: upper }
    }

    return { base: upper, quote: null, assetClass: 'equity', underlyingTicker: null }
  }

  const base = upper.slice(0, dash)
  const quote = upper.slice(dash + 1).replace(/:.*$/, '')
  const assetClass = classifyPair(base, quote)
  const underlyingTicker = derivePairUnderlying(base, quote, assetClass)

  return { base, quote, assetClass, underlyingTicker }
}

function matchPerpStrip(upper: string): { base: string; quote: string } | null {
  const suffix = pickPerpSuffix(upper)

  if (suffix !== null) {
    return splitFirstDash(upper.slice(0, -suffix.length))
  }

  const futureMatch = FUTURE_DATE_RE.exec(upper)

  if (futureMatch !== null) {
    return splitFirstDash(upper.slice(0, futureMatch.index))
  }

  return null
}

function pickPerpSuffix(upper: string): string | null {
  if (upper.endsWith(PERP_INV_SUFFIX)) {
    return PERP_INV_SUFFIX
  }

  if (upper.endsWith(PERP_SUFFIX)) {
    return PERP_SUFFIX
  }

  return null
}

function splitFirstDash(stripped: string): { base: string; quote: string } | null {
  const dash = stripped.indexOf('-')

  if (dash <= 0) {
    return null
  }

  return { base: stripped.slice(0, dash), quote: stripped.slice(dash + 1) }
}

function classifyPair(base: string, quote: string): AssetClass {
  const baseFiat = isFiat(base)
  const quoteFiat = isFiat(quote)

  if (baseFiat && quoteFiat) {
    return 'forex'
  }

  if (!baseFiat && !quoteFiat && !isStablecoin(quote)) {
    return 'crypto-cross'
  }

  return 'crypto-spot'
}

function derivePairUnderlying(base: string, quote: string, assetClass: AssetClass): string | null {
  if (assetClass === 'forex') {
    return `${base}${quote}`
  }

  return base
}

function parseKrakenEquitiesSymbol(symbol: string): ParsedInstrument {
  const dash = symbol.indexOf('-')

  if (dash <= 0) {
    return { base: symbol, quote: null, assetClass: 'unknown', underlyingTicker: null }
  }

  const contractCode = symbol.slice(0, dash)
  const venue = symbol.slice(dash + 1)
  const prefix = extractAlphaPrefix(contractCode)

  if (FOREX_PREFIXES.has(prefix)) {
    return { base: contractCode, quote: venue, assetClass: 'forex', underlyingTicker: prefix }
  }

  if (YIELD_PREFIXES.has(prefix)) {
    return {
      base: contractCode,
      quote: venue,
      assetClass: 'yield',
      underlyingTicker: yieldUnderlyingFromPrefix(prefix),
    }
  }

  if (INDEX_PREFIXES.has(prefix)) {
    return {
      base: contractCode,
      quote: venue,
      assetClass: 'index',
      underlyingTicker: indexUnderlyingFromPrefix(prefix),
    }
  }

  if (COMMODITY_PREFIXES.has(prefix)) {
    return {
      base: contractCode,
      quote: venue,
      assetClass: 'commodity-future',
      underlyingTicker: prefix,
    }
  }

  return { base: contractCode, quote: venue, assetClass: 'unknown', underlyingTicker: null }
}

function extractAlphaPrefix(code: string): string {
  let i = 0

  while (i < code.length && /[A-Z0-9]/.test(code[i]) && !/^[FGHJKMNQUVXZ]\d/.test(code.slice(i))) {
    i++
  }

  return code.slice(0, i)
}

const YIELD_UNDERLYING: Record<string, string> = {
  '2YY': 'US2Y',
  '5YY': 'US5Y',
  '10Y': 'US10Y',
  '30Y': 'US30Y',
  ZT: 'US2Y',
  ZF: 'US5Y',
  ZN: 'US10Y',
  ZB: 'US30Y',
}

const INDEX_UNDERLYING: Record<string, string> = {
  ES: 'SPX',
  MES: 'SPX',
  NQ: 'NDX',
  MNQ: 'NDX',
  YM: 'DJI',
  MYM: 'DJI',
  RTY: 'RUT',
  EMD: 'EMD',
  NK: 'NK',
  MNK: 'NK',
}

function yieldUnderlyingFromPrefix(prefix: string): string {
  return YIELD_UNDERLYING[prefix]
}

function indexUnderlyingFromPrefix(prefix: string): string {
  return INDEX_UNDERLYING[prefix]
}
