/**
 * Instrument icon types — discriminated unions for the dispatcher.
 *
 * Smart-hybrid dispatch rules (encoded in `InstrumentIcon.tsx`):
 *
 * 1. Single-only classes (equity, index, commodity-future, yield, unknown):
 *    one icon, no quote-currency dimension to render.
 * 2. USD-implicit classes (crypto-spot, crypto-perp where quote === "USD"):
 *    render base only, suppress redundant USD flag in dense tables.
 * 3. Always-dual classes (forex regardless of leg, crypto-spot with
 *    non-USD/stablecoin quote, crypto-cross): render base + quote together
 *    because the quote carries information.
 *
 * Stablecoin handling is delegated to `taxRules.ts` and intentionally
 * never collapses USDT/USDC/DAI/PYUSD/RLUSD into the USD bucket — see the
 * comment block in that file for the legal/product rationale.
 */

export type AssetClass =
  | 'crypto-spot'
  | 'crypto-perp'
  | 'crypto-cross'
  | 'forex'
  | 'equity'
  | 'index'
  | 'commodity-future'
  | 'yield'
  | 'unknown'

export type CryptoIconSpec = {
  readonly kind: 'crypto'
  readonly symbol: string
}

export type FlagIconSpec = {
  readonly kind: 'flag'
  readonly country: string
}

export type LucideIconSpec = {
  readonly kind: 'lucide'
  readonly name: LucideName
  readonly color?: string
}

type FallbackIconSpec = {
  readonly kind: 'fallback'
  readonly label: string
}

export type IconSpec = CryptoIconSpec | FlagIconSpec | LucideIconSpec | FallbackIconSpec

export type LucideName =
  | 'building-2'
  | 'chart-line'
  | 'coins'
  | 'droplet'
  | 'flame'
  | 'fuel'
  | 'gem'
  | 'hexagon'
  | 'landmark'
  | 'leaf'
  | 'trending-up'
  | 'wheat'

export type ParsedInstrument = {
  readonly base: string
  readonly quote: string | null
  readonly assetClass: AssetClass
  readonly underlyingTicker: string | null
}
