/**
 * Instrument icon types — discriminated unions for the dispatcher.
 *
 * Smart-hybrid rules: see `proprietary/memory/feedback_instrument_icon_smart_hybrid.md`.
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
