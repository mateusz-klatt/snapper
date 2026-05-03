import type { AssetClass, IconSpec } from './types'
import { CRYPTO_ICONS } from './registry/crypto'
import { FIAT_FLAGS } from './registry/fiat'
import { UNDERLYING_ICONS } from './registry/underlying'
import { ASSET_CLASS_FALLBACK } from './registry/fallback'

export function resolveIcon(symbol: string, assetClass: AssetClass): IconSpec {
  const upper = symbol.toUpperCase()

  return (
    CRYPTO_ICONS[upper] ??
    FIAT_FLAGS[upper] ??
    UNDERLYING_ICONS[upper] ??
    ASSET_CLASS_FALLBACK[assetClass]
  )
}
