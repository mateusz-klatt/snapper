import type { AssetClass, IconSpec } from '../types'

export const ASSET_CLASS_FALLBACK: Record<AssetClass, IconSpec> = {
  'crypto-spot': { kind: 'lucide', name: 'coins', color: '#f0883e' },
  'crypto-perp': { kind: 'lucide', name: 'coins', color: '#f0883e' },
  'crypto-cross': { kind: 'lucide', name: 'coins', color: '#f0883e' },
  forex: { kind: 'lucide', name: 'landmark', color: '#58a6ff' },
  equity: { kind: 'lucide', name: 'building-2', color: '#2563eb' },
  index: { kind: 'lucide', name: 'trending-up', color: '#7c3aed' },
  'commodity-future': { kind: 'lucide', name: 'gem', color: '#8b949e' },
  yield: { kind: 'lucide', name: 'landmark', color: '#059669' },
  unknown: { kind: 'lucide', name: 'coins', color: '#8b949e' },
}
