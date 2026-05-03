import {
  Building2,
  ChartLine,
  Coins,
  Droplet,
  Flame,
  Fuel,
  Gem,
  Hexagon,
  Landmark,
  Leaf,
  TrendingUp,
  Wheat,
} from 'lucide-react'
import type { CSSProperties } from 'react'
import { CIRCLE_FLAGS_CDN, CRYPTO_ICONS_CDN, RemoteSvg, isVendored } from './RemoteSvg'
import type { IconSpec, LucideName } from './types'

const LUCIDE_MAP: Record<
  LucideName,
  React.ComponentType<{ size?: number; color?: string; strokeWidth?: number }>
> = {
  'building-2': Building2,
  'chart-line': ChartLine,
  coins: Coins,
  droplet: Droplet,
  flame: Flame,
  fuel: Fuel,
  gem: Gem,
  hexagon: Hexagon,
  landmark: Landmark,
  leaf: Leaf,
  'trending-up': TrendingUp,
  wheat: Wheat,
}

type SingleAssetIconProps = {
  spec: IconSpec
  size?: number
}

export function SingleAssetIcon({
  spec,
  size = 28,
}: Readonly<SingleAssetIconProps>): React.ReactElement {
  if (spec.kind === 'crypto') {
    return (
      <RemoteSvg
        src={`${CRYPTO_ICONS_CDN}/${spec.symbol}.svg`}
        label={spec.symbol.toUpperCase()}
        size={size}
        vendored={isVendored('crypto', spec.symbol)}
      />
    )
  }

  if (spec.kind === 'flag') {
    return (
      <RemoteSvg
        src={`${CIRCLE_FLAGS_CDN}/${spec.country}.svg`}
        label={spec.country.toUpperCase()}
        size={size}
        vendored={isVendored('flag', spec.country)}
      />
    )
  }

  if (spec.kind === 'lucide') {
    const Icon = LUCIDE_MAP[spec.name]

    return <Icon size={size} color={spec.color} strokeWidth={1.8} />
  }

  const fallbackStyle: CSSProperties = {
    width: size,
    height: size,
    borderRadius: '50%',
    background: 'rgba(255,255,255,0.06)',
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: Math.max(8, Math.round(size / 3)),
    fontWeight: 600,
    color: '#888',
    flexShrink: 0,
  }

  return (
    <span style={fallbackStyle} aria-label={spec.label} role='img'>
      {spec.label}
    </span>
  )
}
