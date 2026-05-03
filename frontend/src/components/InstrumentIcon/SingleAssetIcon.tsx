import { CSSProperties } from 'react'
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
import type { IconSpec, LucideName } from './types'

const CIRCLE_FLAGS_CDN = 'https://hatscripts.github.io/circle-flags/flags'
const CRYPTO_ICONS_CDN =
  'https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color'

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

export function SingleAssetIcon({ spec, size = 28 }: SingleAssetIconProps): React.ReactElement {
  if (spec.kind === 'crypto') {
    return (
      <img
        src={`${CRYPTO_ICONS_CDN}/${spec.symbol}.svg`}
        alt={spec.symbol.toUpperCase()}
        width={size}
        height={size}
        style={{ borderRadius: '50%', display: 'block', flexShrink: 0 }}
      />
    )
  }

  if (spec.kind === 'flag') {
    return (
      <img
        src={`${CIRCLE_FLAGS_CDN}/${spec.country}.svg`}
        alt={spec.country.toUpperCase()}
        width={size}
        height={size}
        style={{ borderRadius: '50%', display: 'block', objectFit: 'cover', flexShrink: 0 }}
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
    <span style={fallbackStyle} aria-label={spec.label}>
      {spec.label}
    </span>
  )
}
