import type { CSSProperties } from 'react'
import { CIRCLE_FLAGS_CDN, CRYPTO_ICONS_CDN, RemoteSvg, isVendored } from './RemoteSvg'
import { SingleAssetIcon } from './SingleAssetIcon'
import type { IconSpec } from './types'

type PairIconProps = {
  base: IconSpec
  quote: IconSpec
  size?: number
  borderColor?: string
}

export function PairIcon({
  base,
  quote,
  size = 28,
  borderColor = 'var(--background, #fff)',
}: Readonly<PairIconProps>): React.ReactElement {
  const overlap = Math.round(size * 0.55)
  const containerStyle: CSSProperties = {
    position: 'relative',
    display: 'inline-block',
    width: size + overlap,
    height: size,
    flexShrink: 0,
  }
  const baseStyle: CSSProperties = {
    position: 'absolute',
    left: 0,
    top: 0,
    zIndex: 2,
    width: size,
    height: size,
    borderRadius: '50%',
    border: `2px solid ${borderColor}`,
    boxSizing: 'border-box',
    overflow: 'hidden',
  }
  const quoteStyle: CSSProperties = {
    ...baseStyle,
    left: overlap,
    zIndex: 1,
  }

  return (
    <span
      style={containerStyle}
      aria-label={`${describeIcon(base)} / ${describeIcon(quote)}`}
      role='img'
    >
      <span style={baseStyle}>{renderCircle(base, size - 4)}</span>
      <span style={quoteStyle}>{renderCircle(quote, size - 4)}</span>
    </span>
  )
}

function renderCircle(spec: IconSpec, innerSize: number): React.ReactElement {
  if (spec.kind === 'crypto') {
    return (
      <RemoteSvg
        src={`${CRYPTO_ICONS_CDN}/${spec.symbol}.svg`}
        label={spec.symbol.toUpperCase()}
        size={innerSize}
        vendored={isVendored('crypto', spec.symbol)}
      />
    )
  }

  if (spec.kind === 'flag') {
    return (
      <RemoteSvg
        src={`${CIRCLE_FLAGS_CDN}/${spec.country}.svg`}
        label={spec.country.toUpperCase()}
        size={innerSize}
        vendored={isVendored('flag', spec.country)}
      />
    )
  }

  return <SingleAssetIcon spec={spec} size={innerSize} />
}

function describeIcon(spec: IconSpec): string {
  if (spec.kind === 'crypto') {
    return spec.symbol.toUpperCase()
  }

  if (spec.kind === 'flag') {
    return spec.country.toUpperCase()
  }

  if (spec.kind === 'lucide') {
    return spec.name
  }

  return spec.label
}
