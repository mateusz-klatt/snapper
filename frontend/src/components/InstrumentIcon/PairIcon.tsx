import { CSSProperties } from 'react'
import { SingleAssetIcon } from './SingleAssetIcon'
import type { IconSpec } from './types'

const CIRCLE_FLAGS_CDN = 'https://hatscripts.github.io/circle-flags/flags'
const CRYPTO_ICONS_CDN =
  'https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color'

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
    <span style={containerStyle} aria-label={`${describeIcon(base)} / ${describeIcon(quote)}`}>
      <span style={baseStyle}>{renderCircle(base, size - 4)}</span>
      <span style={quoteStyle}>{renderCircle(quote, size - 4)}</span>
    </span>
  )
}

function renderCircle(spec: IconSpec, innerSize: number): React.ReactElement {
  if (spec.kind === 'crypto') {
    return (
      <img
        src={`${CRYPTO_ICONS_CDN}/${spec.symbol}.svg`}
        alt=''
        width={innerSize}
        height={innerSize}
        style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
      />
    )
  }

  if (spec.kind === 'flag') {
    return (
      <img
        src={`${CIRCLE_FLAGS_CDN}/${spec.country}.svg`}
        alt=''
        width={innerSize}
        height={innerSize}
        style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
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
