/**
 * Remote SVG image with onError fallback to a textual badge.
 *
 * Used by `SingleAssetIcon` and `PairIcon` to render crypto/flag icons
 * sourced from public CDNs (see `CRYPTO_ICONS_CDN` and `CIRCLE_FLAGS_CDN`
 * exported below). When the CDN is offline, the symbol is unknown to the
 * CDN, or the network is air-gapped, the fallback `<span>` keeps the row
 * layout intact and surfaces the ticker as text.
 *
 * CDN sources (third-party, MIT-licensed, no API key required):
 *
 * - Crypto: spothq/cryptocurrency-icons (MIT)
 *   https://github.com/spothq/cryptocurrency-icons
 * - Flags : hatscripts/circle-flags (MIT)
 *   https://github.com/HatScripts/circle-flags
 *
 * Both repositories serve raw SVG over GitHub Pages / githubusercontent.com.
 * For self-hosted or production-hardened deployments, mirror the SVG
 * directories under `/public/icons/` and override the CDN URLs with a
 * relative path — this component itself stays unchanged.
 */
import { CSSProperties, useState } from 'react'

export const CIRCLE_FLAGS_CDN = 'https://hatscripts.github.io/circle-flags/flags'
export const CRYPTO_ICONS_CDN =
  'https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color'

type RemoteSvgProps = {
  src: string
  label: string
  size: number
  shape?: 'circle' | 'rect'
  fallbackBackground?: string
}

export function RemoteSvg({
  src,
  label,
  size,
  shape = 'circle',
  fallbackBackground = 'rgba(255,255,255,0.06)',
}: Readonly<RemoteSvgProps>): React.ReactElement {
  const [errored, setErrored] = useState(false)

  if (errored) {
    const fallbackStyle: CSSProperties = {
      width: size,
      height: size,
      borderRadius: shape === 'circle' ? '50%' : 4,
      background: fallbackBackground,
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      fontSize: Math.max(8, Math.round(size / 3.2)),
      fontWeight: 600,
      color: '#888',
      flexShrink: 0,
    }

    return (
      <span style={fallbackStyle} aria-label={label} role='img'>
        {label.slice(0, 3)}
      </span>
    )
  }

  return (
    <img
      src={src}
      alt={label}
      width={size}
      height={size}
      onError={() => setErrored(true)}
      loading='lazy'
      decoding='async'
      style={{
        borderRadius: shape === 'circle' ? '50%' : 4,
        display: 'block',
        objectFit: 'cover',
        flexShrink: 0,
      }}
    />
  )
}
