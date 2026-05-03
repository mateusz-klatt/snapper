import { PairIcon } from './PairIcon'
import { SingleAssetIcon } from './SingleAssetIcon'
import { parseInstrument } from './parseInstrument'
import { resolveIcon } from './resolver'
import { isUsdImplicit } from './taxRules'
import type { AssetClass, ParsedInstrument } from './types'

type InstrumentIconProps = {
  readonly symbol: string
  readonly exchange: string
  readonly size?: number
  readonly borderColor?: string
}

export function InstrumentIcon({
  symbol,
  exchange,
  size,
  borderColor,
}: Readonly<InstrumentIconProps>): React.ReactElement {
  const parsed = parseInstrument(symbol, exchange)
  const renderSingle = shouldRenderSingle(parsed)

  if (renderSingle || parsed.quote === null) {
    const singleTicker = parsed.underlyingTicker ?? parsed.base

    return <SingleAssetIcon spec={resolveIcon(singleTicker, parsed.assetClass)} size={size} />
  }

  const baseSpec = resolveIcon(parsed.base, parsed.assetClass)
  const quoteSpec = resolveIcon(parsed.quote, parsed.assetClass)

  return <PairIcon base={baseSpec} quote={quoteSpec} size={size} borderColor={borderColor} />
}

function shouldRenderSingle(parsed: ParsedInstrument): boolean {
  if (isSingleOnlyClass(parsed.assetClass)) {
    return true
  }

  if (parsed.assetClass === 'forex') {
    return false
  }

  return isUsdImplicit(parsed.quote)
}

function isSingleOnlyClass(assetClass: AssetClass): boolean {
  return (
    assetClass === 'equity' ||
    assetClass === 'index' ||
    assetClass === 'commodity-future' ||
    assetClass === 'yield' ||
    assetClass === 'unknown'
  )
}
