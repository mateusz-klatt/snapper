import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { SingleAssetIcon } from './SingleAssetIcon'

describe('SingleAssetIcon', () => {
  it('renders crypto icon as img with vendored local path', (): void => {
    const { container } = render(
      <SingleAssetIcon spec={{ kind: 'crypto', symbol: 'btc' }} size={32} />
    )
    const img = container.querySelector('img')

    expect(img).not.toBeNull()
    expect(img?.getAttribute('src')).toBe('/icons/crypto/btc.svg')
    expect(img?.getAttribute('alt')).toBe('BTC')
  })

  it('renders flag icon as img with vendored local path', (): void => {
    const { container } = render(
      <SingleAssetIcon spec={{ kind: 'flag', country: 'us' }} size={28} />
    )
    const img = container.querySelector('img')

    expect(img?.getAttribute('src')).toBe('/icons/flags/us.svg')
    expect(img?.getAttribute('alt')).toBe('US')
  })

  it('renders lucide icon as svg element', (): void => {
    const { container } = render(
      <SingleAssetIcon spec={{ kind: 'lucide', name: 'gem', color: '#FFD700' }} size={28} />
    )
    const svg = container.querySelector('svg')

    expect(svg).not.toBeNull()
  })

  it('renders fallback as span with label', (): void => {
    const { container } = render(
      <SingleAssetIcon spec={{ kind: 'fallback', label: 'XYZ' }} size={32} />
    )
    const span = container.querySelector('span')

    expect(span?.textContent).toBe('XYZ')
    expect(span?.getAttribute('aria-label')).toBe('XYZ')
  })

  it('uses default size when not specified', (): void => {
    const { container } = render(<SingleAssetIcon spec={{ kind: 'crypto', symbol: 'eth' }} />)
    const img = container.querySelector('img')

    expect(img?.getAttribute('width')).toBe('28')
  })
})
