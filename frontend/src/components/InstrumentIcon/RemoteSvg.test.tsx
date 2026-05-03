import { describe, it, expect } from 'vitest'
import { fireEvent, render } from '@testing-library/react'
import { RemoteSvg, CIRCLE_FLAGS_CDN, CRYPTO_ICONS_CDN } from './RemoteSvg'

describe('RemoteSvg', () => {
  it('renders an img with the given src and label as alt', () => {
    const { container } = render(
      <RemoteSvg src='https://example.com/btc.svg' label='BTC' size={28} />
    )
    const img = container.querySelector('img')

    expect(img).not.toBeNull()
    expect(img?.getAttribute('src')).toBe('https://example.com/btc.svg')
    expect(img?.getAttribute('alt')).toBe('BTC')
    expect(img?.getAttribute('width')).toBe('28')
    expect(img?.getAttribute('loading')).toBe('lazy')
    expect(img?.getAttribute('decoding')).toBe('async')
  })

  it('falls back to a textual badge when the img errors', () => {
    const { container } = render(
      <RemoteSvg src='https://example.com/missing.svg' label='XYZW' size={32} />
    )
    const img = container.querySelector('img')

    expect(img).not.toBeNull()
    if (!img) throw new Error('img not found')
    fireEvent.error(img)
    const badge = container.querySelector('span[role="img"]')

    expect(badge).not.toBeNull()
    expect(badge?.textContent).toBe('XYZ')
    expect(badge?.getAttribute('aria-label')).toBe('XYZW')
  })

  it('uses square fallback shape when shape="rect"', () => {
    const { container } = render(
      <RemoteSvg src='https://example.com/x.svg' label='AB' size={20} shape='rect' />
    )
    const img = container.querySelector('img')

    if (!img) throw new Error('img not found')
    fireEvent.error(img)
    const badge = container.querySelector('span[role="img"]') as HTMLElement

    expect(badge.style.borderRadius).not.toBe('50%')
  })

  it('honours custom fallback background', () => {
    const { container } = render(
      <RemoteSvg src='https://example.com/x.svg' label='AB' size={20} fallbackBackground='red' />
    )
    const img = container.querySelector('img')

    if (!img) throw new Error('img not found')
    fireEvent.error(img)
    const badge = container.querySelector('span[role="img"]') as HTMLElement

    expect(badge.style.background).toBe('red')
  })

  it('exports the public CDN constants', () => {
    expect(CIRCLE_FLAGS_CDN).toBe('https://hatscripts.github.io/circle-flags/flags')
    expect(CRYPTO_ICONS_CDN).toBe(
      'https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color'
    )
  })
})
