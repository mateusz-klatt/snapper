import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { PairIcon } from './PairIcon'

describe('PairIcon', () => {
  it('renders two crypto icons overlapping', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'crypto', symbol: 'btc' }}
        quote={{ kind: 'crypto', symbol: 'usdt' }}
      />
    )
    const imgs = container.querySelectorAll('img')

    expect(imgs.length).toBe(2)
    expect(imgs[0]?.getAttribute('src')).toContain('btc.svg')
    expect(imgs[1]?.getAttribute('src')).toContain('usdt.svg')
  })

  it('renders crypto base + flag quote', (): void => {
    const { container } = render(
      <PairIcon base={{ kind: 'crypto', symbol: 'btc' }} quote={{ kind: 'flag', country: 'eu' }} />
    )
    const imgs = container.querySelectorAll('img')

    expect(imgs[0]?.getAttribute('src')).toContain('btc.svg')
    expect(imgs[1]?.getAttribute('src')).toContain('eu.svg')
  })

  it('renders flag-flag forex pair', (): void => {
    const { container } = render(
      <PairIcon base={{ kind: 'flag', country: 'eu' }} quote={{ kind: 'flag', country: 'us' }} />
    )
    const imgs = container.querySelectorAll('img')

    expect(imgs[0]?.getAttribute('src')).toContain('eu.svg')
    expect(imgs[1]?.getAttribute('src')).toContain('us.svg')
  })

  it('renders lucide quote spec via SingleAssetIcon', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'crypto', symbol: 'btc' }}
        quote={{ kind: 'lucide', name: 'coins', color: '#888' }}
      />
    )

    expect(container.querySelector('svg')).not.toBeNull()
  })

  it('renders fallback quote spec via SingleAssetIcon', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'crypto', symbol: 'btc' }}
        quote={{ kind: 'fallback', label: 'XYZ' }}
      />
    )

    expect(container.textContent).toContain('XYZ')
  })

  it('describes the pair via aria-label', (): void => {
    const { container } = render(
      <PairIcon base={{ kind: 'crypto', symbol: 'btc' }} quote={{ kind: 'flag', country: 'us' }} />
    )
    const wrap = container.querySelector('span[aria-label]')

    expect(wrap?.getAttribute('aria-label')).toBe('BTC / US')
  })

  it('describes a lucide leg via name', (): void => {
    const { container } = render(
      <PairIcon base={{ kind: 'lucide', name: 'gem' }} quote={{ kind: 'crypto', symbol: 'btc' }} />
    )

    expect(container.querySelector('span[aria-label]')?.getAttribute('aria-label')).toBe(
      'gem / BTC'
    )
  })

  it('describes a fallback leg via label', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'fallback', label: 'AB' }}
        quote={{ kind: 'fallback', label: 'CD' }}
      />
    )

    expect(container.querySelector('span[aria-label]')?.getAttribute('aria-label')).toBe('AB / CD')
  })

  it('respects custom size', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'crypto', symbol: 'btc' }}
        quote={{ kind: 'crypto', symbol: 'eth' }}
        size={40}
      />
    )
    const wrap = container.firstElementChild as HTMLElement

    expect(wrap.style.height).toBe('40px')
  })

  it('respects custom border color', (): void => {
    const { container } = render(
      <PairIcon
        base={{ kind: 'crypto', symbol: 'btc' }}
        quote={{ kind: 'flag', country: 'us' }}
        borderColor='red'
      />
    )
    const baseSpan = container.querySelector('span > span')

    expect((baseSpan as HTMLElement).style.border).toContain('red')
  })
})
