import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MarketDataOnlyBadge } from './MarketDataOnlyBadge'

describe('MarketDataOnlyBadge', () => {
  it('renders the Market-data only label', () => {
    render(<MarketDataOnlyBadge />)
    expect(screen.getByTestId('market-data-only-badge')).toBeTruthy()
    expect(screen.getByText('Market-data only')).toBeTruthy()
  })

  it('has a default tooltip explaining the semantics', () => {
    render(<MarketDataOnlyBadge />)
    const badge = screen.getByTestId('market-data-only-badge')

    expect(badge.getAttribute('title')).toMatch(/Observation-only instrument/)
  })

  it('allows overriding the tooltip', () => {
    render(<MarketDataOnlyBadge title='Custom tooltip text' />)
    expect(screen.getByTestId('market-data-only-badge').getAttribute('title')).toBe(
      'Custom tooltip text'
    )
  })

  it('applies small size classes by default', () => {
    render(<MarketDataOnlyBadge />)
    const badge = screen.getByTestId('market-data-only-badge')

    expect(badge.className).toContain('text-[10px]')
  })

  it('applies medium size classes when size=md', () => {
    render(<MarketDataOnlyBadge size='md' />)
    const badge = screen.getByTestId('market-data-only-badge')

    expect(badge.className).toContain('text-xs')
  })

  it('merges caller-provided className', () => {
    render(<MarketDataOnlyBadge className='extra-class-for-test' />)
    const badge = screen.getByTestId('market-data-only-badge')

    expect(badge.className).toContain('extra-class-for-test')
  })
})
