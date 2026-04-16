import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { TradesDiffList } from './TradesDiffList'
import type { TradeDiffEntry } from '../../../types/api'

const trade = (overrides: Partial<TradeDiffEntry>): TradeDiffEntry =>
  ({
    instrument: 'BTC-USD',
    executed_at: '2026-01-01T00:00:00Z',
    side: 'buy',
    quantity: 1,
    price: 100,
    leg: 'common',
    pnl_a: 5,
    pnl_b: 4,
    pnl_delta: -1,
    ...overrides,
  }) as TradeDiffEntry

describe('TradesDiffList', () => {
  it('renders three columns each with empty marker when entries are empty', () => {
    render(<TradesDiffList entries={[]} />)
    expect(screen.getByTestId('trades-diff-common').textContent).toContain('Common (0)')
    expect(screen.getByTestId('trades-diff-only-a').textContent).toContain('Only in A (0)')
    expect(screen.getByTestId('trades-diff-only-b').textContent).toContain('Only in B (0)')
  })

  it('routes entries by leg into the correct column with counts', () => {
    render(
      <TradesDiffList
        entries={[
          trade({ leg: 'common', instrument: 'BTC-COMMON' }),
          trade({ leg: 'a', instrument: 'BTC-ONLYA' }),
          trade({ leg: 'a', instrument: 'ETH-ONLYA' }),
          trade({ leg: 'b', instrument: 'BTC-ONLYB' }),
        ]}
      />
    )
    const common = screen.getByTestId('trades-diff-common')
    const onlyA = screen.getByTestId('trades-diff-only-a')
    const onlyB = screen.getByTestId('trades-diff-only-b')

    expect(common.textContent).toContain('Common (1)')
    expect(common.textContent).toContain('BTC-COMMON')
    expect(onlyA.textContent).toContain('Only in A (2)')
    expect(onlyA.textContent).toContain('BTC-ONLYA')
    expect(onlyA.textContent).toContain('ETH-ONLYA')
    expect(onlyB.textContent).toContain('Only in B (1)')
    expect(onlyB.textContent).toContain('BTC-ONLYB')
  })

  it('renders the trade card with side, quantity, price, and pnl_a/pnl_b/pnl_delta', () => {
    render(
      <TradesDiffList
        entries={[
          trade({
            leg: 'common',
            side: 'sell',
            quantity: 2,
            price: 250,
            pnl_a: 10,
            pnl_b: 12,
            pnl_delta: 2,
          }),
        ]}
      />
    )
    const card = screen.getByTestId('trades-diff-common').textContent ?? ''

    expect(card).toContain('sell')
    expect(card).toContain('2')
    expect(card).toContain('250')
    expect(card).toContain('10.00')
    expect(card).toContain('12.00')
    expect(card).toContain('2.00')
  })

  it('renders em-dash + muted color for null pnl values', () => {
    render(
      <TradesDiffList
        entries={[
          trade({
            leg: 'a',
            instrument: 'BTC-NOPNL',
            pnl_a: null,
            pnl_b: null,
            pnl_delta: null,
          }),
        ]}
      />
    )
    const onlyA = screen.getByTestId('trades-diff-only-a')

    expect(onlyA.textContent).toContain('—')
  })

  it('color-codes pnl by sign (positive = gain, negative = loss)', () => {
    render(
      <TradesDiffList entries={[trade({ leg: 'common', pnl_a: 5, pnl_b: -3, pnl_delta: -8 })]} />
    )
    const positive = screen.getByText('5.00')
    const negative = screen.getByText('-3.00')

    expect(positive.className).toContain('text-gain-400')
    expect(negative.className).toContain('text-loss-400')
  })
})
