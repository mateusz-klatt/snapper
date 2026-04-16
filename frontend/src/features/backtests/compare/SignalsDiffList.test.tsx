import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { SignalsDiffList } from './SignalsDiffList'
import type { SignalDiffEntry } from '../../../types/api'

const signal = (overrides: Partial<SignalDiffEntry>): SignalDiffEntry =>
  ({
    instrument: 'BTC-USD',
    signal_time: '2026-01-01T00:00:00Z',
    signal_type: 'cross_up',
    leg: 'common',
    ...overrides,
  }) as SignalDiffEntry

describe('SignalsDiffList', () => {
  it('renders three columns each with empty marker when entries are empty', () => {
    render(<SignalsDiffList entries={[]} />)
    expect(screen.getByTestId('signals-diff-common').textContent).toContain('Common (0)')
    expect(screen.getByTestId('signals-diff-only-a').textContent).toContain('Only in A (0)')
    expect(screen.getByTestId('signals-diff-only-b').textContent).toContain('Only in B (0)')
  })

  it('routes entries by leg into the correct column with counts', () => {
    render(
      <SignalsDiffList
        entries={[
          signal({ leg: 'common', instrument: 'BTC-COMMON' }),
          signal({ leg: 'a', instrument: 'BTC-ONLYA' }),
          signal({ leg: 'a', instrument: 'ETH-ONLYA' }),
          signal({ leg: 'b', instrument: 'BTC-ONLYB' }),
        ]}
      />
    )
    expect(screen.getByTestId('signals-diff-common').textContent).toContain('Common (1)')
    expect(screen.getByTestId('signals-diff-only-a').textContent).toContain('Only in A (2)')
    expect(screen.getByTestId('signals-diff-only-b').textContent).toContain('Only in B (1)')
  })

  it('renders signal_type and instrument in the card', () => {
    render(
      <SignalsDiffList entries={[signal({ signal_type: 'reversal', instrument: 'ETH-USD' })]} />
    )
    const card = screen.getByTestId('signals-diff-common').textContent ?? ''

    expect(card).toContain('reversal')
    expect(card).toContain('ETH-USD')
  })
})
