import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ComparePage } from './ComparePage'

describe('ComparePage (Step 3 stub)', () => {
  it('renders the comparisonPublicId in a marker element', () => {
    render(<ComparePage comparisonPublicId='cmp-1' />)
    const node = screen.getByTestId('compare-page')

    expect(node.textContent).toBe('cmp-1')
  })
})
