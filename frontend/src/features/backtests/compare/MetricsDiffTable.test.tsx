import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MetricsDiffTable } from './MetricsDiffTable'
import type { MetricDiffRow } from '../../../types/api'

const row = (overrides: Partial<MetricDiffRow>): MetricDiffRow => ({
  name: 'sharpe_ratio',
  run_a: 1.5,
  run_b: 1.2,
  delta: -0.3,
  pct: -0.2,
  ...overrides,
})

describe('MetricsDiffTable', () => {
  it('renders empty state when no rows', () => {
    render(<MetricsDiffTable rows={[]} />)
    expect(screen.getByTestId('metrics-diff-empty')).toBeDefined()
    expect(screen.queryByTestId('metrics-diff-table')).toBeNull()
  })

  it('renders one row with formatted values + delta + pct', () => {
    render(<MetricsDiffTable rows={[row({})]} />)
    expect(screen.getByText('sharpe_ratio')).toBeDefined()
    expect(screen.getByText('1.5000')).toBeDefined()
    expect(screen.getByText('1.2000')).toBeDefined()
    expect(screen.getByText('-0.3000')).toBeDefined()
    expect(screen.getByText('-20.00 %')).toBeDefined()
  })

  it('renders multiple rows with sign-based color (positive = gain, negative = loss, zero = muted)', () => {
    render(
      <MetricsDiffTable
        rows={[
          row({ name: 'cagr', run_a: 0.1, run_b: 0.2, delta: 0.07, pct: 0.7 }),
          row({ name: 'mdd', run_a: 0.5, run_b: 0.5, delta: 0, pct: 0 }),
          row({ name: 'sortino', run_a: 1, run_b: 0.5, delta: -0.55, pct: -0.55 }),
        ]}
      />
    )

    const cagrDelta = screen.getByText('0.0700')

    expect(cagrDelta.className).toContain('text-gain-400')

    const mddDelta = screen.getByText('0.0000')

    expect(mddDelta.className).toContain('text-muted-400')

    const sortinoDelta = screen.getByText('-0.5500')

    expect(sortinoDelta.className).toContain('text-loss-400')
  })

  it('renders em-dash for null/undefined values (one-sided metric)', () => {
    render(
      <MetricsDiffTable rows={[row({ name: 'only_in_a', run_b: null, delta: null, pct: null })]} />
    )
    expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(3)
  })

  it('renders raw value when not finite (Infinity/NaN guard)', () => {
    render(
      <MetricsDiffTable
        rows={[row({ name: 'profit_factor', run_a: Infinity, run_b: 1, delta: Infinity })]}
      />
    )
    expect(screen.getAllByText('Infinity').length).toBeGreaterThanOrEqual(2)
  })
})
