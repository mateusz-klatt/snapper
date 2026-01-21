import { describe, it, expect, vi } from 'vitest'
import { render } from '@testing-library/react'
import { EquityChart } from './EquityChart'

vi.mock('react-plotly.js', () => ({
  default: ({
    data,
    layout,
    config,
    style,
  }: {
    data: never
    layout: never
    config: never
    style: never
  }) => (
    <div
      data-testid='plotly-chart'
      data-plot-data={JSON.stringify(data)}
      data-plot-layout={JSON.stringify(layout)}
      data-plot-config={JSON.stringify(config)}
      data-plot-style={JSON.stringify(style)}
    >
      Plotly Chart
    </div>
  ),
}))
describe('EquityChart', () => {
  const sampleData = [
    { timestamp: '2024-01-01 00:00', equity: 10000 },
    { timestamp: '2024-01-02 00:00', equity: 10100 },
    { timestamp: '2024-01-03 00:00', equity: 9900 },
  ]

  it('renders plotly chart', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)

    expect(getByTestId('plotly-chart')).toBeInTheDocument()
  })
  it('uses default title when not provided', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.title.text).toBe('Equity Curve')
  })
  it('uses custom title when provided', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} title='My Custom Equity Chart' />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.title.text).toBe('My Custom Equity Chart')
  })
  it('uses default height when not provided', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.height).toBe(500)
  })
  it('uses custom height when provided', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} height={800} />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.height).toBe(800)
  })
  it('maps data to plotly format', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const plotData = JSON.parse(chart.getAttribute('data-plot-data') || '[]')

    expect(plotData).toHaveLength(1)
    expect(plotData[0].x).toEqual(['2024-01-01 00:00', '2024-01-02 00:00', '2024-01-03 00:00'])
    expect(plotData[0].y).toEqual([10000, 10100, 9900])
  })
  it('configures scatter plot with lines', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const plotData = JSON.parse(chart.getAttribute('data-plot-data') || '[]')

    expect(plotData[0].type).toBe('scatter')
    expect(plotData[0].mode).toBe('lines')
  })
  it('adds break-even line at initial equity', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.shapes).toHaveLength(1)
    expect(layout.shapes[0].type).toBe('line')
    expect(layout.shapes[0].y0).toBe(10000)
    expect(layout.shapes[0].y1).toBe(10000)
  })
  it('adds break-even annotation', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const layout = JSON.parse(chart.getAttribute('data-plot-layout') || '{}')

    expect(layout.annotations).toHaveLength(1)
    expect(layout.annotations[0].text).toBe('Break-even')
  })
  it('configures plot with responsive mode', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const config = JSON.parse(chart.getAttribute('data-plot-config') || '{}')

    expect(config.responsive).toBe(true)
    expect(config.displaylogo).toBe(false)
  })
  it('sets full width style', () => {
    const { getByTestId } = render(<EquityChart data={sampleData} />)
    const chart = getByTestId('plotly-chart')
    const style = JSON.parse(chart.getAttribute('data-plot-style') || '{}')

    expect(style.width).toBe('100%')
  })
  it('handles empty data gracefully', () => {
    const { getByTestId } = render(<EquityChart data={[]} />)
    const chart = getByTestId('plotly-chart')
    const plotData = JSON.parse(chart.getAttribute('data-plot-data') || '[]')

    expect(plotData[0].x).toEqual([])
    expect(plotData[0].y).toEqual([])
  })
  it('applies proper chart wrapper class', () => {
    const { container } = render(<EquityChart data={sampleData} />)
    const wrapper = container.firstChild as HTMLElement

    expect(wrapper).toHaveClass('equity-chart')
  })
})
