import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render } from '@testing-library/react'
import { EquityOverlayChart } from './EquityOverlayChart'
import type { EquityOverlayPoint } from '../../../types/api'

vi.mock('../../../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: { isDarkMode: boolean }) => boolean) =>
    selector({ isDarkMode: false })
  ),
}))

const setDataA = vi.fn()
const setDataB = vi.fn()
const mockApplyOptions = vi.fn()
const mockTimeScaleFitContent = vi.fn()
const mockRemove = vi.fn()
const mockAddSeries = vi.fn()
const mockCreateChart = vi.fn()

mockAddSeries.mockImplementation(() => {
  const callIdx = mockAddSeries.mock.calls.length

  return { setData: callIdx % 2 === 1 ? setDataA : setDataB }
})
mockCreateChart.mockReturnValue({
  addSeries: mockAddSeries,
  remove: mockRemove,
  applyOptions: mockApplyOptions,
  timeScale: () => ({ fitContent: mockTimeScaleFitContent }),
})

vi.mock('lightweight-charts', () => ({
  createChart: (container: never, options: never) => mockCreateChart(container, options),
  LineSeries: 'LineSeries',
}))

const point = (
  ts: string,
  a: number | null | undefined,
  b: number | null | undefined
): EquityOverlayPoint =>
  ({
    point_time: ts,
    equity_a: a,
    equity_b: b,
  }) as EquityOverlayPoint

beforeEach(() => {
  vi.clearAllMocks()
  setDataA.mockReset()
  setDataB.mockReset()
  mockAddSeries.mockImplementation(() => {
    const callIdx = mockAddSeries.mock.calls.length

    return { setData: callIdx % 2 === 1 ? setDataA : setDataB }
  })
  mockCreateChart.mockReturnValue({
    addSeries: mockAddSeries,
    remove: mockRemove,
    applyOptions: mockApplyOptions,
    timeScale: () => ({ fitContent: mockTimeScaleFitContent }),
  })
})

describe('EquityOverlayChart', () => {
  it('creates two LineSeries (Run A blue, Run B orange) and renders the legend', () => {
    const { getByText } = render(<EquityOverlayChart points={[]} />)

    expect(mockAddSeries).toHaveBeenCalledTimes(2)
    expect(mockAddSeries.mock.calls[0][0]).toBe('LineSeries')
    expect(mockAddSeries.mock.calls[0][1]).toMatchObject({ color: '#3b82f6', lineWidth: 2 })
    expect(mockAddSeries.mock.calls[1][1]).toMatchObject({ color: '#f97316', lineWidth: 2 })
    expect(getByText('Run A')).toBeDefined()
    expect(getByText('Run B')).toBeDefined()
  })

  it('builds two independent series with gap-preservation (no cross-leg backfill)', () => {
    const points = [
      point('2026-01-01T00:00:00Z', 100, null),
      point('2026-01-02T00:00:00Z', 110, 200),
      point('2026-01-03T00:00:00Z', null, 220),
    ]

    render(<EquityOverlayChart points={points} />)
    expect(setDataA).toHaveBeenCalledTimes(1)
    expect(setDataB).toHaveBeenCalledTimes(1)

    const aData = setDataA.mock.calls[0][0]
    const bData = setDataB.mock.calls[0][0]

    expect(aData).toHaveLength(2)
    expect(aData.map((p: { value: number }) => p.value)).toEqual([100, 110])

    expect(bData).toHaveLength(2)
    expect(bData.map((p: { value: number }) => p.value)).toEqual([200, 220])
  })

  it('treats undefined equity values as gaps (matches null behavior)', () => {
    const points = [point('2026-01-01T00:00:00Z', undefined, 50)]

    render(<EquityOverlayChart points={points} />)
    expect(setDataA.mock.calls[0][0]).toEqual([])
    expect(setDataB.mock.calls[0][0]).toHaveLength(1)
  })

  it('skips fitContent when points are empty', () => {
    render(<EquityOverlayChart points={[]} />)
    expect(mockTimeScaleFitContent).not.toHaveBeenCalled()
  })

  it('calls fitContent when points are present', () => {
    render(<EquityOverlayChart points={[point('2026-01-01T00:00:00Z', 100, 200)]} />)
    expect(mockTimeScaleFitContent).toHaveBeenCalled()
  })

  it('cleans up the chart on unmount', () => {
    const { unmount } = render(<EquityOverlayChart points={[]} />)

    unmount()
    expect(mockRemove).toHaveBeenCalled()
  })

  it('handles resize event by re-applying width', () => {
    render(<EquityOverlayChart points={[]} />)
    mockApplyOptions.mockClear()
    globalThis.dispatchEvent(new Event('resize'))
    expect(mockApplyOptions).toHaveBeenCalled()
  })

  it('applies dark theme via applyOptions when isDarkMode flips on', async () => {
    const appStore = await import('../../../stores/app')

    vi.mocked(appStore.useAppStore).mockImplementation(((
      selector: (s: { isDarkMode: boolean }) => boolean
    ) => selector({ isDarkMode: true })) as never)
    render(<EquityOverlayChart points={[]} />)
    expect(mockApplyOptions).toHaveBeenCalledWith(
      expect.objectContaining({
        layout: expect.objectContaining({ background: { color: '#181a1e' } }),
      })
    )
    vi.mocked(appStore.useAppStore).mockImplementation(((
      selector: (s: { isDarkMode: boolean }) => boolean
    ) => selector({ isDarkMode: false })) as never)
  })

  it('applies className on the wrapper', () => {
    const { container } = render(<EquityOverlayChart points={[]} className='custom-overlay' />)
    const wrapper = container.querySelector('[data-testid=equity-overlay-chart]')

    expect(wrapper?.className).toContain('custom-overlay')
  })

  it('skips setup when containerRef is null', async () => {
    vi.resetModules()
    const React = await import('react')
    const originalUseRef = React.useRef
    let callCount = 0

    vi.doMock('react', async () => ({
      ...React,
      useRef: ((initialValue: unknown) => {
        callCount += 1
        const ref = originalUseRef(initialValue)

        if (callCount === 1) {
          Object.defineProperty(ref, 'current', {
            get: () => null,
            set: () => {},
            configurable: true,
          })
        }

        return ref
      }) as typeof React.useRef,
    }))
    const { EquityOverlayChart: Mocked } = await import('./EquityOverlayChart')

    mockCreateChart.mockClear()
    render(<Mocked points={[]} />)
    expect(mockCreateChart).not.toHaveBeenCalled()
    vi.doUnmock('react')
    vi.resetModules()
  })

  it('skips resize when chart ref is missing', async () => {
    vi.resetModules()
    const React = await import('react')
    const containerRef = { current: document.createElement('div') }
    const chartRef = { current: null as unknown }
    const seriesARef = { current: null as unknown }
    const seriesBRef = { current: null as unknown }
    let callCount = 0

    vi.doMock('react', async () => ({
      ...React,
      useRef: ((initialValue: unknown) => {
        callCount += 1
        if (callCount === 1) return containerRef
        if (callCount === 2) return chartRef
        if (callCount === 3) return seriesARef
        if (callCount === 4) return seriesBRef

        return React.useRef(initialValue)
      }) as typeof React.useRef,
    }))
    const { EquityOverlayChart: Mocked } = await import('./EquityOverlayChart')

    render(<Mocked points={[]} />)
    mockApplyOptions.mockClear()
    chartRef.current = null
    globalThis.dispatchEvent(new Event('resize'))
    expect(mockApplyOptions).not.toHaveBeenCalled()
    vi.doUnmock('react')
    vi.resetModules()
  })
})
