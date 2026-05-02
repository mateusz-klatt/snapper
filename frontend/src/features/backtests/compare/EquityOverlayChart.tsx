import React, { useEffect, useRef } from 'react'
import {
  IChartApi,
  ISeriesApi,
  LineData,
  LineSeries,
  Time,
  UTCTimestamp,
  createChart,
} from 'lightweight-charts'
import { useAppStore } from '../../../stores/app'
import type { EquityOverlayPoint } from '../../../types/api'

interface Props {
  points: EquityOverlayPoint[]
  height?: number
  className?: string
}

const LIGHT_THEME = {
  background: '#fdf8f0',
  text: '#6f695f',
  grid: '#ece8df',
  border: '#e6e3dc',
}
const DARK_THEME = {
  background: '#181a1e',
  text: '#9aa4b4',
  grid: '#262a30',
  border: '#3a4048',
}
const SERIES_A_COLOR = '#3b82f6'
const SERIES_B_COLOR = '#f97316'

const toUtc = (iso: string): UTCTimestamp =>
  Math.floor(new Date(iso).getTime() / 1000) as UTCTimestamp

const buildSeriesData = (
  points: EquityOverlayPoint[],
  pick: (p: EquityOverlayPoint) => number | null | undefined
): LineData<Time>[] =>
  points
    .filter(p => pick(p) !== null && pick(p) !== undefined)
    .map(p => ({ time: toUtc(p.point_time), value: pick(p) as number }))

/**
 * Equity overlay chart — two independent line series (Run A blue,
 * Run B orange) on a shared X axis. One-sided gaps are preserved:
 * a point with `equity_a=null` produces zero entries on series A
 * at that timestamp (no `?? 0` backfill, no cross-leg substitution).
 * Theme-aware via useAppStore.
 */
export const EquityOverlayChart: React.FC<Props> = ({ points, height = 300, className = '' }) => {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesARef = useRef<ISeriesApi<'Line'> | null>(null)
  const seriesBRef = useRef<ISeriesApi<'Line'> | null>(null)
  const isDarkMode = useAppStore(s => s.isDarkMode)

  useEffect(() => {
    if (!containerRef.current) return

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height,
      layout: {
        background: { color: LIGHT_THEME.background },
        textColor: LIGHT_THEME.text,
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: LIGHT_THEME.grid },
        horzLines: { color: LIGHT_THEME.grid },
      },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: LIGHT_THEME.border },
      timeScale: {
        borderColor: LIGHT_THEME.border,
        timeVisible: true,
        secondsVisible: false,
      },
    })

    seriesARef.current = chart.addSeries(LineSeries, { color: SERIES_A_COLOR, lineWidth: 2 })
    seriesBRef.current = chart.addSeries(LineSeries, { color: SERIES_B_COLOR, lineWidth: 2 })
    chartRef.current = chart

    const onResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth })
      }
    }

    globalThis.addEventListener('resize', onResize)

    return () => {
      globalThis.removeEventListener('resize', onResize)
      chartRef.current?.remove()
      chartRef.current = null
      seriesARef.current = null
      seriesBRef.current = null
    }
  }, [height])

  useEffect(() => {
    if (!chartRef.current) return
    const theme = isDarkMode ? DARK_THEME : LIGHT_THEME

    chartRef.current.applyOptions({
      layout: { background: { color: theme.background }, textColor: theme.text },
      grid: { vertLines: { color: theme.grid }, horzLines: { color: theme.grid } },
      rightPriceScale: { borderColor: theme.border },
      timeScale: { borderColor: theme.border },
    })
  }, [isDarkMode])

  useEffect(() => {
    if (!seriesARef.current || !seriesBRef.current || !chartRef.current) return

    seriesARef.current.setData(buildSeriesData(points, p => p.equity_a))
    seriesBRef.current.setData(buildSeriesData(points, p => p.equity_b))

    if (points.length > 0) {
      chartRef.current.timeScale().fitContent()
    }
  }, [points])

  return (
    <div className={`relative ${className}`} data-testid='equity-overlay-chart'>
      <div ref={containerRef} style={{ height: `${height}px` }} />
      <div className='mt-2 flex items-center gap-4 text-xs text-muted-500'>
        <span className='flex items-center gap-1'>
          <span
            className='inline-block h-2 w-3 rounded-sm'
            style={{ backgroundColor: SERIES_A_COLOR }}
          />
          <span>Run A</span>
        </span>
        <span className='flex items-center gap-1'>
          <span
            className='inline-block h-2 w-3 rounded-sm'
            style={{ backgroundColor: SERIES_B_COLOR }}
          />
          <span>Run B</span>
        </span>
      </div>
    </div>
  )
}
