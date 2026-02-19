import { useEffect, useRef } from 'react'
import {
  CandlestickSeries,
  CandlestickData,
  IChartApi,
  ISeriesApi,
  Time,
  createChart,
} from 'lightweight-charts'
import { useAppStore } from '../stores/app'

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

interface LightweightChartProps {
  data: CandlestickData<Time>[]
  height?: number
  width?: number
  className?: string
}

export const LightweightChart = ({
  data,
  height = 400,
  width,
  className = '',
}: Readonly<LightweightChartProps>) => {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const isDarkMode = useAppStore(s => s.isDarkMode)

  useEffect(() => {
    if (!chartContainerRef.current) {
      return
    }

    const chart = createChart(chartContainerRef.current, {
      width: width || chartContainerRef.current.clientWidth,
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
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: LIGHT_THEME.border,
      },
      timeScale: {
        borderColor: LIGHT_THEME.border,
        timeVisible: true,
        secondsVisible: false,
      },
    })
    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#3cb67a',
      downColor: '#d8062a',
      borderUpColor: '#3cb67a',
      borderDownColor: '#d8062a',
      wickUpColor: '#3cb67a',
      wickDownColor: '#d8062a',
    })

    chartRef.current = chart
    seriesRef.current = candlestickSeries

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: width || chartContainerRef.current.clientWidth,
        })
      }
    }

    globalThis.addEventListener('resize', handleResize)

    return () => {
      globalThis.removeEventListener('resize', handleResize)
      chartRef.current?.remove()
      chartRef.current = null
      seriesRef.current = null
    }
  }, [height, width])
  useEffect(() => {
    if (!chartRef.current) {
      return
    }

    const theme = isDarkMode ? DARK_THEME : LIGHT_THEME

    chartRef.current.applyOptions({
      layout: {
        background: { color: theme.background },
        textColor: theme.text,
      },
      grid: {
        vertLines: { color: theme.grid },
        horzLines: { color: theme.grid },
      },
      rightPriceScale: {
        borderColor: theme.border,
      },
      timeScale: {
        borderColor: theme.border,
      },
    })
  }, [isDarkMode])
  useEffect(() => {
    if (!seriesRef.current || !chartRef.current) {
      return
    }

    try {
      if (data.length > 0) {
        const uniqueData = data.filter((item, index, arr) => {
          if (index === 0) return true

          return item.time !== arr[index - 1].time
        })

        if (uniqueData.length > 0) {
          seriesRef.current.setData(uniqueData)
          chartRef.current.timeScale().fitContent()
        } else {
          seriesRef.current.setData([])
        }
      } else {
        seriesRef.current.setData([])
      }
    } catch (error) {
      console.error('LightweightChart: Error updating chart data:', error)

      try {
        seriesRef.current.setData([])
      } catch (cleanupError) {
        console.error('LightweightChart: Error clearing chart data:', cleanupError)
      }
    }
  }, [data])

  return (
    <div
      ref={chartContainerRef}
      className={`relative ${className}`}
      style={{ height: `${height}px` }}
    />
  )
}
