import { useEffect, useRef } from 'react'
import {
  CandlestickSeries,
  CandlestickData,
  IChartApi,
  ISeriesApi,
  Time,
  createChart,
} from 'lightweight-charts'

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

  useEffect(() => {
    if (!chartContainerRef.current) {
      return
    }

    const chart = createChart(chartContainerRef.current, {
      width: width || chartContainerRef.current.clientWidth,
      height,
      layout: {
        background: { color: '#fdf8f0' },
        textColor: '#6f695f',
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: '#ece8df' },
        horzLines: { color: '#ece8df' },
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: '#e6e3dc',
      },
      timeScale: {
        borderColor: '#e6e3dc',
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
