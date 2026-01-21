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
}: LightweightChartProps) => {
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
        background: { color: '#1e293b' },
        textColor: '#e2e8f0',
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: '#374151' },
        horzLines: { color: '#374151' },
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: '#374151',
      },
      timeScale: {
        borderColor: '#374151',
        timeVisible: true,
        secondsVisible: false,
      },
    })
    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#10b981',
      downColor: '#ef4444',
      borderUpColor: '#10b981',
      borderDownColor: '#ef4444',
      wickUpColor: '#10b981',
      wickDownColor: '#ef4444',
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

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
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
