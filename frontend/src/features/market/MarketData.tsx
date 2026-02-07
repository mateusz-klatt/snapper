import { useMemo } from 'react'
import { Card, Button, LoadingSpinner } from '../../components/ui'
import { LightweightChart } from '../../components/LightweightChart'
import { useCandles, useExchanges, useExchangeInstruments } from '../../hooks/queries'
import { useMarketStore } from '../../stores/market'
import { useAppStore } from '../../stores/app'
import * as Select from '@radix-ui/react-select'
import { ChevronDownIcon } from 'lucide-react'
import { Time } from 'lightweight-charts'

interface FormattedCandle {
  time: Time
  open: number
  high: number
  low: number
  close: number
}
const timeframes = [
  { value: '1m', label: '1 Minute' },
  { value: '5m', label: '5 Minutes' },
  { value: '15m', label: '15 Minutes' },
  { value: '30m', label: '30 Minutes' },
  { value: '1h', label: '1 Hour' },
  { value: '4h', label: '4 Hours' },
  { value: '1d', label: '1 Day' },
]

export function MarketData() {
  const {
    selectedExchange,
    selectedInstrument,
    selectedTimeframe,
    setSelectedExchange,
    setSelectedInstrument,
    setSelectedTimeframe,
  } = useMarketStore()
  const { isConnected } = useAppStore()
  const { data: exchanges } = useExchanges()
  const { data: instruments } = useExchangeInstruments(selectedExchange)
  const {
    data: candles,
    isLoading,
    error,
    isFetching,
    refetch,
  } = useCandles(selectedInstrument ?? '', selectedExchange ?? '', selectedTimeframe)
  const chartData: FormattedCandle[] = useMemo(() => {
    if (!candles || isFetching) return []
    const sortedCandles = [...candles].sort((a, b) => {
      const timeA = new Date(a.timestamp).getTime()
      const timeB = new Date(b.timestamp).getTime()

      return timeA - timeB
    })
    const deduped: FormattedCandle[] = []

    for (const candle of sortedCandles) {
      const unixTime = Math.floor(new Date(candle.timestamp).getTime() / 1000)
      const previous = deduped[deduped.length - 1]

      if (previous?.time === unixTime) {
        deduped[deduped.length - 1] = {
          time: unixTime as Time,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
        }
      } else {
        deduped.push({
          time: unixTime as Time,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
        })
      }
    }

    return deduped
  }, [candles, isFetching])

  const getConnectionStatusColor = () => {
    if (isConnected) {
      return 'bg-green-500'
    } else {
      return 'bg-red-500'
    }
  }

  const stats = useMemo(() => {
    if (!chartData.length) return null
    const latest = chartData[chartData.length - 1]
    const previous = chartData.length > 1 ? chartData[chartData.length - 2] : null
    const change = previous ? latest.close - previous.close : 0
    const changePercent = previous ? (change / previous.close) * 100 : 0
    const prices = chartData.map(c => c.close)
    const high24h = Math.max(...prices)
    const low24h = Math.min(...prices)

    return {
      price: latest.close,
      change,
      changePercent,
      high24h,
      low24h,
    }
  }, [chartData])

  return (
    <div className='h-full flex flex-col space-y-6'>
      {}
      <div className='flex items-center justify-between'>
        <div className='flex items-center space-x-3'>
          <h2 className='text-xl font-bold'>Market Data</h2>
          <div className='flex items-center space-x-2'>
            <div className={`w-2 h-2 rounded-full ${getConnectionStatusColor()}`}></div>
            <span className='text-sm text-gray-500 capitalize'>
              {isConnected ? 'connected' : 'disconnected'}
            </span>
          </div>
        </div>
        <Button variant='secondary' size='sm' onClick={() => refetch()}>
          Refresh
        </Button>
      </div>
      {}
      <div className='flex items-center space-x-4'>
        <div className='flex items-center space-x-2'>
          <label htmlFor='exchange-select' className='text-sm font-medium text-dark-300'>
            Exchange:
          </label>
          <Select.Root value={selectedExchange ?? undefined} onValueChange={setSelectedExchange}>
            <Select.Trigger
              id='exchange-select'
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-dark-800 border border-dark-600 text-white hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500'
            >
              <Select.Value placeholder='Select exchange' />
              <Select.Icon className='ml-2'>
                <ChevronDownIcon size={16} />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content className='overflow-hidden bg-dark-800 rounded-md shadow-lg border border-dark-600'>
                <Select.Viewport className='p-1'>
                  {(exchanges ?? []).map(ex => (
                    <Select.Item
                      key={ex}
                      value={ex}
                      className='flex select-none items-center px-3 py-2 text-sm text-white rounded-sm hover:bg-dark-700 focus:bg-dark-700 cursor-pointer'
                    >
                      <Select.ItemText>{ex}</Select.ItemText>
                    </Select.Item>
                  ))}
                </Select.Viewport>
              </Select.Content>
            </Select.Portal>
          </Select.Root>
        </div>
        <div className='flex items-center space-x-2'>
          <label htmlFor='instrument-select' className='text-sm font-medium text-dark-300'>
            Instrument:
          </label>
          <Select.Root
            value={selectedInstrument ?? undefined}
            onValueChange={setSelectedInstrument}
            disabled={!selectedExchange}
          >
            <Select.Trigger
              id='instrument-select'
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-dark-800 border border-dark-600 text-white hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50'
            >
              <Select.Value placeholder='Select instrument' />
              <Select.Icon className='ml-2'>
                <ChevronDownIcon size={16} />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content className='overflow-hidden bg-dark-800 rounded-md shadow-lg border border-dark-600'>
                <Select.Viewport className='p-1'>
                  {(instruments ?? []).map(inst => (
                    <Select.Item
                      key={inst}
                      value={inst}
                      className='flex select-none items-center px-3 py-2 text-sm text-white rounded-sm hover:bg-dark-700 focus:bg-dark-700 cursor-pointer'
                    >
                      <Select.ItemText>{inst}</Select.ItemText>
                    </Select.Item>
                  ))}
                </Select.Viewport>
              </Select.Content>
            </Select.Portal>
          </Select.Root>
        </div>
        <div className='flex items-center space-x-2'>
          <label htmlFor='timeframe-select' className='text-sm font-medium text-dark-300'>
            Timeframe:
          </label>
          <Select.Root value={selectedTimeframe} onValueChange={setSelectedTimeframe}>
            <Select.Trigger
              id='timeframe-select'
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-dark-800 border border-dark-600 text-white hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500'
            >
              <Select.Value />
              <Select.Icon className='ml-2'>
                <ChevronDownIcon size={16} />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content className='overflow-hidden bg-dark-800 rounded-md shadow-lg border border-dark-600'>
                <Select.Viewport className='p-1'>
                  {timeframes.map(timeframe => (
                    <Select.Item
                      key={timeframe.value}
                      value={timeframe.value}
                      className='flex select-none items-center px-3 py-2 text-sm text-white rounded-sm hover:bg-dark-700 focus:bg-dark-700 cursor-pointer'
                    >
                      <Select.ItemText>{timeframe.label}</Select.ItemText>
                    </Select.Item>
                  ))}
                </Select.Viewport>
              </Select.Content>
            </Select.Portal>
          </Select.Root>
        </div>
      </div>
      {}
      {stats && (
        <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
          <Card title='Current Price' className='p-4'>
            <div>
              <p className='text-sm text-gray-500'>Current Price</p>
              <p className='text-lg font-semibold'>{stats.price.toFixed(5)}</p>
            </div>
          </Card>
          <Card title='24h Change' className='p-4'>
            <div className='flex items-center justify-between'>
              <div>
                <p className='text-sm text-gray-500'>24h Change</p>
                <p
                  className={`text-lg font-semibold ${stats.change >= 0 ? 'text-green-600' : 'text-red-600'}`}
                >
                  {stats.change >= 0 ? '+' : ''}
                  {stats.change.toFixed(5)} ({stats.changePercent.toFixed(2)}%)
                </p>
              </div>
            </div>
          </Card>
          <Card title='24h High' className='p-4'>
            <div>
              <p className='text-sm text-gray-500'>24h High</p>
              <p className='text-lg font-semibold text-green-600'>{stats.high24h.toFixed(5)}</p>
            </div>
          </Card>
          <Card title='24h Low' className='p-4'>
            <div>
              <p className='text-sm text-gray-500'>24h Low</p>
              <p className='text-lg font-semibold text-red-600'>{stats.low24h.toFixed(5)}</p>
            </div>
          </Card>
        </div>
      )}
      {}
      <Card title='Price Chart' className='flex-1 p-6'>
        {isLoading && (
          <div className='flex items-center justify-center h-full'>
            <LoadingSpinner />
          </div>
        )}
        {!isLoading && error && (
          <div className='flex items-center justify-center h-full'>
            <p className='text-red-600'>
              Error loading chart data: {error?.message || 'Unknown error'}
            </p>
          </div>
        )}
        {!isLoading && !error && chartData.length > 0 && (
          <LightweightChart data={chartData} height={400} />
        )}
        {!isLoading && !error && chartData.length === 0 && (
          <div className='flex items-center justify-center h-full'>
            <div className='text-center'>
              <p className='text-gray-500 mb-2'>No data available for {selectedInstrument}</p>
              <p className='text-sm text-gray-400'>
                The instrument may not exist or has no{' '}
                {timeframes.find(t => t.value === selectedTimeframe)?.label.toLowerCase()} data
              </p>
            </div>
          </div>
        )}
      </Card>
    </div>
  )
}
