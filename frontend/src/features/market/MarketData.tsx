import { useMemo, useState, useRef, useEffect } from 'react'
import { Card, LoadingSpinner } from '../../components/ui'
import { InstrumentIcon } from '../../components/InstrumentIcon'
import { MarketDataOnlyBadge } from '../../components/MarketDataOnlyBadge'
import { LightweightChart } from '../../components/LightweightChart'
import { useCandles, useExchanges, useExchangeInstrumentsDetail } from '../../hooks/queries'
import { useAppStore } from '../../stores/app'
import { useMarketStore } from '../../stores/market'
import { useMarketSubscription } from '../../hooks/useMarketSubscription'
import { useWebSocketStore } from '../../stores/websocket'
import { useWSDispatcher } from '../../hooks/useWSDispatcher'
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

  const { isConnected } = useWebSocketStore()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const dispatcher = useWSDispatcher({ enabled: !isTimeTraveling })
  const subscribed = useMarketSubscription({
    instrument: selectedInstrument,
    exchange: selectedExchange,
    timeframe: selectedTimeframe,
    dispatcher,
  })
  const snapshotEnabled = subscribed || !isConnected || isTimeTraveling
  const { data: exchanges } = useExchanges()
  const { data: instruments } = useExchangeInstrumentsDetail(selectedExchange)
  const capabilityMap = useMemo(() => {
    const map = new Map<string, boolean>()

    for (const row of instruments?.payload ?? []) {
      map.set(row.symbol, row.can_trade)
    }

    return map
  }, [instruments])
  const isSelectedMarketDataOnly =
    selectedInstrument !== null && capabilityMap.get(selectedInstrument) === false
  const {
    data: candles,
    isLoading,
    error,
    isFetching,
  } = useCandles(
    selectedInstrument ?? '',
    selectedExchange ?? '',
    selectedTimeframe,
    100,
    snapshotEnabled
  )
  const flushedRef = useRef(false)

  useEffect(() => {
    flushedRef.current = false
  }, [selectedInstrument, selectedExchange, selectedTimeframe])
  useEffect(() => {
    if (
      candles &&
      !isFetching &&
      dispatcher &&
      selectedInstrument &&
      selectedExchange &&
      !flushedRef.current
    ) {
      dispatcher.flushBuffer(selectedInstrument, selectedExchange, selectedTimeframe)
      flushedRef.current = true
    }
  }, [candles, isFetching, dispatcher, selectedInstrument, selectedExchange, selectedTimeframe])
  const [instrumentSearch, setInstrumentSearch] = useState('')
  const [instrumentDropdownOpen, setInstrumentDropdownOpen] = useState(false)
  const instrumentRef = useRef<HTMLDivElement>(null)
  const filteredInstruments = useMemo(() => {
    const list = (instruments?.payload ?? []).map(row => row.symbol)

    if (!instrumentSearch) return list

    return list.filter(inst => inst.toLowerCase().includes(instrumentSearch.toLowerCase()))
  }, [instruments, instrumentSearch])

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (instrumentRef.current && !instrumentRef.current.contains(e.target as Node)) {
        setInstrumentDropdownOpen(false)
      }
    }

    if (instrumentDropdownOpen) {
      document.addEventListener('mousedown', handleClickOutside)
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [instrumentDropdownOpen])

  const chartData: FormattedCandle[] = useMemo(() => {
    if (!candles || isFetching) return []
    const sortedCandles = [...candles].sort((a, b) => {
      const timeA = new Date(a.open_at).getTime()
      const timeB = new Date(b.open_at).getTime()

      return timeA - timeB
    })
    const deduped: FormattedCandle[] = []

    for (const candle of sortedCandles) {
      const unixTime = Math.floor(new Date(candle.open_at).getTime() / 1000)
      const previous = deduped.at(-1)

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

  const stats = useMemo(() => {
    if (!chartData.length) return null
    const latest = chartData.at(-1) as FormattedCandle
    const previous = chartData.length > 1 ? (chartData.at(-2) as FormattedCandle) : null
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
    <div className='flex flex-col space-y-6'>
      {}
      <div className='flex items-center justify-between'>
        <div className='flex items-center space-x-3'>
          {selectedInstrument !== null && selectedExchange !== null && (
            <InstrumentIcon symbol={selectedInstrument} exchange={selectedExchange} size={32} />
          )}
          <h2 className='text-xl font-bold'>Market Data</h2>
          {isSelectedMarketDataOnly && <MarketDataOnlyBadge size='md' />}
        </div>
      </div>
      {}
      <div className='flex flex-wrap items-center gap-4'>
        <div className='flex items-center gap-2'>
          <label htmlFor='exchange-select' className='text-sm font-medium text-muted-600'>
            Exchange:
          </label>
          <Select.Root value={selectedExchange ?? undefined} onValueChange={setSelectedExchange}>
            <Select.Trigger
              id='exchange-select'
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-alpine-50 border border-dark-600 text-alpine-900 hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500'
            >
              <Select.Value placeholder='Select exchange' />
              <Select.Icon className='ml-2'>
                <ChevronDownIcon size={16} />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content className='z-50 overflow-hidden bg-alpine-50 rounded-md shadow-lg border border-dark-600'>
                <Select.Viewport className='p-1'>
                  {(exchanges?.payload ?? []).map(ex => (
                    <Select.Item
                      key={ex}
                      value={ex}
                      className='flex select-none items-center px-3 py-2 text-sm text-alpine-900 rounded-sm hover:bg-dark-700 focus:bg-dark-700 cursor-pointer'
                    >
                      <Select.ItemText>{ex}</Select.ItemText>
                    </Select.Item>
                  ))}
                </Select.Viewport>
              </Select.Content>
            </Select.Portal>
          </Select.Root>
        </div>
        <div className='flex items-center gap-2'>
          <label htmlFor='instrument-search' className='text-sm font-medium text-muted-600'>
            Instrument:
          </label>
          <div ref={instrumentRef} className='relative'>
            <input
              id='instrument-search'
              type='text'
              value={instrumentDropdownOpen ? instrumentSearch : (selectedInstrument ?? '')}
              onChange={e => setInstrumentSearch(e.target.value)}
              onFocus={() => {
                setInstrumentDropdownOpen(true)
                setInstrumentSearch('')
              }}
              placeholder='Search instrument...'
              disabled={!selectedExchange}
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-alpine-50 border border-dark-600 text-alpine-900 hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50'
            />
            {instrumentDropdownOpen && (
              <div className='absolute top-full left-0 mt-1 w-full min-w-48 max-h-60 overflow-y-auto z-50 bg-alpine-50 rounded-md shadow-lg border border-dark-600'>
                {filteredInstruments.length > 0 ? (
                  filteredInstruments.map(inst => {
                    const canTrade = capabilityMap.get(inst)
                    const isMarketDataOnly = canTrade === false

                    return (
                      <button
                        key={inst}
                        type='button'
                        onClick={() => {
                          setSelectedInstrument(inst)
                          setInstrumentDropdownOpen(false)
                          setInstrumentSearch('')
                        }}
                        className='flex w-full select-none items-center justify-between gap-3 px-3 py-2 text-sm text-alpine-900 rounded-sm hover:bg-dark-700 cursor-pointer'
                      >
                        <span className='flex items-center gap-2'>
                          {selectedExchange !== null && (
                            <InstrumentIcon symbol={inst} exchange={selectedExchange} size={20} />
                          )}
                          {inst}
                        </span>
                        {isMarketDataOnly && <MarketDataOnlyBadge size='sm' />}
                      </button>
                    )
                  })
                ) : (
                  <div className='px-3 py-2 text-sm text-muted-500'>No instruments found</div>
                )}
              </div>
            )}
          </div>
        </div>
        <div className='flex items-center gap-2'>
          <label htmlFor='timeframe-select' className='text-sm font-medium text-muted-600'>
            Timeframe:
          </label>
          <Select.Root value={selectedTimeframe} onValueChange={setSelectedTimeframe}>
            <Select.Trigger
              id='timeframe-select'
              className='inline-flex items-center justify-center rounded-sm px-3 py-2 text-sm bg-alpine-50 border border-dark-600 text-alpine-900 hover:bg-dark-700 focus:outline-hidden focus:ring-2 focus:ring-primary-500 focus:border-primary-500'
            >
              <Select.Value />
              <Select.Icon className='ml-2'>
                <ChevronDownIcon size={16} />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content className='z-50 overflow-hidden bg-alpine-50 rounded-md shadow-lg border border-dark-600'>
                <Select.Viewport className='p-1'>
                  {timeframes.map(timeframe => (
                    <Select.Item
                      key={timeframe.value}
                      value={timeframe.value}
                      className='flex select-none items-center px-3 py-2 text-sm text-alpine-900 rounded-sm hover:bg-dark-700 focus:bg-dark-700 cursor-pointer'
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
              <p className='text-sm text-muted-500'>Current Price</p>
              <p className='text-lg font-semibold'>{stats.price.toFixed(5)}</p>
            </div>
          </Card>
          <Card title='24h Change' className='p-4'>
            <div className='flex items-center justify-between'>
              <div>
                <p className='text-sm text-muted-500'>24h Change</p>
                <p
                  className={`text-lg font-semibold ${stats.change >= 0 ? 'text-gain-600' : 'text-loss-600'}`}
                >
                  {stats.change >= 0 ? '+' : ''}
                  {stats.change.toFixed(5)} ({stats.changePercent.toFixed(2)}%)
                </p>
              </div>
            </div>
          </Card>
          <Card title='24h High' className='p-4'>
            <div>
              <p className='text-sm text-muted-500'>24h High</p>
              <p className='text-lg font-semibold text-gain-600'>{stats.high24h.toFixed(5)}</p>
            </div>
          </Card>
          <Card title='24h Low' className='p-4'>
            <div>
              <p className='text-sm text-muted-500'>24h Low</p>
              <p className='text-lg font-semibold text-loss-600'>{stats.low24h.toFixed(5)}</p>
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
            <p className='text-loss-600'>
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
              <p className='text-muted-500 mb-2'>No data available for {selectedInstrument}</p>
              <p className='text-sm text-muted-400'>
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
