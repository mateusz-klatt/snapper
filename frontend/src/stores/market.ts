import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { MarketDataState } from '../types/ui'

interface MarketDataStore extends MarketDataState {
  setSelectedExchange: (exchange: string | null) => void
  setSelectedInstrument: (instrument: string | null) => void
  setSelectedTimeframe: (timeframe: string) => void
  updateLastPrice: (price: number) => void
  clearMarketData: () => void
}

export const useMarketStore = create<MarketDataStore>()(
  subscribeWithSelector((set, _get) => ({
    selectedExchange: 'kraken',
    selectedInstrument: 'EUR-USD',
    selectedTimeframe: '1m',
    lastPrice: null,
    candles: {},
    ticks: {},
    lastUpdate: Date.now(),
    setSelectedExchange: exchange => {
      set({
        selectedExchange: exchange,
        selectedInstrument: null,
        lastPrice: null,
      })
    },
    setSelectedInstrument: instrument => {
      set({
        selectedInstrument: instrument,
        lastPrice: null,
      })
    },
    setSelectedTimeframe: timeframe => set({ selectedTimeframe: timeframe }),
    updateLastPrice: price => set({ lastPrice: price }),
    clearMarketData: () =>
      set({
        selectedExchange: null,
        selectedInstrument: null,
        selectedTimeframe: '1m',
        lastPrice: null,
        candles: {},
        ticks: {},
        lastUpdate: Date.now(),
      }),
  }))
)
