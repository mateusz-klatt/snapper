import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { MarketDataState } from '../types/ui'

interface MarketDataStore extends MarketDataState {
  setSelectedInstrument: (instrument: string | null) => void
  setSelectedTimeframe: (timeframe: string) => void
  updateLastPrice: (price: number) => void
  clearMarketData: () => void
}

export const useMarketStore = create<MarketDataStore>()(
  subscribeWithSelector((set, _get) => ({
    selectedInstrument: null,
    selectedTimeframe: '1m',
    lastPrice: null,
    candles: {},
    ticks: {},
    lastUpdate: Date.now(),
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
        selectedInstrument: null,
        selectedTimeframe: '1m',
        lastPrice: null,
        candles: {},
        ticks: {},
        lastUpdate: Date.now(),
      }),
  }))
)
