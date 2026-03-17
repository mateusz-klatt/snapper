import { describe, it, expect, beforeEach } from 'vitest'
import { useMarketStore } from './market'

describe('useMarketStore', () => {
  beforeEach(() => {
    useMarketStore.setState({
      selectedExchange: 'kraken',
      selectedInstrument: 'EUR-USD',
      selectedTimeframe: '1h',
      lastPrice: null,
      lastUpdate: Date.now(),
    })
  })
  describe('initial state', () => {
    it('has correct default values', () => {
      const state = useMarketStore.getState()

      expect(state.selectedExchange).toBe('kraken')
      expect(state.selectedInstrument).toBe('EUR-USD')
      expect(state.selectedTimeframe).toBe('1h')
      expect(state.lastPrice).toBeNull()
    })
  })
  describe('setSelectedExchange', () => {
    it('sets selected exchange', () => {
      useMarketStore.getState().setSelectedExchange('kraken')
      expect(useMarketStore.getState().selectedExchange).toBe('kraken')
    })
    it('resets selectedInstrument and lastPrice when changing exchange', () => {
      useMarketStore.setState({ selectedInstrument: 'BTC-USD', lastPrice: 50000 })
      useMarketStore.getState().setSelectedExchange('binance')
      expect(useMarketStore.getState().selectedInstrument).toBeNull()
      expect(useMarketStore.getState().lastPrice).toBeNull()
    })
    it('can set exchange to null', () => {
      useMarketStore.getState().setSelectedExchange('kraken')
      useMarketStore.getState().setSelectedExchange(null)
      expect(useMarketStore.getState().selectedExchange).toBeNull()
    })
  })
  describe('setSelectedInstrument', () => {
    it('sets selected instrument', () => {
      useMarketStore.getState().setSelectedInstrument('BTC-USD')
      expect(useMarketStore.getState().selectedInstrument).toBe('BTC-USD')
    })
    it('resets lastPrice when changing instrument', () => {
      useMarketStore.setState({ lastPrice: 50000 })
      useMarketStore.getState().setSelectedInstrument('ETH-USD')
      expect(useMarketStore.getState().lastPrice).toBeNull()
    })
    it('can set instrument to null', () => {
      useMarketStore.getState().setSelectedInstrument('BTC-USD')
      useMarketStore.getState().setSelectedInstrument(null)
      expect(useMarketStore.getState().selectedInstrument).toBeNull()
    })
  })
  describe('setSelectedTimeframe', () => {
    it('sets selected timeframe', () => {
      useMarketStore.getState().setSelectedTimeframe('1d')
      expect(useMarketStore.getState().selectedTimeframe).toBe('1d')
    })
    it('can change timeframe multiple times', () => {
      useMarketStore.getState().setSelectedTimeframe('5m')
      useMarketStore.getState().setSelectedTimeframe('15m')
      useMarketStore.getState().setSelectedTimeframe('4h')
      expect(useMarketStore.getState().selectedTimeframe).toBe('4h')
    })
  })
  describe('updateLastPrice', () => {
    it('updates last price', () => {
      useMarketStore.getState().updateLastPrice(45000)
      expect(useMarketStore.getState().lastPrice).toBe(45000)
    })
    it('can update price multiple times', () => {
      useMarketStore.getState().updateLastPrice(45000)
      useMarketStore.getState().updateLastPrice(45100)
      expect(useMarketStore.getState().lastPrice).toBe(45100)
    })
  })
})
