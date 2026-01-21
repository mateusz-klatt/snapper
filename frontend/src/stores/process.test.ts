import { describe, it, expect, beforeEach } from 'vitest'
import { useProcessStore } from './process'

describe('useProcessStore', () => {
  beforeEach(() => {
    useProcessStore.setState({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    })
  })
  describe('initial state', () => {
    it('has correct default values', () => {
      const state = useProcessStore.getState()

      expect(state.feeds).toEqual({})
      expect(state.strategies).toEqual({})
      expect(state.executors).toEqual({})
      expect(state.brokers).toEqual({})
    })
  })
  describe('updateFeedStatus', () => {
    it('adds new feed status', () => {
      useProcessStore.getState().updateFeedStatus('feed_kraken', { running: true })
      expect(useProcessStore.getState().feeds).toEqual({
        feed_kraken: { running: true },
      })
    })
    it('updates existing feed status', () => {
      useProcessStore.getState().updateFeedStatus('feed_kraken', { running: true })
      useProcessStore.getState().updateFeedStatus('feed_kraken', { running: false })
      expect(useProcessStore.getState().feeds.feed_kraken).toEqual({ running: false })
    })
    it('preserves other feeds when updating one', () => {
      useProcessStore.getState().updateFeedStatus('feed_kraken', { running: true })
      useProcessStore.getState().updateFeedStatus('feed_zonda', { running: false })
      expect(useProcessStore.getState().feeds).toEqual({
        feed_kraken: { running: true },
        feed_zonda: { running: false },
      })
    })
  })
  describe('updateStrategyStatus', () => {
    it('adds new strategy status', () => {
      useProcessStore.getState().updateStrategyStatus('macd_btc', { running: true })
      expect(useProcessStore.getState().strategies).toEqual({
        macd_btc: { running: true },
      })
    })
    it('updates existing strategy status', () => {
      useProcessStore.getState().updateStrategyStatus('macd_btc', { running: true })
      useProcessStore.getState().updateStrategyStatus('macd_btc', { running: false })
      expect(useProcessStore.getState().strategies.macd_btc).toEqual({ running: false })
    })
    it('preserves other strategies when updating one', () => {
      useProcessStore.getState().updateStrategyStatus('macd_btc', { running: true })
      useProcessStore.getState().updateStrategyStatus('rsi_eth', { running: true })
      expect(useProcessStore.getState().strategies).toEqual({
        macd_btc: { running: true },
        rsi_eth: { running: true },
      })
    })
  })
  describe('updateExecutorStatus', () => {
    it('adds new executor status', () => {
      useProcessStore.getState().updateExecutorStatus('executor_kraken', { running: true })
      expect(useProcessStore.getState().executors).toEqual({
        executor_kraken: { running: true },
      })
    })
    it('updates existing executor status', () => {
      useProcessStore.getState().updateExecutorStatus('executor_kraken', { running: true })
      useProcessStore.getState().updateExecutorStatus('executor_kraken', { running: false })
      expect(useProcessStore.getState().executors.executor_kraken).toEqual({ running: false })
    })
    it('handles multiple executors', () => {
      useProcessStore.getState().updateExecutorStatus('executor_kraken', { running: true })
      useProcessStore.getState().updateExecutorStatus('executor_zonda', { running: false })
      expect(useProcessStore.getState().executors).toEqual({
        executor_kraken: { running: true },
        executor_zonda: { running: false },
      })
    })
  })
  describe('updateBrokerStatus', () => {
    it('adds new broker status', () => {
      useProcessStore.getState().updateBrokerStatus('broker_kraken', { running: true })
      expect(useProcessStore.getState().brokers).toEqual({
        broker_kraken: { running: true },
      })
    })
    it('updates existing broker status', () => {
      useProcessStore.getState().updateBrokerStatus('broker_kraken', { running: true })
      useProcessStore.getState().updateBrokerStatus('broker_kraken', { running: false })
      expect(useProcessStore.getState().brokers.broker_kraken).toEqual({ running: false })
    })
    it('handles multiple brokers', () => {
      useProcessStore.getState().updateBrokerStatus('broker_kraken', { running: true })
      useProcessStore.getState().updateBrokerStatus('broker_zonda', { running: true })
      expect(useProcessStore.getState().brokers).toEqual({
        broker_kraken: { running: true },
        broker_zonda: { running: true },
      })
    })
  })
  describe('resetProcessStates', () => {
    it('resets all process states to empty', () => {
      useProcessStore.setState({
        feeds: { feed_kraken: { running: true } },
        strategies: { macd_btc: { running: true } },
        executors: { executor_kraken: { running: true } },
        brokers: { broker_kraken: { running: true } },
      })
      useProcessStore.getState().resetProcessStates()
      const state = useProcessStore.getState()

      expect(state.feeds).toEqual({})
      expect(state.strategies).toEqual({})
      expect(state.executors).toEqual({})
      expect(state.brokers).toEqual({})
    })
  })
})
