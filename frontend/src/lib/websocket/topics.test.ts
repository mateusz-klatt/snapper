import { describe, it, expect } from 'vitest'
import {
  buildMarketTopic,
  getMessageTopic,
  shouldThrottle,
  getSubscriptionTopics,
  MARKET_TOPIC_PREFIX,
  ORDERS_COMMANDS_PREFIX,
  ORDERS_EVENTS_PREFIX,
  SIGNALS_TOPIC_PREFIX,
  STRATEGY_TOPIC_PREFIX,
  HEARTBEATS_TOPIC_PREFIX,
  AI_REVIEWS_TOPIC_PREFIX,
} from './topics'
import {
  createCandle,
  createTick,
  createSignal,
  createOrder,
  createExecution,
  createHeartbeat,
  createError,
} from '@/test/wsMessageFactories'

describe('topics', () => {
  describe('buildMarketTopic', () => {
    it('builds candles topic with all parameters', () => {
      expect(buildMarketTopic('candles', 'BTC-USD', 'kraken', '1m')).toBe(
        'market.kraken.BTC-USD.candles.1m'
      )
    })
    it('builds candles topic with default exchange and timeframe', () => {
      expect(buildMarketTopic('candles', 'ETH-USD')).toBe('market.kraken.ETH-USD.candles.1m')
    })
    it('converts instrument to uppercase', () => {
      expect(buildMarketTopic('candles', 'btc-usd', 'kraken', '5m')).toBe(
        'market.kraken.BTC-USD.candles.5m'
      )
    })
    it('builds ticks topic (4 segments)', () => {
      expect(buildMarketTopic('ticks', 'BTC-USD', 'kraken')).toBe('market.kraken.BTC-USD.ticks')
    })
    it('builds ticks topic with default exchange', () => {
      expect(buildMarketTopic('ticks', 'ETH-EUR')).toBe('market.kraken.ETH-EUR.ticks')
    })
    it('trims whitespace from instrument', () => {
      expect(buildMarketTopic('candles', '  BTC-USD  ', 'kraken', '1m')).toBe(
        'market.kraken.BTC-USD.candles.1m'
      )
    })
    it('builds paper candles topic with source exchange', () => {
      expect(buildMarketTopic('candles', 'BTC-USD', 'paper', '1m', 'kraken')).toBe(
        'market.paper.kraken.BTC-USD.candles.1m'
      )
    })
    it('builds paper ticks topic with source exchange', () => {
      expect(buildMarketTopic('ticks', 'AAPL', 'paper', '1m', 'polygon')).toBe(
        'market.paper.polygon.AAPL.ticks'
      )
    })
    it('ignores sourceExchange when exchange is not paper', () => {
      expect(buildMarketTopic('candles', 'BTC-USD', 'kraken', '1m', 'polygon')).toBe(
        'market.kraken.BTC-USD.candles.1m'
      )
    })
    it('uses live format for paper without sourceExchange', () => {
      expect(buildMarketTopic('candles', 'BTC-USD', 'paper', '1m')).toBe(
        'market.paper.BTC-USD.candles.1m'
      )
    })
  })
  describe('getMessageTopic', () => {
    it('builds topic from candle message data', () => {
      const message = createCandle({
        instrument: 'BTC-USD',
        exchange: 'KRAKEN',
        timeframe: '1m',
      })

      expect(getMessageTopic(message as never)).toBe('market.kraken.BTC-USD.candles.1m')
    })
    it('returns fallback for candle with missing data', () => {
      const message = createCandle({
        instrument: '',
        exchange: '',
        timeframe: '',
      })

      expect(getMessageTopic(message as never)).toBe('market.')
    })
    it('builds topic from tick message data', () => {
      const message = createTick({
        instrument: 'ETH-EUR',
        exchange: 'KRAKEN',
      })

      expect(getMessageTopic(message as never)).toBe('market.kraken.ETH-EUR.ticks')
    })
    it('returns fallback for tick with missing data', () => {
      const message = createTick({
        instrument: '',
        exchange: '',
      })

      expect(getMessageTopic(message as never)).toBe('market.')
    })
    it('returns orders prefix for order message', () => {
      const message = createOrder()

      expect(getMessageTopic(message as never)).toBe('orders.')
    })
    it('returns executions prefix for execution message', () => {
      const message = createExecution()

      expect(getMessageTopic(message as never)).toBe('executions.')
    })
    it('builds topic from signal message data', () => {
      const message = createSignal({
        exchange: 'KRAKEN',
        instrument: 'BTC-USD',
      })

      expect(getMessageTopic(message as never)).toBe('signals.kraken.BTC-USD.live')
    })
    it('returns fallback for signal with missing data', () => {
      const message = createSignal({
        exchange: '',
        instrument: '',
      })

      expect(getMessageTopic(message as never)).toBe('signals.')
    })
    it('returns heartbeats prefix for heartbeat message', () => {
      const message = createHeartbeat()

      expect(getMessageTopic(message as never)).toBe('system.heartbeats.')
    })
    it('returns message type for unknown types', () => {
      const message = createError({ message: 'test' })

      expect(getMessageTopic(message as never)).toBe('error')
    })
  })
  describe('shouldThrottle', () => {
    it('returns true for candle messages', () => {
      expect(shouldThrottle('candle')).toBe(true)
    })
    it('returns true for order messages', () => {
      expect(shouldThrottle('order')).toBe(true)
    })
    it('returns true for execution messages', () => {
      expect(shouldThrottle('execution')).toBe(true)
    })
    it('returns true for position messages', () => {
      expect(shouldThrottle('position')).toBe(true)
    })
    it('returns false for heartbeat messages', () => {
      expect(shouldThrottle('heartbeat')).toBe(false)
    })
    it('returns false for signal messages', () => {
      expect(shouldThrottle('signal')).toBe(false)
    })
    it('returns false for unknown message types', () => {
      expect(shouldThrottle('unknown')).toBe(false)
    })
  })
  describe('topic constants', () => {
    it('exports MARKET_TOPIC_PREFIX', () => {
      expect(MARKET_TOPIC_PREFIX).toBe('market.')
    })
    it('exports ORDERS_COMMANDS_PREFIX for subscription', () => {
      expect(ORDERS_COMMANDS_PREFIX).toBe('orders.commands.')
    })
    it('exports ORDERS_EVENTS_PREFIX for subscription', () => {
      expect(ORDERS_EVENTS_PREFIX).toBe('orders.events.')
    })
    it('exports SIGNALS_TOPIC_PREFIX', () => {
      expect(SIGNALS_TOPIC_PREFIX).toBe('signals.')
    })
    it('exports STRATEGY_TOPIC_PREFIX', () => {
      expect(STRATEGY_TOPIC_PREFIX).toBe('strategy.')
    })
    it('exports HEARTBEATS_TOPIC_PREFIX', () => {
      expect(HEARTBEATS_TOPIC_PREFIX).toBe('system.heartbeats.')
    })
    it('exports AI_REVIEWS_TOPIC_PREFIX', () => {
      expect(AI_REVIEWS_TOPIC_PREFIX).toBe('ai_reviews.')
    })
  })
  describe('getSubscriptionTopics', () => {
    it('returns array of valid subscription prefixes', () => {
      const topics = getSubscriptionTopics()

      expect(topics).toEqual([
        'market.',
        'orders.commands.',
        'orders.events.',
        'signals.',
        'strategy.',
        'system.heartbeats.',
        'ai_reviews.',
      ])
    })
    it('returns 7 topics', () => {
      expect(getSubscriptionTopics()).toHaveLength(7)
    })
    it('includes ai_reviews prefix so AI delegate consumers receive request/decision_ack/caps_violation frames', () => {
      expect(getSubscriptionTopics()).toContain(AI_REVIEWS_TOPIC_PREFIX)
    })
  })
})
