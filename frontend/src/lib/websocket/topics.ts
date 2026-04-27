import type { WebSocketMessages } from '../../types/ws'

function normalizeInstrument(instrument: string): string {
  return instrument.trim().toUpperCase()
}

export function buildMarketTopic(
  type: 'candles' | 'ticks',
  instrument: string,
  exchange: string = 'kraken',
  timeframe: string = '1m',
  sourceExchange?: string
): string {
  const internalSymbol = normalizeInstrument(instrument)
  const prefix =
    exchange === 'paper' && sourceExchange ? `market.paper.${sourceExchange}` : `market.${exchange}`

  if (type === 'candles') {
    return `${prefix}.${internalSymbol}.candles.${timeframe}`
  }

  return `${prefix}.${internalSymbol}.ticks`
}

export function getMessageTopic(message: WebSocketMessages): string | null {
  const normalizeInst = (value: string | undefined): string | null => {
    if (!value) {
      return null
    }

    return value.toUpperCase()
  }

  const normalizeExchange = (value: string | undefined): string | null => {
    if (!value) {
      return null
    }

    return value.trim().toLowerCase()
  }

  switch (message.type) {
    case 'candle': {
      const instrument = normalizeInst(message.instrument)
      const exchange = normalizeExchange(message.exchange)
      const timeframe = message.timeframe

      if (instrument && exchange && timeframe) {
        return `market.${exchange}.${instrument}.candles.${timeframe}`
      }

      return 'market.'
    }

    case 'tick': {
      const instrument = normalizeInst(message.instrument)
      const exchange = normalizeExchange(message.exchange)

      if (instrument && exchange) {
        return `market.${exchange}.${instrument}.ticks`
      }

      return 'market.'
    }

    case 'order':
      return 'orders.'
    case 'execution':
      return 'executions.'

    case 'signal': {
      const exchange = normalizeExchange(message.exchange)
      const instrument = normalizeInst(message.instrument)

      if (exchange && instrument) {
        return `signals.${exchange}.${instrument}.live`
      }

      return 'signals.'
    }

    case 'heartbeat':
      return 'system.heartbeats.'
    default:
      return message.type
  }
}

const THROTTLED_MESSAGE_TYPES = new Set(['candle', 'order', 'execution', 'position'])

export function shouldThrottle(messageType: string): boolean {
  return THROTTLED_MESSAGE_TYPES.has(messageType)
}

export const MARKET_TOPIC_PREFIX = 'market.'
export const ORDERS_COMMANDS_PREFIX = 'orders.commands.'
export const ORDERS_EVENTS_PREFIX = 'orders.events.'
export const SIGNALS_TOPIC_PREFIX = 'signals.'
export const STRATEGY_TOPIC_PREFIX = 'strategy.'
export const HEARTBEATS_TOPIC_PREFIX = 'system.heartbeats.'
export const AI_REVIEWS_TOPIC_PREFIX = 'ai_reviews.'

export function getSubscriptionTopics(): string[] {
  return [
    MARKET_TOPIC_PREFIX,
    ORDERS_COMMANDS_PREFIX,
    ORDERS_EVENTS_PREFIX,
    SIGNALS_TOPIC_PREFIX,
    STRATEGY_TOPIC_PREFIX,
    HEARTBEATS_TOPIC_PREFIX,
    AI_REVIEWS_TOPIC_PREFIX,
  ]
}
