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
    case 'bar': {
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

    case 'order_status':
      return 'orders.'
    case 'fill':
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

const THROTTLED_MESSAGE_TYPES = new Set(['bar', 'order_status', 'fill', 'position'])

export function shouldThrottle(messageType: string): boolean {
  return THROTTLED_MESSAGE_TYPES.has(messageType)
}

export const MARKET_TOPIC_PREFIX = 'market.'
export const ORDERS_TOPIC_PREFIX = 'orders.'
export const EXECUTIONS_TOPIC_PREFIX = 'executions.'
export const SIGNALS_TOPIC_PREFIX = 'signals.'
export const STRATEGY_TOPIC_PREFIX = 'strategy.'
export const HEARTBEATS_TOPIC_PREFIX = 'system.heartbeats.'

export function getAllTopics(): string[] {
  return [
    MARKET_TOPIC_PREFIX,
    ORDERS_TOPIC_PREFIX,
    EXECUTIONS_TOPIC_PREFIX,
    SIGNALS_TOPIC_PREFIX,
    STRATEGY_TOPIC_PREFIX,
    HEARTBEATS_TOPIC_PREFIX,
  ]
}
