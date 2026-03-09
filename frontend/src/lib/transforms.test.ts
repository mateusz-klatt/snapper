import { describe, it, expect } from 'vitest'
import {
  orderFromAPI,
  orderFromWS,
  executionFromAPI,
  executionFromWS,
  signalFromAPI,
  signalFromWS,
  positionFromAPI,
  candleFromAPI,
  candleFromWS,
  tickFromWS,
  heartbeatFromWS,
  ordersFromAPI,
  executionsFromAPI,
  signalsFromAPI,
  positionsFromAPI,
  candlesFromAPI,
  isTradeSide,
  isOrderStatus,
  isOrderType,
  safeOrderFromAPI,
  safeExecutionFromAPI,
  safeSignalFromAPI,
} from './transforms'
import type {
  OrderStatus as OrderStatusApi,
  ExecutionRecord,
  TradingSignal,
  PositionSnapshot,
  CandleData,
} from '../types/api'
import type {
  OrderStatusEnvelope,
  FillEnvelope,
  SignalEnvelope,
  CandleEnvelope,
  TickEnvelope,
  HeartbeatEnvelope,
} from '../types/ws'

describe('Order Transformers', () => {
  it('transforms REST API order to canonical entity', () => {
    const apiOrder: OrderStatusApi = {
      id: 1,
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-123',
      exchange_order_id: 'exchange-456',
      created_at: '2026-01-15T10:30:00Z',
      updated_at: '2026-01-15T10:31:00Z',
      side: 'buy',
      type: 'limit',
      size: 0.1,
      price: 50000,
      status: 'filled',
      time_in_force: 'GTC',
      error: null,
    }
    const result = orderFromAPI(apiOrder)

    expect(result.id).toBe(1)
    expect(result.instrument).toBe('BTC/USD')
    expect(result.exchange).toBe('kraken')
    expect(result.side).toBe('buy')
    expect(result.orderType).toBe('limit')
    expect(result.status).toBe('filled')
    expect(result.filledSize).toBe(0)
    expect(result.averagePrice).toBeNull()
    expect(result.createdAt).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.updatedAt).toEqual(new Date('2026-01-15T10:31:00Z'))
  })
  it('transforms WebSocket order to canonical entity', () => {
    const wsOrder: OrderStatusEnvelope = {
      type: 'order_status',
      id: '2',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      side: 'sell',
      order_type: 'market',
      size: 1.5,
      filled_size: 0,
      price: null,
      status: 'new',
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
    }
    const result = orderFromWS(wsOrder)

    expect(result.id).toBe('2')
    expect(result.instrument).toBe('ETH/USD')
    expect(result.exchange).toBe('kraken')
    expect(result.side).toBe('sell')
    expect(result.orderType).toBe('market')
    expect(result.status).toBe('new')
    expect(result.filledSize).toBe(0)
    expect(result.createdAt).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.updatedAt).toBeNull()
  })
  it('throws on missing created_at in WebSocket order', () => {
    const wsOrder = {
      type: 'order_status',
      id: '2',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      side: 'sell',
      order_type: 'market',
      size: 1.5,
      price: null,
      status: 'new',
      updated_at: null,
    } as unknown as OrderStatusEnvelope

    expect(() => orderFromWS(wsOrder)).toThrow(
      'OrderStatusEnvelope missing required field: created_at'
    )
  })
  it('normalizes unknown order type to limit', () => {
    const apiOrder = {
      id: 1,
      instrument: 'BTC/USD',
      exchange: '',
      client_order_id: null,
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      type: 'unknown_type',
      price: null,
      size: 1,
      status: 'new',
      time_in_force: null,
      error: null,
    } as unknown as OrderStatusApi

    expect(() => orderFromAPI(apiOrder)).toThrow(
      'Invalid order type: "unknown_type". Expected "market", "limit", "stop", or "stop_limit".'
    )
  })
  it('passes through order status without validation', () => {
    const apiOrder = {
      id: 1,
      instrument: 'BTC/USD',
      exchange: '',
      client_order_id: null,
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      type: 'market',
      price: null,
      size: 1,
      status: 'unknown_status',
      time_in_force: null,
      error: null,
    } as unknown as OrderStatusApi
    const result = orderFromAPI(apiOrder)

    expect(result.status).toBe('unknown_status')
  })
})
describe('Execution Transformers', () => {
  it('transforms REST API execution to canonical entity', () => {
    const apiExecution: ExecutionRecord = {
      id: 10,
      order_id: 1,
      timestamp: '2026-01-15T10:30:00Z',
      price: 50000,
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      instrument: 'BTC/USD',
      side: 'buy',
      exchange: 'kraken',
    }
    const result = executionFromAPI(apiExecution)

    expect(result.id).toBe(10)
    expect(result.orderId).toBe(1)
    expect(result.exchange).toBe('kraken')
    expect(result.instrument).toBe('BTC/USD')
    expect(result.side).toBe('buy')
    expect(result.feeAsset).toBe('USD')
    expect(result.status).toBe('filled')
    expect(result.executedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('transforms WebSocket execution to canonical entity', () => {
    const wsExecution: FillEnvelope = {
      type: 'fill',
      id: '11',
      order_id: '2',
      exchange: 'zonda',
      instrument: 'ETH/PLN',
      side: 'sell',
      size: 2,
      price: 15000,
      fee: 15,
      fee_asset: 'PLN',
      status: 'filled',
      executed_at: '2026-01-15T10:30:00Z',
    }
    const result = executionFromWS(wsExecution)

    expect(result.id).toBe('11')
    expect(result.orderId).toBe('2')
    expect(result.exchange).toBe('zonda')
    expect(result.instrument).toBe('ETH/PLN')
    expect(result.feeAsset).toBe('PLN')
    expect(result.status).toBe('filled')
    expect(result.executedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('throws on missing executed_at in WebSocket execution', () => {
    const wsExecution = {
      type: 'fill',
      id: '11',
      order_id: '2',
      exchange: 'zonda',
      instrument: 'ETH/PLN',
      side: 'sell',
      size: 2,
      price: 15000,
      fee: 15,
      fee_asset: 'PLN',
    } as unknown as FillEnvelope

    expect(() => executionFromWS(wsExecution)).toThrow(
      'FillEnvelope missing required field: executed_at'
    )
  })
})
describe('Signal Transformers', () => {
  it('transforms REST API signal to canonical entity', () => {
    const apiSignal: TradingSignal = {
      id: 100,
      instrument: 'BTC/USD',
      exchange: 'kraken',
      timestamp: '2026-01-15T10:30:00Z',
      side: 'buy',
      strength: 0.85,
      reason: 'RSI oversold',
      strategy_name: 'momentum_v1',
      price: 49500,
    }
    const result = signalFromAPI(apiSignal)

    expect(result.id).toBe(100)
    expect(result.exchange).toBe('kraken')
    expect(result.instrument).toBe('BTC/USD')
    expect(result.strategyName).toBe('momentum_v1')
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('transforms WebSocket signal to canonical entity', () => {
    const wsSignal: SignalEnvelope = {
      type: 'signal',
      id: '101',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = signalFromWS(wsSignal)

    expect(result.id).toBe('101')
    expect(result.exchange).toBe('kraken')
    expect(result.strategyName).toBeNull()
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('sets missing WebSocket signal id to null', () => {
    const wsSignal: SignalEnvelope = {
      type: 'signal',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = signalFromWS(wsSignal)

    expect(result.id).toBeNull()
  })
  it('throws on missing timestamp in WebSocket signal', () => {
    const wsSignal = {
      type: 'signal',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
    } as unknown as SignalEnvelope

    expect(() => signalFromWS(wsSignal)).toThrow('SignalEnvelope missing required field: timestamp')
  })
})
describe('Position Transformers', () => {
  it('transforms REST API position to canonical entity', () => {
    const apiPosition: PositionSnapshot = {
      id: 50,
      instrument: 'BTC/USD',
      exchange: 'kraken',
      quantity: 1.5,
      average_price: 48000,
      unrealized_pnl: 3000,
      realized_pnl: 500,
      updated_at: '2026-01-15T10:30:00Z',
    }
    const result = positionFromAPI(apiPosition)

    expect(result.id).toBe(50)
    expect(result.instrument).toBe('BTC/USD')
    expect(result.exchange).toBe('kraken')
    expect(result.averagePrice).toBe(48000)
    expect(result.unrealizedPnl).toBe(3000)
    expect(result.realizedPnl).toBe(500)
    expect(result.updatedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
})
describe('Candle Transformers', () => {
  it('transforms REST API candle to canonical entity', () => {
    const apiCandle: CandleData = {
      instrument: 'BTC/USD',
      timeframe: '1h',
      open_at: '2026-01-15T10:00:00Z',
      open: 49000,
      high: 50500,
      low: 48500,
      close: 50000,
      volume: 1000,
      vwap: 49750,
      trades: 5000,
    }
    const result = candleFromAPI(apiCandle)

    expect(result.instrument).toBe('BTC/USD')
    expect(result.timeframe).toBe('1h')
    expect(result.vwap).toBe(49750)
    expect(result.trades).toBe(5000)
    expect(result.openAt).toEqual(new Date('2026-01-15T10:00:00Z'))
  })
  it('transforms WebSocket candle to canonical entity', () => {
    const wsCandle: CandleEnvelope = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    }
    const result = candleFromWS(wsCandle)

    expect(result.instrument).toBe('ETH/USD')
    expect(result.timeframe).toBe('5m')
    expect(result.vwap).toBeUndefined()
    expect(result.openAt).toEqual(new Date('2026-01-15T10:00:00Z'))
    expect(result.timestamp).toBeUndefined()
  })
  it('includes envelope timestamp when present in WebSocket candle', () => {
    const wsCandle: CandleEnvelope = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
      timestamp: '2026-01-15T10:00:01Z',
    }
    const result = candleFromWS(wsCandle)

    expect(result.openAt).toEqual(new Date('2026-01-15T10:00:00Z'))
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:00:01Z'))
  })
  it('throws on missing timeframe in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: null,
      open: 3000,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleEnvelope

    expect(() => candleFromWS(wsCandle)).toThrow('CandleEnvelope missing required field: timeframe')
  })
  it('throws on missing open in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: null,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleEnvelope

    expect(() => candleFromWS(wsCandle)).toThrow('CandleEnvelope missing required field: open')
  })
  it('throws on missing high in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: null,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleEnvelope

    expect(() => candleFromWS(wsCandle)).toThrow('CandleEnvelope missing required field: high')
  })
  it('throws on missing low in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: null,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleEnvelope

    expect(() => candleFromWS(wsCandle)).toThrow('CandleEnvelope missing required field: low')
  })
  it('throws on missing close in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: 2980,
      close: null,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleEnvelope

    expect(() => candleFromWS(wsCandle)).toThrow('CandleEnvelope missing required field: close')
  })
})
describe('Tick Transformers', () => {
  it('transforms WebSocket tick to canonical entity', () => {
    const wsTick: TickEnvelope = {
      type: 'tick',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: 49990,
      ask: 50010,
      last: 50000,
      volume: 100,
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = tickFromWS(wsTick)

    expect(result.instrument).toBe('BTC/USD')
    expect(result.bid).toBe(49990)
    expect(result.ask).toBe(50010)
    expect(result.last).toBe(50000)
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('handles missing last and nullable bid/ask', () => {
    const wsTick: TickEnvelope = {
      type: 'tick',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: null,
      ask: null,
      volume: 0,
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = tickFromWS(wsTick)

    expect(result.bid).toBeNull()
    expect(result.ask).toBeNull()
    expect(result.last).toBeUndefined()
    expect(result.volume).toBe(0)
  })
  it('throws on missing timestamp in WebSocket tick', () => {
    const wsTick = {
      type: 'tick',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: 49990,
      ask: 50010,
      volume: 100,
    } as unknown as TickEnvelope

    expect(() => tickFromWS(wsTick)).toThrow('TickEnvelope missing required field: timestamp')
  })
})
describe('Heartbeat Transformers', () => {
  it('transforms WebSocket heartbeat to canonical entity', () => {
    const wsHeartbeat: HeartbeatEnvelope = {
      type: 'heartbeat',
      component: 'executor_kraken',
      status: 'healthy',
      meta: { version: '1.0' },
      sequence: 1,
      timestamp: '2026-01-15T10:30:00Z',
      lag_ms: 15,
    }
    const result = heartbeatFromWS(wsHeartbeat)

    expect(result.component).toBe('executor_kraken')
    expect(result.status).toBe('healthy')
    expect(result.lagMs).toBe(15)
    expect(result.sequence).toBe(1)
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('handles heartbeat with different sequence', () => {
    const wsHeartbeat: HeartbeatEnvelope = {
      type: 'heartbeat',
      component: 'executor_kraken',
      status: 'healthy',
      sequence: 42,
      timestamp: '2026-01-15T10:30:00Z',
      lag_ms: 15,
    }
    const result = heartbeatFromWS(wsHeartbeat)

    expect(result.sequence).toBe(42)
  })
  it('throws on missing timestamp in WebSocket heartbeat', () => {
    const wsHeartbeat = {
      type: 'heartbeat',
      component: 'executor_kraken',
      status: 'healthy',
      lag_ms: 15,
    } as unknown as HeartbeatEnvelope

    expect(() => heartbeatFromWS(wsHeartbeat)).toThrow(
      'HeartbeatEnvelope missing required field: timestamp'
    )
  })
})
describe('Batch Transformers', () => {
  it('transforms array of orders', () => {
    const apiOrders: OrderStatusApi[] = [
      {
        id: 1,
        instrument: 'BTC/USD',
        exchange: 'kraken',
        client_order_id: null,
        exchange_order_id: null,
        created_at: '2026-01-15T10:30:00Z',
        updated_at: null,
        side: 'buy',
        type: 'market',
        price: null,
        size: 1,
        status: 'new',
        time_in_force: null,
        error: null,
      },
      {
        id: 2,
        instrument: 'ETH/USD',
        exchange: 'zonda',
        client_order_id: null,
        exchange_order_id: null,
        created_at: '2026-01-15T10:31:00Z',
        updated_at: null,
        side: 'sell',
        type: 'limit',
        price: 3000,
        size: 2,
        status: 'open',
        time_in_force: null,
        error: null,
      },
    ]
    const result = ordersFromAPI(apiOrders)

    expect(result).toHaveLength(2)
    expect(result[0].id).toBe(1)
    expect(result[1].id).toBe(2)
  })
  it('transforms array of executions', () => {
    const apiExecutions: ExecutionRecord[] = [
      {
        id: 10,
        order_id: 1,
        timestamp: '2026-01-15T10:30:00Z',
        price: 50000,
        size: 0.1,
        fee: 5,
        fee_asset: 'USD',
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
      },
    ]
    const result = executionsFromAPI(apiExecutions)

    expect(result).toHaveLength(1)
    expect(result[0].id).toBe(10)
  })
  it('transforms array of signals', () => {
    const apiSignals: TradingSignal[] = [
      {
        id: 100,
        instrument: 'BTC/USD',
        exchange: 'kraken',
        timestamp: '2026-01-15T10:30:00Z',
        side: 'buy',
        strength: 0.85,
        reason: 'RSI oversold',
        strategy_name: 'momentum_v1',
        price: 49500,
      },
    ]
    const result = signalsFromAPI(apiSignals)

    expect(result).toHaveLength(1)
    expect(result[0].strategyName).toBe('momentum_v1')
  })
  it('transforms array of positions', () => {
    const apiPositions: PositionSnapshot[] = [
      {
        id: 50,
        instrument: 'BTC/USD',
        exchange: 'kraken',
        quantity: 1.5,
        average_price: 48000,
        unrealized_pnl: 3000,
        realized_pnl: 500,
        updated_at: '2026-01-15T10:30:00Z',
      },
    ]
    const result = positionsFromAPI(apiPositions)

    expect(result).toHaveLength(1)
    expect(result[0].averagePrice).toBe(48000)
  })
  it('transforms array of candles', () => {
    const apiCandles: CandleData[] = [
      {
        instrument: 'BTC/USD',
        timeframe: '1h',
        open_at: '2026-01-15T10:00:00Z',
        open: 49000,
        high: 50500,
        low: 48500,
        close: 50000,
        volume: 1000,
      },
    ]
    const result = candlesFromAPI(apiCandles)

    expect(result).toHaveLength(1)
    expect(result[0].timeframe).toBe('1h')
  })
})
describe('Type Guards', () => {
  it('validates TradeSide', () => {
    expect(isTradeSide('buy')).toBe(true)
    expect(isTradeSide('sell')).toBe(true)
    expect(isTradeSide('hold')).toBe(false)
    expect(isTradeSide(123)).toBe(false)
    expect(isTradeSide(null)).toBe(false)
  })
  it('validates OrderStatus', () => {
    expect(isOrderStatus('new')).toBe(true)
    expect(isOrderStatus('submitted')).toBe(true)
    expect(isOrderStatus('open')).toBe(true)
    expect(isOrderStatus('filled')).toBe(true)
    expect(isOrderStatus('partially_filled')).toBe(true)
    expect(isOrderStatus('cancelled')).toBe(true)
    expect(isOrderStatus('rejected')).toBe(true)
    expect(isOrderStatus('unknown')).toBe(false)
    expect(isOrderStatus(null)).toBe(false)
  })
  it('validates OrderType', () => {
    expect(isOrderType('market')).toBe(true)
    expect(isOrderType('limit')).toBe(true)
    expect(isOrderType('stop')).toBe(true)
    expect(isOrderType('stop_limit')).toBe(true)
    expect(isOrderType('trailing_stop')).toBe(false)
    expect(isOrderType(null)).toBe(false)
  })
})
describe('Safe API Transformers', () => {
  it('safeOrderFromAPI returns order on valid input', () => {
    const apiOrder: OrderStatusApi = {
      id: 1,
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-123',
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      type: 'limit',
      size: 0.1,
      price: 50000,
      status: 'filled',
      time_in_force: null,
      error: null,
    }
    const result = safeOrderFromAPI(apiOrder)

    expect(result).not.toBeNull()
    expect(result?.id).toBe(1)
  })
  it('safeOrderFromAPI returns null on invalid side', () => {
    const apiOrder = {
      id: 1,
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: null,
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'invalid_side',
      type: 'limit',
      size: 0.1,
      status: 'filled',
      time_in_force: null,
      error: null,
    } as unknown as OrderStatusApi
    const result = safeOrderFromAPI(apiOrder)

    expect(result).toBeNull()
  })
  it('safeExecutionFromAPI returns execution on valid input', () => {
    const apiExecution: ExecutionRecord = {
      id: 1,
      order_id: 1,
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'buy',
      price: 50000,
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = safeExecutionFromAPI(apiExecution)

    expect(result).not.toBeNull()
    expect(result?.id).toBe(1)
  })
  it('safeExecutionFromAPI returns null on invalid side', () => {
    const apiExecution = {
      id: 'exec-1',
      order_id: 'order-1',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'unknown',
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      timestamp: '2026-01-15T10:30:00Z',
    } as unknown as ExecutionRecord
    const result = safeExecutionFromAPI(apiExecution)

    expect(result).toBeNull()
  })
  it('safeSignalFromAPI returns signal on valid input', () => {
    const apiSignal: TradingSignal = {
      id: 1,
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'buy',
      strength: 0.8,
      reason: 'Test signal',
      strategy_name: null,
      price: null,
      timestamp: '2026-01-15T10:30:00Z',
    }
    const result = safeSignalFromAPI(apiSignal)

    expect(result).not.toBeNull()
    expect(result?.id).toBe(1)
  })
  it('safeSignalFromAPI returns null on invalid side', () => {
    const apiSignal = {
      id: 'signal-1',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'hold',
      strength: 0.8,
      reason: 'Test signal',
      strategy_name: null,
      price: null,
      timestamp: '2026-01-15T10:30:00Z',
    } as unknown as TradingSignal
    const result = safeSignalFromAPI(apiSignal)

    expect(result).toBeNull()
  })
})
