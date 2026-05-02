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
  orderDataFromEnvelope,
  executionDataFromEnvelope,
  signalDataFromEnvelope,
} from './transforms'
import type { PositionData } from '../types/api'
import type {
  OrderData,
  ExecutionData,
  SignalData,
  CandleData,
  TickData,
  HeartbeatData,
} from '../types/ws'

describe('Order Transformers', () => {
  it('transforms REST API order to canonical entity', () => {
    const apiOrder: OrderData = {
      type: 'order',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-123',
      exchange_order_id: 'exchange-456',
      created_at: '2026-01-15T10:30:00Z',
      updated_at: '2026-01-15T10:31:00Z',
      side: 'buy',
      order_type: 'limit',
      size: 0.1,
      filled_size: 0,
      price: 50000,
      status: 'filled',
      time_in_force: 'GTC',
      error: null,
    }
    const result = orderFromAPI(apiOrder)

    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.clientOrderId).toBe('client-123')
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
  it('falls back to current date when created_at is undefined in API order', () => {
    const apiOrder = {
      timestamp: '2026-01-15T10:30:00Z',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-123',
      side: 'buy',
      order_type: 'limit',
      size: 0.1,
      filled_size: 0,
      price: 50000,
      status: 'open',
    } as unknown as OrderData
    const before = new Date()
    const result = orderFromAPI(apiOrder)

    expect(result.createdAt).toBeDefined()
    expect((result.createdAt as Date).getTime()).toBeGreaterThanOrEqual(before.getTime())
  })
  it('transforms WebSocket order to canonical entity', () => {
    const wsOrder: OrderData = {
      type: 'order',
      sequence_id: 0,
      public_id: 'ws-order-uuid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-2',
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

    expect(result.publicId).toBe('ws-order-uuid')
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.clientOrderId).toBe('client-2')
    expect(result.instrument).toBe('ETH/USD')
    expect(result.exchange).toBe('kraken')
    expect(result.side).toBe('sell')
    expect(result.orderType).toBe('market')
    expect(result.status).toBe('new')
    expect(result.filledSize).toBe(0)
    expect(result.createdAt).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.updatedAt).toBeNull()
  })
  it('transforms WebSocket order with updated_at to canonical entity', () => {
    const wsOrder: OrderData = {
      type: 'order',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-3',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      order_type: 'limit',
      size: 1,
      filled_size: 0.5,
      price: 50000,
      status: 'partially_filled',
      created_at: '2026-01-15T10:30:00Z',
      updated_at: '2026-01-15T11:00:00Z',
    }
    const result = orderFromWS(wsOrder)

    expect(result.updatedAt).toEqual(new Date('2026-01-15T11:00:00Z'))
  })
  it('throws on missing created_at in WebSocket order', () => {
    const wsOrder = {
      type: 'order',
      timestamp: '2026-01-15T10:30:00Z',
      client_order_id: 'client-2',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      side: 'sell',
      order_type: 'market',
      size: 1.5,
      price: null,
      status: 'new',
      updated_at: null,
    } as unknown as OrderData

    expect(() => orderFromWS(wsOrder)).toThrow('OrderData missing required field: created_at')
  })
  it('normalizes unknown order type to limit', () => {
    const apiOrder = {
      timestamp: '2026-01-15T10:30:00Z',
      instrument: 'BTC/USD',
      exchange: '',
      client_order_id: 'client-1',
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      order_type: 'unknown_type',
      price: null,
      size: 1,
      filled_size: 0,
      status: 'new',
      time_in_force: null,
      error: null,
    } as unknown as OrderData

    expect(() => orderFromAPI(apiOrder)).toThrow(
      'Invalid order type: "unknown_type". Expected "market", "limit", "stop", or "stop_limit".'
    )
  })
  it('throws when API order has no timestamp', () => {
    const apiOrder = {
      type: 'order',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-1',
      side: 'buy',
      order_type: 'market',
      size: 1,
      filled_size: 0,
      status: 'new',
      created_at: '2026-01-15T10:30:00Z',
    } as unknown as OrderData

    expect(() => orderFromAPI(apiOrder)).toThrow('OrderData missing required field: timestamp')
  })
  it('passes through order status without validation', () => {
    const apiOrder = {
      timestamp: '2026-01-15T10:30:00Z',
      instrument: 'BTC/USD',
      exchange: '',
      client_order_id: 'client-1',
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      order_type: 'market',
      price: null,
      size: 1,
      filled_size: 0,
      status: 'unknown_status',
      time_in_force: null,
      error: null,
    } as unknown as OrderData
    const result = orderFromAPI(apiOrder)

    expect(result.status).toBe('unknown_status')
  })
})
describe('Execution Transformers', () => {
  it('transforms REST API execution to canonical entity', () => {
    const apiExecution: ExecutionData = {
      type: 'execution',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-10',
      executed_at: '2026-01-15T10:30:00Z',
      price: 50000,
      size: 0.1,
      last_size: 0.1,
      last_price: 50000,
      fee: 5,
      fee_asset: 'USD',
      instrument: 'BTC/USD',
      side: 'buy',
      exchange: 'kraken',
      status: 'filled',
    }
    const result = executionFromAPI(apiExecution)

    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.clientOrderId).toBe('client-10')
    expect(result.exchange).toBe('kraken')
    expect(result.instrument).toBe('BTC/USD')
    expect(result.side).toBe('buy')
    expect(result.feeAsset).toBe('USD')
    expect(result.status).toBe('filled')
    expect(result.executedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('transforms WebSocket execution to canonical entity', () => {
    const wsExecution: ExecutionData = {
      type: 'execution',
      sequence_id: 0,
      public_id: 'ws-exec-uuid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-11',
      exchange: 'walutomat',
      instrument: 'EUR/PLN',
      side: 'sell',
      size: 2,
      price: 15000,
      last_size: 2,
      last_price: 15000,
      fee: 15,
      fee_asset: 'PLN',
      status: 'filled',
      executed_at: '2026-01-15T10:30:00Z',
    }
    const result = executionFromWS(wsExecution)

    expect(result.publicId).toBe('ws-exec-uuid')
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.clientOrderId).toBe('client-11')
    expect(result.exchange).toBe('walutomat')
    expect(result.instrument).toBe('EUR/PLN')
    expect(result.feeAsset).toBe('PLN')
    expect(result.status).toBe('filled')
    expect(result.executedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('throws when WS execution has no timestamp', () => {
    const wsExecution = {
      type: 'execution',
      client_order_id: 'client-notimestamp',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'buy',
      size: 1,
      price: 50000,
      fee: 0.1,
      fee_asset: 'USD',
      status: 'filled',
      executed_at: '2026-01-15T10:30:00Z',
    } as unknown as ExecutionData

    expect(() => executionFromWS(wsExecution)).toThrow(
      'ExecutionData missing required field: timestamp'
    )
  })
  it('throws on missing executed_at in WebSocket execution', () => {
    const wsExecution = {
      type: 'execution',
      timestamp: '2026-01-15T10:30:00Z',
      client_order_id: 'client-11',
      exchange: 'walutomat',
      instrument: 'EUR/PLN',
      side: 'sell',
      size: 2,
      price: 15000,
      fee: 15,
      fee_asset: 'PLN',
    } as unknown as ExecutionData

    expect(() => executionFromWS(wsExecution)).toThrow(
      'ExecutionData missing required field: executed_at'
    )
  })
  it('throws when API execution has no timestamp', () => {
    const apiExecution = {
      type: 'execution',
      client_order_id: 'client-10',
      executed_at: '2026-01-15T10:30:00Z',
      price: 50000,
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      instrument: 'BTC/USD',
      side: 'buy',
      exchange: 'kraken',
      status: 'filled',
    } as unknown as ExecutionData

    expect(() => executionFromAPI(apiExecution)).toThrow(
      'ExecutionData missing required field: timestamp'
    )
  })
  it('falls back to current date when executed_at is undefined in API execution', () => {
    const apiExecution = {
      timestamp: '2026-01-15T10:30:00Z',
      client_order_id: 'client-12',
      price: 50000,
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      instrument: 'BTC/USD',
      side: 'buy',
      exchange: 'kraken',
      status: 'filled',
    } as unknown as ExecutionData
    const before = new Date()
    const result = executionFromAPI(apiExecution)

    expect(result.executedAt).toBeDefined()
    expect((result.executedAt as Date).getTime()).toBeGreaterThanOrEqual(before.getTime())
  })
})
describe('Signal Transformers', () => {
  it('transforms REST API signal to canonical entity', () => {
    const apiSignal: SignalData = {
      type: 'signal',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:29:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      fired_at: '2026-01-15T10:30:00Z',
      side: 'buy',
      strength: 0.85,
      reason: 'RSI oversold',
      strategy_name: 'momentum_v1',
      price: 49500,
    }
    const result = signalFromAPI(apiSignal)

    expect(result.timestamp).toEqual(new Date('2026-01-15T10:29:00Z'))
    expect(result.exchange).toBe('kraken')
    expect(result.instrument).toBe('BTC/USD')
    expect(result.strategyName).toBe('momentum_v1')
    expect(result.firedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('transforms WebSocket signal to canonical entity', () => {
    const wsSignal: SignalData = {
      type: 'signal',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
      fired_at: '2026-01-15T10:30:00Z',
    }
    const result = signalFromWS(wsSignal)

    expect(result.exchange).toBe('kraken')
    expect(result.strategyName).toBeNull()
    expect(result.firedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('throws when fired_at and timestamp are missing in WebSocket signal', () => {
    const wsSignal = {
      type: 'signal',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
    } as unknown as SignalData

    expect(() => signalFromWS(wsSignal)).toThrow('SignalData missing required field: timestamp')
  })
  it('throws when API signal has no timestamp', () => {
    const apiSignal = {
      type: 'signal',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      fired_at: '2026-01-15T10:30:00Z',
      side: 'buy',
      strength: 0.85,
      reason: 'RSI oversold',
      strategy_name: 'momentum_v1',
      price: 49500,
    } as unknown as SignalData

    expect(() => signalFromAPI(apiSignal)).toThrow('SignalData missing required field: timestamp')
  })
  it('falls back to timestamp when fired_at is undefined in API signal', () => {
    const apiSignal = {
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      strength: 0.85,
      reason: 'RSI oversold',
      strategy_name: 'momentum_v1',
      price: 49500,
      timestamp: '2026-01-15T10:30:00Z',
    } as unknown as SignalData
    const result = signalFromAPI(apiSignal)

    expect(result.firedAt).toBeDefined()
    expect(result.firedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('falls back to timestamp when fired_at is undefined in WS signal', () => {
    const wsSignal = {
      type: 'signal',
      exchange: 'kraken',
      instrument: 'ETH/USD',
      side: 'sell',
      strength: 0.7,
      reason: 'MACD divergence',
      strategy_name: null,
      price: null,
      timestamp: '2026-01-15T10:30:00Z',
    } as unknown as SignalData
    const result = signalFromWS(wsSignal)

    expect(result.firedAt).toEqual(new Date('2026-01-15T10:30:00Z'))
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
})
describe('Position Transformers', () => {
  it('transforms REST API position to canonical entity', () => {
    const apiPosition: PositionData = {
      type: 'position',
      sequence_id: 0,
      public_id: 'pos-uuid-1',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      instrument_public_id: 'inst-uuid-1',
      exchange: 'kraken',
      mode: 'live',
      quantity: 1.5,
      average_price: 48000,
      unrealized_pnl: 3000,
      realized_pnl: 500,
      wallet_public_id: 'wal-uuid-1',
    }
    const result = positionFromAPI(apiPosition)

    expect(result.publicId).toBe('pos-uuid-1')
    expect(result.instrument).toBe('BTC/USD')

    expect(result.exchange).toBe('kraken')
    expect(result.mode).toBe('live')
    expect(result.averagePrice).toBe(48000)
    expect(result.unrealizedPnl).toBe(3000)
    expect(result.realizedPnl).toBe(500)
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('propagates paper mode through positionFromAPI', () => {
    const apiPosition: PositionData = {
      type: 'position',
      sequence_id: 0,
      public_id: 'pos-uuid-2',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      instrument_public_id: 'inst-uuid-2',
      exchange: 'kraken',
      mode: 'paper',
      quantity: -1,
      average_price: 48000,
      unrealized_pnl: 0,
      realized_pnl: 0,
      wallet_public_id: 'wal-uuid-2',
    }
    const result = positionFromAPI(apiPosition)

    expect(result.mode).toBe('paper')
  })
  it('throws when API position has no timestamp', () => {
    const apiPosition = {
      type: 'position',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      quantity: 1.5,
      average_price: 48000,
      unrealized_pnl: 3000,
      realized_pnl: 500,
    } as unknown as PositionData

    expect(() => positionFromAPI(apiPosition)).toThrow(
      'PositionData missing required field: timestamp'
    )
  })
})
describe('Candle Transformers', () => {
  it('transforms REST API candle to canonical entity', () => {
    const apiCandle: CandleData = {
      type: 'candle',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
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

    expect(result.timestamp).toEqual(new Date('2026-01-15T10:00:00Z'))
    expect(result.instrument).toBe('BTC/USD')
    expect(result.timeframe).toBe('1h')
    expect(result.vwap).toBe(49750)
    expect(result.trades).toBe(5000)
    expect(result.openAt).toEqual(new Date('2026-01-15T10:00:00Z'))
  })
  it('throws when API candle has no timestamp', () => {
    const apiCandle = {
      type: 'candle',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      timeframe: '1h',
      open_at: '2026-01-15T10:00:00Z',
      open: 49000,
      high: 50500,
      low: 48500,
      close: 50000,
      volume: 1000,
    } as unknown as CandleData

    expect(() => candleFromAPI(apiCandle)).toThrow('CandleData missing required field: timestamp')
  })
  it('transforms WebSocket candle to canonical entity', () => {
    const wsCandle: CandleData = {
      type: 'candle',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
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
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:00:00Z'))
  })
  it('includes envelope timestamp when present in WebSocket candle', () => {
    const wsCandle: CandleData = {
      type: 'candle',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:00:01Z',
      session_id: 'test-sid',
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

    expect(result.openAt).toEqual(new Date('2026-01-15T10:00:00Z'))
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:00:01Z'))
  })
  it('throws on missing timeframe in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      timestamp: '2026-01-15T10:00:00Z',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: null,
      open: 3000,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleData

    expect(() => candleFromWS(wsCandle)).toThrow('CandleData missing required field: timeframe')
  })
  it('throws on missing open in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      timestamp: '2026-01-15T10:00:00Z',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: null,
      high: 3050,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleData

    expect(() => candleFromWS(wsCandle)).toThrow('CandleData missing required field: open')
  })
  it('throws on missing high in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      timestamp: '2026-01-15T10:00:00Z',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: null,
      low: 2980,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleData

    expect(() => candleFromWS(wsCandle)).toThrow('CandleData missing required field: high')
  })
  it('throws on missing low in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      timestamp: '2026-01-15T10:00:00Z',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: null,
      close: 3020,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleData

    expect(() => candleFromWS(wsCandle)).toThrow('CandleData missing required field: low')
  })
  it('throws on missing close in WebSocket candle', () => {
    const wsCandle = {
      type: 'candle',
      timestamp: '2026-01-15T10:00:00Z',
      instrument: 'ETH/USD',
      exchange: 'kraken',
      timeframe: '5m',
      open: 3000,
      high: 3050,
      low: 2980,
      close: null,
      volume: 500,
      open_at: '2026-01-15T10:00:00Z',
    } as unknown as CandleData

    expect(() => candleFromWS(wsCandle)).toThrow('CandleData missing required field: close')
  })
})
describe('Tick Transformers', () => {
  it('transforms WebSocket tick to canonical entity', () => {
    const wsTick: TickData = {
      type: 'tick',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: 49990,
      ask: 50010,
      last: 50000,
      volume: 100,
    }
    const result = tickFromWS(wsTick)

    expect(result.instrument).toBe('BTC/USD')
    expect(result.bid).toBe(49990)
    expect(result.ask).toBe(50010)
    expect(result.last).toBe(50000)
    expect(result.timestamp).toEqual(new Date('2026-01-15T10:30:00Z'))
  })
  it('handles missing last and nullable bid/ask', () => {
    const wsTick: TickData = {
      type: 'tick',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: null,
      ask: null,
      volume: 0,
    }
    const result = tickFromWS(wsTick)

    expect(result.bid).toBeNull()
    expect(result.ask).toBeNull()
    expect(result.last).toBeUndefined()
    expect(result.volume).toBe(0)
  })
  it('throws when WebSocket tick has no timestamp', () => {
    const wsTick = {
      type: 'tick',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      bid: 49990,
      ask: 50010,
      volume: 100,
    } as unknown as TickData

    expect(() => tickFromWS(wsTick)).toThrow('TickData missing required field: timestamp')
  })
})
describe('Heartbeat Transformers', () => {
  it('transforms WebSocket heartbeat to canonical entity', () => {
    const wsHeartbeat: HeartbeatData = {
      type: 'heartbeat',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      component: 'executor_kraken',
      status: 'healthy',
      meta: { version: '1.0' },
      sequence: 1,
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
    const wsHeartbeat: HeartbeatData = {
      type: 'heartbeat',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      component: 'executor_kraken',
      status: 'healthy',
      sequence: 42,
      lag_ms: 15,
    }
    const result = heartbeatFromWS(wsHeartbeat)

    expect(result.sequence).toBe(42)
  })
  it('throws when WebSocket heartbeat has no timestamp', () => {
    const wsHeartbeat = {
      type: 'heartbeat',
      component: 'executor_kraken',
      status: 'healthy',
      lag_ms: 15,
    } as unknown as HeartbeatData

    expect(() => heartbeatFromWS(wsHeartbeat)).toThrow(
      'HeartbeatData missing required field: timestamp'
    )
  })
})
describe('Batch Transformers', () => {
  it('transforms array of orders', () => {
    const apiOrders: OrderData[] = [
      {
        type: 'order',
        sequence_id: 0,
        public_id: 'test-pid-1',
        timestamp: '2026-01-15T10:30:00Z',
        session_id: 'test-sid',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        client_order_id: 'client-1',
        exchange_order_id: null,
        created_at: '2026-01-15T10:30:00Z',
        updated_at: null,
        side: 'buy',
        order_type: 'market',
        price: null,
        size: 1,
        filled_size: 0,
        status: 'new',
        time_in_force: null,
        error: null,
      },
      {
        type: 'order',
        sequence_id: 1,
        public_id: 'test-pid-2',
        timestamp: '2026-01-15T10:31:00Z',
        session_id: 'test-sid',
        instrument: 'ETH/USD',
        exchange: 'kraken',
        client_order_id: 'client-2',
        exchange_order_id: null,
        created_at: '2026-01-15T10:31:00Z',
        updated_at: null,
        side: 'sell',
        order_type: 'limit',
        price: 3000,
        size: 2,
        filled_size: 0,
        status: 'open',
        time_in_force: null,
        error: null,
      },
    ]
    const result = ordersFromAPI(apiOrders)

    expect(result).toHaveLength(2)
    expect(result[0].clientOrderId).toBe('client-1')
    expect(result[1].clientOrderId).toBe('client-2')
  })
  it('transforms array of executions', () => {
    const apiExecutions: ExecutionData[] = [
      {
        type: 'execution',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2026-01-15T10:30:00Z',
        session_id: 'test-sid',
        client_order_id: 'client-10',
        executed_at: '2026-01-15T10:30:00Z',
        price: 50000,
        size: 0.1,
        last_size: 0.1,
        last_price: 50000,
        fee: 5,
        fee_asset: 'USD',
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const result = executionsFromAPI(apiExecutions)

    expect(result).toHaveLength(1)
    expect(result[0].clientOrderId).toBe('client-10')
  })
  it('transforms array of signals', () => {
    const apiSignals: SignalData[] = [
      {
        type: 'signal',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2026-01-15T10:30:00Z',
        session_id: 'test-sid',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        fired_at: '2026-01-15T10:30:00Z',
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
    const apiPositions: PositionData[] = [
      {
        type: 'position',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2026-01-15T10:30:00Z',
        session_id: 'test-sid',
        instrument: 'BTC/USD',
        instrument_public_id: 'inst-test-pid',
        exchange: 'kraken',
        mode: 'live',
        quantity: 1.5,
        average_price: 48000,
        unrealized_pnl: 3000,
        realized_pnl: 500,
        wallet_public_id: 'wal-test-pid',
      },
    ]
    const result = positionsFromAPI(apiPositions)

    expect(result).toHaveLength(1)
    expect(result[0].averagePrice).toBe(48000)
  })
  it('transforms array of candles', () => {
    const apiCandles: CandleData[] = [
      {
        type: 'candle',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2026-01-15T10:00:00Z',
        session_id: 'test-sid',
        instrument: 'BTC/USD',
        exchange: 'kraken',
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
    const apiOrder: OrderData = {
      type: 'order',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-123',
      exchange_order_id: null,
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'buy',
      order_type: 'limit',
      size: 0.1,
      filled_size: 0,
      price: 50000,
      status: 'filled',
      time_in_force: null,
      error: null,
    }
    const result = safeOrderFromAPI(apiOrder)

    expect(result).not.toBeNull()
    expect(result?.clientOrderId).toBe('client-123')
  })
  it('safeOrderFromAPI returns null on invalid side', () => {
    const apiOrder = {
      instrument: 'BTC/USD',
      exchange: 'kraken',
      client_order_id: 'client-1',
      exchange_order_id: null,
      timestamp: '2026-01-15T10:30:00Z',
      created_at: '2026-01-15T10:30:00Z',
      updated_at: null,
      side: 'invalid_side',
      order_type: 'limit',
      size: 0.1,
      filled_size: 0,
      status: 'filled',
      time_in_force: null,
      error: null,
    } as unknown as OrderData
    const result = safeOrderFromAPI(apiOrder)

    expect(result).toBeNull()
  })
  it('safeExecutionFromAPI returns execution on valid input', () => {
    const apiExecution: ExecutionData = {
      type: 'execution',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-1',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'buy',
      price: 50000,
      size: 0.1,
      last_size: 0.1,
      last_price: 50000,
      fee: 5,
      fee_asset: 'USD',
      executed_at: '2026-01-15T10:30:00Z',
      status: 'filled',
    }
    const result = safeExecutionFromAPI(apiExecution)

    expect(result).not.toBeNull()
    expect(result?.clientOrderId).toBe('client-1')
  })
  it('safeExecutionFromAPI returns null on invalid side', () => {
    const apiExecution = {
      client_order_id: 'client-1',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      timestamp: '2026-01-15T10:30:00Z',
      side: 'unknown',
      size: 0.1,
      fee: 5,
      fee_asset: 'USD',
      executed_at: '2026-01-15T10:30:00Z',
      status: 'filled',
    } as unknown as ExecutionData
    const result = safeExecutionFromAPI(apiExecution)

    expect(result).toBeNull()
  })
  it('safeSignalFromAPI returns signal on valid input', () => {
    const apiSignal: SignalData = {
      type: 'signal',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:30:00Z',
      session_id: 'test-sid',
      exchange: 'kraken',
      instrument: 'BTC/USD',
      side: 'buy',
      strength: 0.8,
      reason: 'Test signal',
      strategy_name: null,
      price: null,
      fired_at: '2026-01-15T10:30:00Z',
    }
    const result = safeSignalFromAPI(apiSignal)

    expect(result).not.toBeNull()
    expect(result?.instrument).toBe('BTC/USD')
  })
  it('safeSignalFromAPI returns null on invalid side', () => {
    const apiSignal = {
      exchange: 'kraken',
      instrument: 'BTC/USD',
      timestamp: '2026-01-15T10:30:00Z',
      side: 'hold',
      strength: 0.8,
      reason: 'Test signal',
      strategy_name: null,
      price: null,
      fired_at: '2026-01-15T10:30:00Z',
    } as unknown as SignalData
    const result = safeSignalFromAPI(apiSignal)

    expect(result).toBeNull()
  })
  it('orderDataFromEnvelope converts to OrderData preserving type', () => {
    const envelope: OrderData = {
      type: 'order',
      sequence_id: 0,
      public_id: 'uuid-1',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
      client_order_id: 'client-1',
      exchange_order_id: 'exch-1',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      status: 'new',
      order_type: 'limit',
      size: 1,
      filled_size: 0,
      price: 50000,
      average_price: null,
      reason: null,
      time_in_force: 'GTC',
      error: null,
      created_at: '2026-01-15T10:00:00Z',
      updated_at: null,
    }
    const data = orderDataFromEnvelope(envelope)

    expect(data.public_id).toBe('uuid-1')
    expect(data.client_order_id).toBe('client-1')
    expect(data.exchange).toBe('kraken')
    expect(data.order_type).toBe('limit')
    expect(data.time_in_force).toBe('GTC')
    expect(data.type).toBe('order')
    expect(data.timestamp).toBe('2026-01-15T10:00:00Z')
  })
  it('executionDataFromEnvelope converts to ExecutionData preserving type', () => {
    const envelope: ExecutionData = {
      type: 'execution',
      sequence_id: 0,
      public_id: 'uuid-2',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
      trade_id: 'trade-1',
      exchange_order_id: 'exch-1',
      client_order_id: 'ord-1',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      size: 1,
      price: 50000,
      last_size: 1,
      last_price: 50000,
      fee: 0.1,
      fee_asset: 'USD',
      status: 'filled',
      executed_at: '2026-01-15T10:00:01Z',
    }
    const data = executionDataFromEnvelope(envelope)

    expect(data.public_id).toBe('uuid-2')
    expect(data.trade_id).toBe('trade-1')
    expect(data.client_order_id).toBe('ord-1')
    expect(data.executed_at).toBe('2026-01-15T10:00:01Z')
    expect(data.type).toBe('execution')
    expect(data.timestamp).toBe('2026-01-15T10:00:00Z')
  })
  it('signalDataFromEnvelope converts to SignalData preserving type', () => {
    const envelope: SignalData = {
      type: 'signal',
      sequence_id: 0,
      public_id: 'uuid-3',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      strength: 0.8,
      reason: 'Test signal',
      price: 50000,
      strategy_name: 'macd',
      fired_at: '2026-01-15T10:00:00Z',
    }
    const data = signalDataFromEnvelope(envelope)

    expect(data.public_id).toBe('uuid-3')
    expect(data.strategy_name).toBe('macd')
    expect(data.fired_at).toBe('2026-01-15T10:00:00Z')
    expect(data.type).toBe('signal')
    expect(data.timestamp).toBe('2026-01-15T10:00:00Z')
  })
  it('signalDataFromEnvelope falls back to timestamp when fired_at is absent', () => {
    const envelope: SignalData = {
      type: 'signal',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2026-01-15T10:00:00Z',
      session_id: 'test-sid',
      instrument: 'BTC/USD',
      exchange: 'kraken',
      side: 'buy',
      strength: 0.5,
      reason: 'Fallback',
      fired_at: '2026-01-15T10:30:00Z',
    }
    const data = signalDataFromEnvelope(envelope)

    expect(data.fired_at).toBe('2026-01-15T10:30:00Z')
  })
})
