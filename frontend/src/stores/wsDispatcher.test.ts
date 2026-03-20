import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { QueryClient } from '@tanstack/react-query'
import { WSDispatcher, getDispatcher, resetDispatcher } from './wsDispatcher'
import WebSocketClient from '../lib/websocket/client'
import { useMarketStore } from './market'
import { useAppStore } from './app'
import type {
  OrderData,
  ExecutionData,
  SignalData,
  CandleData,
  TradeData,
  HeartbeatData,
} from '../types/ws'

vi.mock('./market', () => ({
  useMarketStore: {
    getState: vi.fn(),
  },
}))
vi.mock('./app', () => ({
  useAppStore: {
    getState: vi.fn(),
  },
}))
vi.mock('../lib/websocket', () => {
  return {
    default: vi.fn().mockImplementation(() => ({
      onMessage: vi.fn(() => vi.fn()),
      onConnection: vi.fn(() => vi.fn()),
      subscribe: vi.fn(),
      isConnected: vi.fn(() => true),
    })),
  }
})
describe('WSDispatcher', () => {
  let queryClient: QueryClient
  let mockWsClient: WebSocketClient
  let messageHandlers: Map<string, (msg: unknown) => void>
  let connectionHandlers: ((connected: boolean) => void)[]

  beforeEach(() => {
    vi.clearAllMocks()
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    })
    messageHandlers = new Map()
    connectionHandlers = []
    mockWsClient = {
      onMessage: vi.fn((type: string, handler: (msg: unknown) => void) => {
        messageHandlers.set(type, handler)

        return vi.fn()
      }),
      onConnection: vi.fn((handler: (connected: boolean) => void) => {
        connectionHandlers.push(handler)

        return vi.fn()
      }),
      subscribe: vi.fn(),
      isConnected: vi.fn(() => true),
      getSubscribedTopics: vi.fn(() => []),
    } as unknown as WebSocketClient
    const mockMarketStore = {
      updateLastPrice: vi.fn(),
    }

    vi.mocked(useMarketStore.getState).mockReturnValue(mockMarketStore as never)
    const mockAppStore = {
      setConnected: vi.fn(),
      setConnectionLag: vi.fn(),
      updateLastUpdate: vi.fn(),
      setSubscribedTopics: vi.fn(),
    }

    vi.mocked(useAppStore.getState).mockReturnValue(mockAppStore as never)
    resetDispatcher()
  })
  afterEach(() => {
    resetDispatcher()
  })
  describe('constructor', () => {
    it('creates dispatcher with default topics', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      expect(dispatcher).toBeDefined()
      expect(dispatcher.isAttached()).toBe(false)
    })
    it('creates dispatcher with custom topics', () => {
      const dispatcher = new WSDispatcher({
        queryClient,
        topics: ['custom.topic'],
      })

      expect(dispatcher).toBeDefined()
    })
  })
  describe('attach/detach', () => {
    it('attaches to WebSocket client and registers handlers', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('order', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('execution', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('signal', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('candle', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('tick', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
      expect(mockWsClient.onConnection).toHaveBeenCalled()
      expect(dispatcher.isAttached()).toBe(true)
    })
    it('detaches and cleans up handlers', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.detach()
      expect(dispatcher.isAttached()).toBe(false)
      expect(dispatcher.getClient()).toBeNull()
    })
    it('detaches previous client before attaching new one', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const newMockClient = {
        onMessage: vi.fn(() => vi.fn()),
        onConnection: vi.fn(() => vi.fn()),
        subscribe: vi.fn(),
        isConnected: vi.fn(() => false),
      } as unknown as WebSocketClient

      dispatcher.attach(newMockClient)
      expect(dispatcher.getClient()).toBe(newMockClient)
    })
  })
  describe('message handling', () => {
    it('handles candle message and updates market store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        timeframe: '1m',
        open_at: nowIso,
        open: 49000,
        high: 51000,
        low: 48500,
        close: 50500,
        volume: 100,
      }
      const candleHandler = messageHandlers.get('candle')

      expect(candleHandler).toBeDefined()
      candleHandler?.(candleMessage)
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalledWith(50500)
    })
    it('skips candle last price update when close is undefined', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open_at: nowIso,
        open: 49000,
        high: 51000,
        low: 48500,
        close: undefined,
        volume: 100,
      } as unknown as CandleData
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
    })
    it('handles candle message with instrument/timeframe - merges into cache', () => {
      const nowIso = new Date().toISOString()
      const existingCandles = [
        {
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timeframe: '1m',
          open_at: nowIso,
          open: 49000,
          high: 50000,
          low: 48000,
          close: 49500,
          volume: 50,
          vwap: null,
          trades: null,
        },
      ]

      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], existingCandles)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: nowIso,
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]).toMatchObject({ close: 50500, volume: 100 })
    })
    it('order message merges new order into cache', () => {
      const existingOrders = [
        {
          public_id: 'uuid-1',
          client_order_id: 'existing-1',
          instrument: 'ETH/USD',
          exchange: 'kraken' as const,
          side: 'sell' as const,
          status: 'filled',
          order_type: 'market' as const,
          size: 2,
          filled_size: 2,
          created_at: new Date().toISOString(),
        },
      ]

      queryClient.setQueryData(['orders', undefined], existingOrders)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderMessage: OrderData = {
        type: 'order',
        client_order_id: 'client-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      }
      const orderHandler = messageHandlers.get('order')

      orderHandler?.(orderMessage)
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>(['orders', undefined])

      expect(cached).toHaveLength(2)
      expect(cached?.[0]?.client_order_id).toBe('client-1')
    })
    it('order message updates existing order in cache by client_order_id', () => {
      const existingOrders = [
        {
          public_id: 'uuid-1',
          client_order_id: 'client-1',
          instrument: 'BTC/USD',
          exchange: 'kraken' as const,
          side: 'buy' as const,
          status: 'new',
          order_type: 'limit' as const,
          size: 1,
          filled_size: 0,
          created_at: new Date().toISOString(),
        },
      ]

      queryClient.setQueryData(['orders', undefined], existingOrders)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderMessage: OrderData = {
        type: 'order',
        client_order_id: 'client-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'filled',
        filled_size: 1,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }
      const orderHandler = messageHandlers.get('order')

      orderHandler?.(orderMessage)
      const cached = queryClient.getQueryData<{ client_order_id: string; status: string }[]>([
        'orders',
        undefined,
      ])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.status).toBe('filled')
    })
    it('execution message merges into cache', () => {
      const existingExecs = [
        {
          public_id: 'uuid-1',
          client_order_id: 'ord-old',
          instrument: 'ETH/USD',
          exchange: 'kraken' as const,
          side: 'sell' as const,
          size: 2,
          price: 3000,
          fee: 0.05,
          fee_asset: 'USD',
          status: 'filled' as const,
          executed_at: '2026-01-01T00:00:00Z',
        },
      ]

      queryClient.setQueryData(['executions', undefined], existingExecs)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const execMessage: ExecutionData = {
        type: 'execution',
        client_order_id: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: new Date().toISOString(),
      }
      const execHandler = messageHandlers.get('execution')

      execHandler?.(execMessage)
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>([
        'executions',
        undefined,
      ])

      expect(cached).toHaveLength(2)
      expect(cached?.[0]?.client_order_id).toBe('ord-1')
    })
    it('execution message deduplicates by public_id', () => {
      const executedAt = new Date().toISOString()
      const existingExecs = [
        {
          public_id: 'uuid-1',
          client_order_id: 'ord-1',
          instrument: 'BTC/USD',
          exchange: 'kraken' as const,
          side: 'buy' as const,
          size: 1,
          price: 50000,
          fee: 0.1,
          fee_asset: 'USD',
          status: 'filled' as const,
          executed_at: executedAt,
        },
      ]

      queryClient.setQueryData(['executions', undefined], existingExecs)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const execMessage: ExecutionData = {
        type: 'execution',
        public_id: 'uuid-1',
        client_order_id: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: executedAt,
      }
      const execHandler = messageHandlers.get('execution')

      execHandler?.(execMessage)
      const cached = queryClient.getQueryData<unknown[]>(['executions', undefined])

      expect(cached).toHaveLength(1)
    })
    it('signal message merges into cache', () => {
      const existingSignals = [
        {
          public_id: 'uuid-1',
          instrument: 'ETH/USD',
          exchange: 'kraken' as const,
          side: 'sell' as const,
          strength: 0.5,
          reason: 'Old signal',
          strategy_name: 'old_strategy',
          fired_at: '2026-01-01T00:00:00Z',
        },
      ]

      queryClient.setQueryData(['signals', undefined, 50, undefined, 24], existingSignals)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalMessage: SignalData = {
        type: 'signal',
        fired_at: new Date().toISOString(),
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Test signal',
        strategy_name: 'test_strategy',
        price: 50000,
      }
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.(signalMessage)
      const cached = queryClient.getQueryData<{ strategy_name: string }[]>([
        'signals',
        undefined,
        50,
        undefined,
        24,
      ])

      expect(cached).toHaveLength(2)
      expect(cached?.[0]?.strategy_name).toBe('test_strategy')
    })
    it('signal message deduplicates by strategy_name and fired_at', () => {
      const firedAt = new Date().toISOString()
      const existingSignals = [
        {
          public_id: 'uuid-1',
          instrument: 'BTC/USD',
          exchange: 'kraken' as const,
          side: 'buy' as const,
          strength: 0.8,
          reason: 'Test signal',
          strategy_name: 'test_strategy',
          fired_at: firedAt,
        },
      ]

      queryClient.setQueryData(['signals', undefined, 50, undefined, 24], existingSignals)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalMessage: SignalData = {
        type: 'signal',
        fired_at: firedAt,
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Test signal',
        strategy_name: 'test_strategy',
        price: 50000,
      }
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.(signalMessage)
      const cached = queryClient.getQueryData<unknown[]>(['signals', undefined, 50, undefined, 24])

      expect(cached).toHaveLength(1)
    })
    it('candle message appends new candle when open_at is newer', () => {
      const oldTime = '2026-01-15T10:00:00Z'
      const newTime = '2026-01-15T10:01:00Z'
      const existingCandles = [
        {
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timeframe: '1m',
          open_at: oldTime,
          open: 49000,
          high: 50000,
          low: 48000,
          close: 49500,
          volume: 50,
          vwap: null,
          trades: null,
        },
      ]

      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], existingCandles)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: newTime,
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(2)
    })
    it('candle message trims cache to maxCandles sliding window', () => {
      const existingCandles = Array.from({ length: 5 }, (_, i) => ({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open_at: new Date(Date.UTC(2026, 0, 15, 10, i)).toISOString(),
        open: 49000 + i * 100,
        high: 49500 + i * 100,
        low: 48500 + i * 100,
        close: 49200 + i * 100,
        volume: 50,
        vwap: null,
        trades: null,
      }))

      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], existingCandles)
      const dispatcher = new WSDispatcher({ queryClient, maxCandles: 5 })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: new Date(Date.UTC(2026, 0, 15, 10, 5)).toISOString(),
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<{ close: number }[]>([
        'candles',
        'BTC-USD',
        'kraken',
        '1m',
      ])

      expect(cached).toHaveLength(5)
      expect(cached?.[0]?.close).toBe(49300)
      expect(cached?.[4]?.close).toBe(50500)
    })
    it('candle message without instrument/timeframe does not update cache', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: '',
        exchange: 'kraken',
        timeframe: '',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: nowIso,
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('candle message skips merge when no cache exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 3000,
        high: 3100,
        low: 2900,
        close: 3050,
        volume: 200,
        open_at: nowIso,
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('candle message ignores old candle open_at', () => {
      const oldTime = '2026-01-15T10:01:00Z'
      const olderTime = '2026-01-15T10:00:00Z'
      const existingCandles = [
        {
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timeframe: '1m',
          open_at: oldTime,
          open: 49000,
          high: 50000,
          low: 48000,
          close: 49500,
          volume: 50,
          vwap: null,
          trades: null,
        },
      ]

      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], existingCandles)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 48000,
        high: 49000,
        low: 47000,
        close: 48500,
        volume: 30,
        open_at: olderTime,
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]).toMatchObject({ close: 49500 })
    })
    it('candle message includes open_at on merged candle', () => {
      const existingCandles = [
        {
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timeframe: '1m',
          open_at: '2020-01-01T00:00:00Z',
          open: 49000,
          high: 50000,
          low: 48000,
          close: 49500,
          volume: 50,
          vwap: null,
          trades: null,
        },
      ]

      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], existingCandles)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: '2020-01-01T00:01:00Z',
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<{ open_at: string }[]>([
        'candles',
        'BTC-USD',
        'kraken',
        '1m',
      ])

      expect(cached).toHaveLength(2)
      expect(cached?.[1]?.open_at).toBeDefined()
    })
    it('candle message handles empty existing cache array', () => {
      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], [])
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: '2026-01-15T10:00:00Z',
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(1)
    })
    it('buffers candle messages when buffering is active and no cache exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startBuffering('ETH-USD', 'kraken', '1m')
      const candleMessage: CandleData = {
        type: 'candle',
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 3000,
        high: 3100,
        low: 2900,
        close: 3050,
        volume: 200,
        open_at: '2026-01-15T10:00:00Z',
      }
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.(candleMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('flushBuffer replays buffered candles onto cache', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startBuffering('BTC-USD', 'kraken', '1m')
      const candleHandler = messageHandlers.get('candle')
      const candle1: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: '2026-01-15T10:00:00Z',
      }
      const candle2: CandleData = {
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51500,
        low: 49000,
        close: 51000,
        volume: 150,
        open_at: '2026-01-15T10:00:00Z',
      }

      candleHandler?.(candle1)
      candleHandler?.(candle2)
      queryClient.setQueryData(
        ['candles', 'BTC-USD', 'kraken', '1m'],
        [
          {
            instrument: 'BTC-USD',
            exchange: 'kraken',
            timeframe: '1m',
            open_at: '2026-01-15T10:00:00Z',
            open: 50000,
            high: 50800,
            low: 49500,
            close: 50200,
            volume: 80,
            vwap: null,
            trades: null,
          },
        ]
      )
      dispatcher.flushBuffer('BTC-USD', 'kraken', '1m')
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(1)
      expect((cached as { close: number }[])[0].close).toBe(51000)
    })
    it('flushBuffer is a no-op when no buffer exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.flushBuffer('XYZ', 'kraken', '1m')
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('flushBuffer is a no-op when buffer is empty', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.startBuffering('BTC-USD', 'kraken', '1m')
      dispatcher.flushBuffer('BTC-USD', 'kraken', '1m')
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('stopBuffering discards buffered candles', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startBuffering('BTC-USD', 'kraken', '1m')
      const candleHandler = messageHandlers.get('candle')

      candleHandler?.({
        type: 'candle',
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        open_at: '2026-01-15T10:00:00Z',
      })
      dispatcher.stopBuffering('BTC-USD', 'kraken', '1m')
      queryClient.setQueryData(['candles', 'BTC-USD', 'kraken', '1m'], [])
      dispatcher.flushBuffer('BTC-USD', 'kraken', '1m')
      const cached = queryClient.getQueryData<unknown[]>(['candles', 'BTC-USD', 'kraken', '1m'])

      expect(cached).toHaveLength(0)
    })
    it('tick handler updates market store with mid price from bid/ask', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const tickMessage = {
        type: 'tick',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        bid: 50000,
        ask: 51000,
        timestamp: nowIso,
      }
      const tickHandler = messageHandlers.get('tick')

      tickHandler?.(tickMessage)
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalledWith(50500)
    })
    it('tick handler updates market store with last price', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const tickMessage = {
        type: 'tick',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        bid: null,
        ask: null,
        last: 51234,
        timestamp: nowIso,
      }
      const tickHandler = messageHandlers.get('tick')

      tickHandler?.(tickMessage)
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalledWith(51234)
    })
    it('skips tick updates when prices are null', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const tickMessage = {
        type: 'tick',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        bid: null,
        ask: null,
        last: null,
        timestamp: nowIso,
      }
      const tickHandler = messageHandlers.get('tick')

      tickHandler?.(tickMessage)
      expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
    })
    it('trade handler updates market store with trade price', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const tradeMessage: TradeData = {
        type: 'trade',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        price: 50250,
        volume: 1.25,
        side: 'buy',
        timestamp: nowIso,
      }
      const tradeHandler = messageHandlers.get('trade')

      expect(tradeHandler).toBeDefined()
      tradeHandler?.(tradeMessage)
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalled()
    })
    it('handles heartbeat message and updates app store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatData = {
        type: 'heartbeat',
        component: 'bridge',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 50,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      expect(heartbeatHandler).toBeDefined()
      heartbeatHandler?.(heartbeatMessage)
      expect(useAppStore.getState().setConnectionLag).not.toHaveBeenCalled()
      expect(useAppStore.getState().updateLastUpdate).toHaveBeenCalled()
    })
  })
  describe('connection handling', () => {
    it('subscribes to topics when connected', () => {
      const dispatcher = new WSDispatcher({ queryClient, topics: ['test.topic'] })

      dispatcher.attach(mockWsClient)
      const connectionHandler = connectionHandlers[0]

      expect(connectionHandler).toBeDefined()
      connectionHandler?.(true)
      expect(mockWsClient.subscribe).toHaveBeenCalledWith(['test.topic'])
      expect(useAppStore.getState().setConnected).toHaveBeenCalledWith(true)
    })
    it('does not subscribe when topics list is empty', () => {
      const dispatcher = new WSDispatcher({ queryClient, topics: [] })

      dispatcher.attach(mockWsClient)
      const connectionHandler = connectionHandlers[0]

      connectionHandler?.(true)
      expect(mockWsClient.subscribe).not.toHaveBeenCalled()
    })
    it('syncs subscribed topics on subscription_success', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      vi.mocked(mockWsClient.getSubscribedTopics).mockReturnValue([
        'market.kraken.BTC-USD.candles.1m',
      ])
      dispatcher.attach(mockWsClient)
      const handler = messageHandlers.get('subscription_success')

      handler?.({})
      expect(useAppStore.getState().setSubscribedTopics).toHaveBeenCalledWith([
        'market.kraken.BTC-USD.candles.1m',
      ])
    })
    it('updates app store when disconnected', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const connectionHandler = connectionHandlers[0]

      connectionHandler?.(false)
      expect(useAppStore.getState().setConnected).toHaveBeenCalledWith(false)
    })
    it('skips subscribe when all topics already subscribed on reconnect', () => {
      const dispatcher = new WSDispatcher({ queryClient, topics: ['test.topic'] })

      vi.mocked(mockWsClient.isConnected).mockReturnValue(false)
      dispatcher.attach(mockWsClient)
      vi.mocked(mockWsClient.isConnected).mockReturnValue(true)
      vi.mocked(mockWsClient.getSubscribedTopics).mockReturnValue(['test.topic'])
      const connectionHandler = connectionHandlers[0]

      connectionHandler?.(true)
      expect(mockWsClient.subscribe).not.toHaveBeenCalled()
      expect(useAppStore.getState().setSubscribedTopics).toHaveBeenCalledWith(['test.topic'])
    })
    it('clears subscribed topics on disconnect', () => {
      const dispatcher = new WSDispatcher({ queryClient, topics: ['test.topic'] })

      dispatcher.attach(mockWsClient)
      const connectionHandler = connectionHandlers[0]

      connectionHandler?.(false)
      expect(useAppStore.getState().setSubscribedTopics).toHaveBeenCalledWith([])
    })
  })
  describe('singleton pattern', () => {
    it('getDispatcher returns same instance', () => {
      const dispatcher1 = getDispatcher(queryClient)
      const dispatcher2 = getDispatcher(queryClient)

      expect(dispatcher1).toBe(dispatcher2)
    })
    it('resetDispatcher clears singleton', () => {
      const dispatcher1 = getDispatcher(queryClient)

      resetDispatcher()
      const dispatcher2 = getDispatcher(queryClient)

      expect(dispatcher1).not.toBe(dispatcher2)
    })
  })
  describe('additional message handling', () => {
    it('handles heartbeat without data', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const heartbeatMessage = {
        type: 'heartbeat',
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      expect(() => heartbeatHandler?.(heartbeatMessage)).not.toThrow()
    })
    it('handles heartbeat without component', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const heartbeatMessage = {
        type: 'heartbeat',
        status: 'healthy',
        timestamp: new Date().toISOString(),
        lag_ms: 5,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useAppStore.getState().setConnectionLag).not.toHaveBeenCalled()
    })
    it('handles pong message with rtt_ms and updates connectionLag', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const pongHandler = messageHandlers.get('pong')

      expect(pongHandler).toBeDefined()
      pongHandler?.({
        type: 'pong',
        timestamp: new Date().toISOString(),
        active_connections: 1,
        rtt_ms: 12,
      })
      expect(useAppStore.getState().setConnectionLag).toHaveBeenCalledWith(12)
    })
    it('ignores pong message without rtt_ms', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const pongHandler = messageHandlers.get('pong')

      pongHandler?.({ type: 'pong', timestamp: new Date().toISOString(), active_connections: 1 })
      expect(useAppStore.getState().setConnectionLag).not.toHaveBeenCalled()
    })
    describe('type guard branches', () => {
      it('order handler ignores non-order messages', () => {
        queryClient.setQueryData(['orders', undefined], [])
        const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const orderHandler = messageHandlers.get('order')

        orderHandler?.({ type: 'execution' })
        expect(setQueryDataSpy).not.toHaveBeenCalled()
        setQueryDataSpy.mockRestore()
      })
      it('execution handler ignores non-execution messages', () => {
        queryClient.setQueryData(['executions', undefined], [])
        const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const execHandler = messageHandlers.get('execution')

        execHandler?.({ type: 'order' })
        expect(setQueryDataSpy).not.toHaveBeenCalled()
        setQueryDataSpy.mockRestore()
      })
      it('signal handler ignores non-signal messages', () => {
        queryClient.setQueryData(['signals', undefined, 50, undefined, 24], [])
        const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const signalHandler = messageHandlers.get('signal')

        signalHandler?.({ type: 'execution' })
        expect(setQueryDataSpy).not.toHaveBeenCalled()
        setQueryDataSpy.mockRestore()
      })
      it('candle handler ignores non-candle messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const candleHandler = messageHandlers.get('candle')

        candleHandler?.({ type: 'execution' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('tick handler ignores non-tick messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const tickHandler = messageHandlers.get('tick')

        tickHandler?.({ type: 'execution' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('trade handler ignores non-trade messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const tradeHandler = messageHandlers.get('trade')

        tradeHandler?.({ type: 'execution' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('heartbeat handler ignores non-heartbeat messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const heartbeatHandler = messageHandlers.get('heartbeat')

        heartbeatHandler?.({ type: 'execution' })
        expect(useAppStore.getState().setConnectionLag).not.toHaveBeenCalled()
      })
    })
  })
  describe('trade buffering', () => {
    it('buffers order messages when buffering active and no cache exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const orderMessage: OrderData = {
        type: 'order',
        client_order_id: 'client-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      }
      const orderHandler = messageHandlers.get('order')

      orderHandler?.(orderMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('buffers execution messages when buffering active and no cache exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const execMessage: ExecutionData = {
        type: 'execution',
        client_order_id: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: new Date().toISOString(),
      }
      const execHandler = messageHandlers.get('execution')

      execHandler?.(execMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('drops execution message when no cache and no buffer active', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const execMessage: ExecutionData = {
        type: 'execution',
        client_order_id: 'ord-drop',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: new Date().toISOString(),
      }
      const execHandler = messageHandlers.get('execution')

      execHandler?.(execMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('drops signal message when no cache and no buffer active', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalMessage: SignalData = {
        type: 'signal',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        strength: 0.9,
        reason: 'test',
        timestamp: new Date().toISOString(),
      }
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.(signalMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('buffers signal messages when buffering active and no cache exists', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const signalMessage: SignalData = {
        type: 'signal',
        fired_at: new Date().toISOString(),
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Test signal',
        strategy_name: 'test_strategy',
        price: 50000,
      }
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.(signalMessage)
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('flushTradeBuffer replays buffered orders into cache', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const orderHandler = messageHandlers.get('order')

      orderHandler?.({
        type: 'order',
        client_order_id: 'buffered-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      })
      queryClient.setQueryData(['orders', undefined], [])
      dispatcher.flushTradeBuffer()
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>(['orders', undefined])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.client_order_id).toBe('buffered-1')
    })
    it('flushTradeBuffer replays buffered executions into cache', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const execHandler = messageHandlers.get('execution')

      execHandler?.({
        type: 'execution',
        client_order_id: 'buffered-exec-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: new Date().toISOString(),
      })
      queryClient.setQueryData(['executions', undefined], [])
      dispatcher.flushTradeBuffer()
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>([
        'executions',
        undefined,
      ])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.client_order_id).toBe('buffered-exec-1')
    })
    it('flushTradeBuffer replays buffered signals into cache', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.({
        type: 'signal',
        fired_at: new Date().toISOString(),
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Buffered signal',
        strategy_name: 'test_strategy',
        price: 50000,
      })
      queryClient.setQueryData(['signals', undefined, 50, undefined, 24], [])
      dispatcher.flushTradeBuffer()
      const cached = queryClient.getQueryData<{ reason: string }[]>([
        'signals',
        undefined,
        50,
        undefined,
        24,
      ])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.reason).toBe('Buffered signal')
    })
    it('flushTradeBuffer is a no-op when no buffers exist', () => {
      const setQueryDataSpy = vi.spyOn(queryClient, 'setQueryData')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.flushTradeBuffer()
      expect(setQueryDataSpy).not.toHaveBeenCalled()
      setQueryDataSpy.mockRestore()
    })
    it('stopTradeBuffering discards buffered messages', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      dispatcher.startTradeBuffering()
      const orderHandler = messageHandlers.get('order')

      orderHandler?.({
        type: 'order',
        client_order_id: 'discarded-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      })
      dispatcher.stopTradeBuffering()
      queryClient.setQueryData(['orders', undefined], [])
      dispatcher.flushTradeBuffer()
      const cached = queryClient.getQueryData<unknown[]>(['orders', undefined])

      expect(cached).toHaveLength(0)
    })
    it('order message without cache and without buffering does not throw', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderHandler = messageHandlers.get('order')

      expect(() =>
        orderHandler?.({
          type: 'order',
          client_order_id: 'no-cache-1',
          instrument: 'BTC/USD',
          exchange: 'kraken',
          side: 'buy',
          order_type: 'limit',
          size: 1,
          price: 50000,
          status: 'new',
          filled_size: 0,
          created_at: new Date().toISOString(),
          updated_at: null,
        })
      ).not.toThrow()
    })
    it('order merge skips query cache entries with undefined data', () => {
      queryClient.setQueryData(['orders', undefined], [])
      queryClient.getQueryCache().build(queryClient, { queryKey: ['orders', 'stale'] })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderHandler = messageHandlers.get('order')

      orderHandler?.({
        type: 'order',
        client_order_id: 'skip-undef-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      })
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>(['orders', undefined])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.client_order_id).toBe('skip-undef-1')
    })
    it('execution merge skips query cache entries with undefined data', () => {
      queryClient.setQueryData(['executions', undefined], [])
      queryClient.getQueryCache().build(queryClient, { queryKey: ['executions', 'stale'] })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const execHandler = messageHandlers.get('execution')

      execHandler?.({
        type: 'execution',
        client_order_id: 'skip-undef-exec',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: new Date().toISOString(),
      })
      const cached = queryClient.getQueryData<{ client_order_id: string }[]>([
        'executions',
        undefined,
      ])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.client_order_id).toBe('skip-undef-exec')
    })
    it('signal merge skips query cache entries with undefined data', () => {
      queryClient.setQueryData(['signals', undefined, 50, undefined, 24], [])
      queryClient.getQueryCache().build(queryClient, { queryKey: ['signals', 'stale'] })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalHandler = messageHandlers.get('signal')

      signalHandler?.({
        type: 'signal',
        fired_at: new Date().toISOString(),
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Skip undef signal',
        strategy_name: 'test',
        price: 50000,
      })
      const cached = queryClient.getQueryData<{ reason: string }[]>([
        'signals',
        undefined,
        50,
        undefined,
        24,
      ])

      expect(cached).toHaveLength(1)
      expect(cached?.[0]?.reason).toBe('Skip undef signal')
    })
    it('order message merges into multiple query caches', () => {
      queryClient.setQueryData(['orders', undefined], [])
      queryClient.setQueryData(['orders', { symbol: 'BTC/USD' }], [])
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderHandler = messageHandlers.get('order')

      orderHandler?.({
        type: 'order',
        client_order_id: 'multi-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: new Date().toISOString(),
        updated_at: null,
      })
      const cached1 = queryClient.getQueryData<unknown[]>(['orders', undefined])
      const cached2 = queryClient.getQueryData<unknown[]>(['orders', { symbol: 'BTC/USD' }])

      expect(cached1).toHaveLength(1)
      expect(cached2).toHaveLength(1)
    })
  })
})
