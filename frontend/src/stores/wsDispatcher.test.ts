import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { QueryClient } from '@tanstack/react-query'
import { WSDispatcher, getDispatcher, resetDispatcher } from './wsDispatcher'
import WebSocketClient from '../lib/websocket/client'
import { useTradeStore } from './trade'
import { useMarketStore } from './market'
import { useAppStore } from './app'
import { useProcessStore } from './process'
import type {
  OrderStatusEnvelope,
  FillEnvelope,
  SignalEnvelope,
  BarEnvelope,
  TradeEnvelope,
  HeartbeatEnvelope,
} from '../types/ws'

vi.mock('./trade', () => ({
  useTradeStore: {
    getState: vi.fn(),
  },
}))
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
vi.mock('./process', () => ({
  useProcessStore: {
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
    } as unknown as WebSocketClient
    const mockTradeStore = {
      orders: [],
      addOrder: vi.fn(),
      updateOrder: vi.fn(),
      addExecution: vi.fn(),
      addSignal: vi.fn(),
    }

    vi.mocked(useTradeStore.getState).mockReturnValue(mockTradeStore as never)
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
    const mockProcessStore = {
      updateFeedStatus: vi.fn(),
      updateExecutorStatus: vi.fn(),
      updateBrokerStatus: vi.fn(),
    }

    vi.mocked(useProcessStore.getState).mockReturnValue(mockProcessStore as never)
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
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('order_status', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('fill', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('signal', expect.any(Function))
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('bar', expect.any(Function))
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
    it('handles order message and updates store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = '2026-01-15T10:30:00Z'
      const orderMessage: OrderStatusEnvelope = {
        type: 'order_status',
        id: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'new',
        filled_size: 0,
        created_at: nowIso,
        updated_at: null,
      }
      const orderHandler = messageHandlers.get('order_status')

      expect(orderHandler).toBeDefined()
      orderHandler?.(orderMessage)
      expect(useTradeStore.getState().addOrder).toHaveBeenCalledWith({
        id: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        averagePrice: null,
        status: 'new',
        createdAt: new Date(nowIso),
        updatedAt: null,
      })
    })
    it('handles order message and updates existing order', () => {
      const existingOrder = { id: '1', status: 'new' }
      const mockTradeStore = {
        orders: [existingOrder],
        addOrder: vi.fn(),
        updateOrder: vi.fn(),
        addExecution: vi.fn(),
        addSignal: vi.fn(),
      }

      vi.mocked(useTradeStore.getState).mockReturnValue(mockTradeStore as never)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = '2026-01-15T10:30:00Z'
      const orderMessage: OrderStatusEnvelope = {
        type: 'order_status',
        id: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        order_type: 'limit',
        size: 1,
        price: 50000,
        status: 'filled',
        filled_size: 0,
        created_at: nowIso,
        updated_at: nowIso,
      }
      const orderHandler = messageHandlers.get('order_status')

      orderHandler?.(orderMessage)
      expect(mockTradeStore.updateOrder).toHaveBeenCalledWith(
        '1',
        expect.objectContaining({ id: '1', status: 'filled' })
      )
      expect(mockTradeStore.addOrder).not.toHaveBeenCalled()
    })
    it('handles execution message and updates store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = '2026-01-15T10:30:00Z'
      const execMessage: FillEnvelope = {
        type: 'fill',
        id: '1',
        order_id: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: nowIso,
      }
      const execHandler = messageHandlers.get('fill')

      expect(execHandler).toBeDefined()
      execHandler?.(execMessage)
      expect(useTradeStore.getState().addExecution).toHaveBeenCalledWith({
        id: '1',
        orderId: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        feeAsset: 'USD',
        status: 'filled',
        executedAt: new Date(nowIso),
      })
    })
    it('handles signal message and updates store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalMessage: SignalEnvelope = {
        type: 'signal',
        id: '1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Test signal',
        strategy_name: 'test_strategy',
        timestamp: new Date().toISOString(),
      }
      const signalHandler = messageHandlers.get('signal')

      expect(signalHandler).toBeDefined()
      signalHandler?.(signalMessage)
      expect(useTradeStore.getState().addSignal).toHaveBeenCalled()
    })
    it('handles candle message and updates market store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: BarEnvelope = {
        type: 'bar',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        timeframe: '1m',
        timestamp: nowIso,
        open: 49000,
        high: 51000,
        low: 48500,
        close: 50500,
        volume: 100,
      }
      const candleHandler = messageHandlers.get('bar')

      expect(candleHandler).toBeDefined()
      candleHandler?.(candleMessage)
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalledWith(50500)
    })
    it('skips candle last price update when close is undefined', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage = {
        type: 'bar',
        instrument: 'BTC-USD',
        exchange: 'test',
        timeframe: '1m',
        timestamp: nowIso,
        open: 49000,
        high: 51000,
        low: 48500,
        close: undefined,
        volume: 100,
      } as unknown as BarEnvelope
      const candleHandler = messageHandlers.get('bar')

      candleHandler?.(candleMessage)
      expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
    })
    it('handles candle message with instrument/timeframe - invalidates specific candle queries', () => {
      const invalidateQueriesSpy = vi.spyOn(queryClient, 'invalidateQueries')
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: BarEnvelope = {
        type: 'bar',
        instrument: 'BTC-USD',
        exchange: 'test',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        timestamp: nowIso,
      }
      const candleHandler = messageHandlers.get('bar')

      candleHandler?.(candleMessage)
      expect(invalidateQueriesSpy).toHaveBeenCalledWith({
        queryKey: ['candles', 'BTC-USD', 'test', '1m'],
      })
    })
    it('order invalidation predicate correctly filters queries', () => {
      let capturedPredicate: ((query: { queryKey: unknown[] }) => boolean) | undefined
      const invalidateQueriesSpy = vi
        .spyOn(queryClient, 'invalidateQueries')
        .mockImplementation(options => {
          if (options && typeof options === 'object' && 'predicate' in options) {
            capturedPredicate = options.predicate as unknown as (query: {
              queryKey: unknown[]
            }) => boolean
          }

          return Promise.resolve()
        })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const orderMessage: OrderStatusEnvelope = {
        type: 'order_status',
        id: '1',
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
      const orderHandler = messageHandlers.get('order_status')

      orderHandler?.(orderMessage)
      expect(capturedPredicate).toBeDefined()
      expect(capturedPredicate?.({ queryKey: ['orders'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['orders', 'kraken'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['executions'] })).toBe(false)
      invalidateQueriesSpy.mockRestore()
    })
    it('execution invalidation predicate correctly filters queries', () => {
      let capturedPredicate: ((query: { queryKey: unknown[] }) => boolean) | undefined
      const invalidateQueriesSpy = vi
        .spyOn(queryClient, 'invalidateQueries')
        .mockImplementation(options => {
          if (options && typeof options === 'object' && 'predicate' in options) {
            capturedPredicate = options.predicate as unknown as (query: {
              queryKey: unknown[]
            }) => boolean
          }

          return Promise.resolve()
        })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const execMessage: FillEnvelope = {
        type: 'fill',
        id: '1',
        order_id: 'ord-1',
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
      const execHandler = messageHandlers.get('fill')

      execHandler?.(execMessage)
      expect(capturedPredicate).toBeDefined()
      expect(capturedPredicate?.({ queryKey: ['executions'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['executions', 'filter'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['orders'] })).toBe(false)
      invalidateQueriesSpy.mockRestore()
    })
    it('signal invalidation predicate correctly filters queries', () => {
      let capturedPredicate: ((query: { queryKey: unknown[] }) => boolean) | undefined
      const invalidateQueriesSpy = vi
        .spyOn(queryClient, 'invalidateQueries')
        .mockImplementation(options => {
          if (options && typeof options === 'object' && 'predicate' in options) {
            capturedPredicate = options.predicate as unknown as (query: {
              queryKey: unknown[]
            }) => boolean
          }

          return Promise.resolve()
        })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const signalMessage: SignalEnvelope = {
        type: 'signal',
        timestamp: new Date().toISOString(),
        id: '1',
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
      expect(capturedPredicate).toBeDefined()
      expect(capturedPredicate?.({ queryKey: ['signals'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['signals', 'strategy'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['orders'] })).toBe(false)
      invalidateQueriesSpy.mockRestore()
    })
    it('candle message with instrument and timeframe uses specific queryKey', () => {
      let capturedQueryKey: unknown[] | undefined
      const invalidateQueriesSpy = vi
        .spyOn(queryClient, 'invalidateQueries')
        .mockImplementation(options => {
          if (options && typeof options === 'object' && 'queryKey' in options) {
            capturedQueryKey = options.queryKey as unknown[]
          }

          return Promise.resolve()
        })
      const nowIso = new Date().toISOString()
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const candleMessage: BarEnvelope = {
        type: 'bar',
        instrument: 'BTC-USD',
        exchange: 'test',
        timeframe: '1m',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        timestamp: nowIso,
      }
      const candleHandler = messageHandlers.get('bar')

      candleHandler?.(candleMessage)
      expect(capturedQueryKey).toBeDefined()
      expect(capturedQueryKey).toEqual(['candles', 'BTC-USD', 'test', '1m'])
      invalidateQueriesSpy.mockRestore()
    })
    it('candle message without instrument/timeframe invalidates via predicate', () => {
      let capturedPredicate: ((query: { queryKey: unknown[] }) => boolean) | undefined
      const invalidateQueriesSpy = vi
        .spyOn(queryClient, 'invalidateQueries')
        .mockImplementation(options => {
          if (options && typeof options === 'object' && 'predicate' in options) {
            capturedPredicate = options.predicate as unknown as (query: {
              queryKey: unknown[]
            }) => boolean
          }

          return Promise.resolve()
        })
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const candleMessage: BarEnvelope = {
        type: 'bar',
        instrument: '',
        exchange: 'test',
        timeframe: '',
        open: 50000,
        high: 51000,
        low: 49000,
        close: 50500,
        volume: 100,
        timestamp: nowIso,
      }
      const candleHandler = messageHandlers.get('bar')

      candleHandler?.(candleMessage)
      expect(capturedPredicate).toBeDefined()
      expect(capturedPredicate?.({ queryKey: ['candles'] })).toBe(true)
      expect(capturedPredicate?.({ queryKey: ['orders'] })).toBe(false)
      invalidateQueriesSpy.mockRestore()
    })
    it('handles tick message without last price - calculates mid price', () => {
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
    it('handles tick message with last price', () => {
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
    it('handles trade message and updates market store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const tradeMessage: TradeEnvelope = {
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
      expect(useMarketStore.getState().updateLastPrice).toHaveBeenCalledWith(50250)
    })
    it('handles heartbeat message and updates app store', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
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
      expect(useAppStore.getState().setConnectionLag).toHaveBeenCalledWith(50)
      expect(useAppStore.getState().updateLastUpdate).toHaveBeenCalled()
    })
    it('handles heartbeat with feed component and updates ProcessStore', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'feed',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 10,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useProcessStore.getState().updateFeedStatus).toHaveBeenCalledWith(
        'feed',
        expect.objectContaining({
          running: true,
          details: { lag_ms: 10 },
        })
      )
    })
    it('handles heartbeat with suffixed feed component (feed.kraken)', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'feed.kraken',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 5,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useProcessStore.getState().updateFeedStatus).toHaveBeenCalledWith(
        'feed_kraken',
        expect.objectContaining({
          running: true,
          details: { lag_ms: 5 },
        })
      )
    })
    it('handles heartbeat with executor component', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'executor_binance',
        status: 'error',
        timestamp: nowIso,
        lag_ms: 0,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useProcessStore.getState().updateExecutorStatus).toHaveBeenCalledWith(
        'executor_binance',
        expect.objectContaining({
          running: false,
          details: { lag_ms: 0 },
        })
      )
    })
    it('handles heartbeat without timestamp (uses Date.now fallback)', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const heartbeatMessage = {
        type: 'heartbeat',
        component: 'feed_test',
        status: 'healthy',
        lag_ms: 5,
        sequence: 1,
      } as HeartbeatEnvelope
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useProcessStore.getState().updateFeedStatus).toHaveBeenCalledWith(
        'feed_test',
        expect.objectContaining({
          running: true,
          lastHeartbeat: expect.any(Number),
          details: { lag_ms: 5 },
        })
      )
    })
    it('handles heartbeat with broker component', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'broker',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 15,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(useProcessStore.getState().updateBrokerStatus).toHaveBeenCalledWith(
        'broker',
        expect.objectContaining({
          running: true,
          details: { lag_ms: 15 },
        })
      )
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
      expect(useAppStore.getState().setSubscribedTopics).not.toHaveBeenCalled()
    })
    it('updates app store when disconnected', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const connectionHandler = connectionHandlers[0]

      connectionHandler?.(false)
      expect(useAppStore.getState().setConnected).toHaveBeenCalledWith(false)
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
  describe('directStoreUpdates option', () => {
    it('does not update stores when directStoreUpdates is false', () => {
      const dispatcher = new WSDispatcher({
        queryClient,
        directStoreUpdates: false,
      })

      dispatcher.attach(mockWsClient)
      const orderMessage: OrderStatusEnvelope = {
        type: 'order_status',
        id: '1',
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
      const orderHandler = messageHandlers.get('order_status')

      orderHandler?.(orderMessage)
      expect(useTradeStore.getState().addOrder).not.toHaveBeenCalled()
    })
    it('skips updates for other message types when directStoreUpdates is false', () => {
      const dispatcher = new WSDispatcher({
        queryClient,
        directStoreUpdates: false,
      })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const execMessage: FillEnvelope = {
        type: 'fill',
        id: 'exec-1',
        order_id: 'ord-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        size: 1,
        price: 50000,
        fee: 0.1,
        fee_asset: 'USD',
        status: 'filled',
        executed_at: nowIso,
      }
      const signalMessage: SignalEnvelope = {
        type: 'signal',
        id: 'sig-1',
        exchange: 'kraken',
        instrument: 'BTC/USD',
        side: 'buy',
        strength: 0.8,
        reason: 'Test signal',
        strategy_name: 'test_strategy',
        timestamp: nowIso,
      }
      const candleMessage: BarEnvelope = {
        type: 'bar',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        timeframe: '1m',
        timestamp: nowIso,
        open: 49000,
        high: 51000,
        low: 48500,
        close: 50500,
        volume: 100,
      }
      const tickMessage = {
        type: 'tick',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        bid: 50000,
        ask: 51000,
        last: 50500,
        timestamp: nowIso,
      }
      const tradeMessage: TradeEnvelope = {
        type: 'trade',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        price: 50250,
        volume: 1.25,
        side: 'buy',
        timestamp: nowIso,
      }

      messageHandlers.get('fill')?.(execMessage)
      messageHandlers.get('signal')?.(signalMessage)
      messageHandlers.get('bar')?.(candleMessage)
      messageHandlers.get('tick')?.(tickMessage)
      messageHandlers.get('trade')?.(tradeMessage)
      expect(useTradeStore.getState().addExecution).not.toHaveBeenCalled()
      expect(useTradeStore.getState().addSignal).not.toHaveBeenCalled()
      expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
    })
    it('handles heartbeat with strategy component', () => {
      const mockProcessStore = {
        updateFeedStatus: vi.fn(),
        updateExecutorStatus: vi.fn(),
        updateBrokerStatus: vi.fn(),
        updateStrategyStatus: vi.fn(),
      }

      vi.mocked(useProcessStore.getState).mockReturnValue(mockProcessStore as never)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'strategy_macd',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 5,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(mockProcessStore.updateStrategyStatus).toHaveBeenCalledWith(
        'strategy_macd',
        expect.objectContaining({
          running: true,
          details: { lag_ms: 5 },
        })
      )
    })
    it('handles heartbeat without data', () => {
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const heartbeatMessage = {
        type: 'heartbeat',
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      expect(() => heartbeatHandler?.(heartbeatMessage)).not.toThrow()
    })
    it('handles heartbeat with dot separator in component', () => {
      const mockProcessStore = {
        updateFeedStatus: vi.fn(),
        updateExecutorStatus: vi.fn(),
        updateBrokerStatus: vi.fn(),
        updateStrategyStatus: vi.fn(),
      }

      vi.mocked(useProcessStore.getState).mockReturnValue(mockProcessStore as never)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage: HeartbeatEnvelope = {
        type: 'heartbeat',
        component: 'feed.kraken',
        status: 'healthy',
        timestamp: nowIso,
        lag_ms: 10,
        sequence: 1,
      }
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(mockProcessStore.updateFeedStatus).toHaveBeenCalledWith(
        'feed_kraken',
        expect.objectContaining({
          running: true,
        })
      )
    })
    it('handles heartbeat without lag_ms', () => {
      const mockProcessStore = {
        updateFeedStatus: vi.fn(),
        updateExecutorStatus: vi.fn(),
        updateBrokerStatus: vi.fn(),
        updateStrategyStatus: vi.fn(),
      }

      vi.mocked(useProcessStore.getState).mockReturnValue(mockProcessStore as never)
      const dispatcher = new WSDispatcher({ queryClient })

      dispatcher.attach(mockWsClient)
      const nowIso = new Date().toISOString()
      const heartbeatMessage = {
        type: 'heartbeat',
        component: 'executor_binance',
        status: 'healthy',
        timestamp: nowIso,
      } as unknown as HeartbeatEnvelope
      const heartbeatHandler = messageHandlers.get('heartbeat')

      heartbeatHandler?.(heartbeatMessage)
      expect(mockProcessStore.updateExecutorStatus).toHaveBeenCalledWith(
        'executor_binance',
        expect.objectContaining({
          running: true,
          details: { lag_ms: undefined },
        })
      )
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
      expect(useAppStore.getState().setConnectionLag).toHaveBeenCalledWith(5)
      expect(useProcessStore.getState().updateFeedStatus).not.toHaveBeenCalled()
    })
    describe('type guard branches', () => {
      it('order handler ignores non-order messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const orderHandler = messageHandlers.get('order_status')

        orderHandler?.({ type: 'fill' })
        expect(useTradeStore.getState().addOrder).not.toHaveBeenCalled()
        expect(useTradeStore.getState().updateOrder).not.toHaveBeenCalled()
      })
      it('execution handler ignores non-execution messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const execHandler = messageHandlers.get('fill')

        execHandler?.({ type: 'order_status' })
        expect(useTradeStore.getState().addExecution).not.toHaveBeenCalled()
      })
      it('signal handler ignores non-signal messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const signalHandler = messageHandlers.get('signal')

        signalHandler?.({ type: 'fill' })
        expect(useTradeStore.getState().addSignal).not.toHaveBeenCalled()
      })
      it('candle handler ignores non-candle messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const candleHandler = messageHandlers.get('bar')

        candleHandler?.({ type: 'fill' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('tick handler ignores non-tick messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const tickHandler = messageHandlers.get('tick')

        tickHandler?.({ type: 'fill' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('trade handler ignores non-trade messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const tradeHandler = messageHandlers.get('trade')

        tradeHandler?.({ type: 'fill' })
        expect(useMarketStore.getState().updateLastPrice).not.toHaveBeenCalled()
      })
      it('heartbeat handler ignores non-heartbeat messages', () => {
        const dispatcher = new WSDispatcher({ queryClient })

        dispatcher.attach(mockWsClient)
        const heartbeatHandler = messageHandlers.get('heartbeat')

        heartbeatHandler?.({ type: 'fill' })
        expect(useAppStore.getState().setConnectionLag).not.toHaveBeenCalled()
      })
    })
  })
})
