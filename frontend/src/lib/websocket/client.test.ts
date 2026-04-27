import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import WebSocketClient from './client'
import { getWsToken, isAuthControlMessage } from './auth'
import { shouldReconnect } from './reconnect'
import {
  createAuthRequired,
  createAuthOk,
  createAuthComplete,
  createAuthFailed,
  createAuthExpired,
  createReauthRequired,
  createReauthOk,
  createCandle,
  createTick,
  createHeartbeat,
  createPong,
  createSubscriptionsList,
} from '@/test/wsMessageFactories'

vi.mock('./topics', () => ({
  getMessageTopic: vi.fn((msg: any) => {
    if (msg.type === 'candle' && msg.exchange && msg.instrument && msg.timeframe) {
      return `market.${msg.exchange}.${msg.instrument}.candles.${msg.timeframe}`
    }

    if (msg.type === 'tick' && msg.exchange && msg.instrument) {
      return `market.${msg.exchange}.${msg.instrument}.ticks`
    }

    return msg.topic || msg.type || null
  }),
  shouldThrottle: vi.fn((type: string) =>
    ['candle', 'order', 'execution', 'position'].includes(type)
  ),
  buildMarketTopic: vi.fn((type: string, inst: string) => `market:${type}:${inst}`),
  MARKET_TOPIC_PREFIX: 'market.',
  ORDERS_COMMANDS_PREFIX: 'orders.commands.',
  ORDERS_EVENTS_PREFIX: 'orders.events.',
  SIGNALS_TOPIC_PREFIX: 'signals.',
  HEARTBEATS_TOPIC_PREFIX: 'system.heartbeats.',
  AI_REVIEWS_TOPIC_PREFIX: 'ai_reviews.',
  getSubscriptionTopics: vi.fn(() => [
    'market.',
    'orders.commands.',
    'orders.events.',
    'signals.',
    'strategy.',
    'system.heartbeats.',
    'ai_reviews.',
  ]),
}))
vi.mock('./reconnect', () => ({
  calculateReconnectDelay: vi.fn(() => 100),
  shouldReconnect: vi.fn(() => true),
  createHeartbeatMessage: vi.fn(() => ({ type: 'ping' })),
  flushThrottledMessages: vi.fn(),
  buildWebSocketUrl: vi.fn(() => 'ws://localhost/ws'),
}))
vi.mock('./auth', () => ({
  getWsToken: vi.fn(async () => ({ token: 'test-token', exp: Date.now() / 1000 + 3600 })),
  isAuthControlMessage: vi.fn(
    (msg: any) =>
      msg.type?.startsWith('auth_') || msg.type === 'reauth_required' || msg.type === 'reauth_ok'
  ),
}))

let mockSeqCounter = 0

vi.mock('uuid', () => ({
  v7: vi.fn(() => `00000000-0000-7000-8000-${String(++mockSeqCounter).padStart(12, '0')}`),
}))
vi.mock('../sequenceTracker', () => ({
  getTracker: vi.fn(() => ({
    sessionId: 'test-session-id',
    nextSequence: vi.fn(() => ++mockSeqCounter),
  })),
}))

class MockWebSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  readonly url: string
  readyState: number = MockWebSocket.CONNECTING
  onopen: ((this: WebSocket, ev: Event) => any) | null = null
  onclose: ((this: WebSocket, ev: CloseEvent) => any) | null = null
  onmessage: ((this: WebSocket, ev: MessageEvent) => any) | null = null
  onerror: ((this: WebSocket, ev: Event) => any) | null = null
  constructor(url: string) {
    this.url = url
    setTimeout(() => {
      this.readyState = MockWebSocket.OPEN
      this.onopen?.call(this as unknown as WebSocket, new Event('open'))
    }, 10)
  }
  send = vi.fn()
  close = vi.fn(() => {
    this.readyState = MockWebSocket.CLOSED
    this.onclose?.call(this as unknown as WebSocket, new CloseEvent('close'))
  })
}

describe('WebSocketClient', () => {
  let client: WebSocketClient
  let originalWebSocket: typeof WebSocket

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    mockSeqCounter = 0
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    originalWebSocket = globalThis.WebSocket
    globalThis.WebSocket = MockWebSocket as unknown as typeof WebSocket
    client = new WebSocketClient({ secure: false })
  })
  afterEach(() => {
    client.disconnect()
    globalThis.WebSocket = originalWebSocket
    vi.useRealTimers()
  })
  describe('constructor', () => {
    it('creates client with default options', () => {
      const defaultClient = new WebSocketClient()

      expect(defaultClient).toBeDefined()
      defaultClient.disconnect()
    })
    it('creates client with custom options', () => {
      const customClient = new WebSocketClient({
        url: 'ws://custom/ws',
        reconnectInterval: 5000,
        maxReconnectAttempts: 5,
        heartbeatInterval: 60000,
        throttleInterval: 500,
        secure: true,
      })

      expect(customClient).toBeDefined()
      customClient.disconnect()
    })
  })
  describe('connect', () => {
    it('establishes WebSocket connection', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      expect(client.isConnected()).toBe(true)
    })
    it('does not reconnect if already connected', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const initialWs = (client as any).ws

      client.connect()
      expect((client as any).ws).toBe(initialWs)
    })
    it('does not reconnect if connecting', () => {
      client.connect()
      const initialWs = (client as any).ws

      client.connect()
      expect((client as any).ws).toBe(initialWs)
    })
    it('assigns websocket event handlers', () => {
      client.connect()
      const mockWs = (client as any).ws

      expect(mockWs.onopen).toEqual(expect.any(Function))
      expect(mockWs.onmessage).toEqual(expect.any(Function))
      expect(mockWs.onclose).toEqual(expect.any(Function))
      expect(mockWs.onerror).toEqual(expect.any(Function))
    })
    it('restarts throttling when timer is missing', () => {
      const setupSpy = vi.spyOn(client as any, 'setupThrottling')

      ;(client as any).throttleTimer = null
      client.connect()
      expect(setupSpy).toHaveBeenCalled()
    })
    it('skips throttling setup when timer already exists', () => {
      const setupSpy = vi.spyOn(client as any, 'setupThrottling')

      ;(client as any).throttleTimer = 1
      client.connect()
      expect(setupSpy).not.toHaveBeenCalled()
    })
    it('covers throttling setup branches explicitly', async () => {
      const setupSpy = vi.spyOn(client as any, 'setupThrottling')

      ;(client as any).ws = null
      ;(client as any).throttleTimer = null
      client.connect()
      await vi.advanceTimersByTimeAsync(20)
      ;(client as any).ws = null
      ;(client as any).throttleTimer = 123
      client.connect()
      await vi.advanceTimersByTimeAsync(20)
      expect(setupSpy).toHaveBeenCalledTimes(1)
      setupSpy.mockRestore()
    })
    it('only marks ready on open for non-secure clients', () => {
      const readySpy = vi.spyOn(client as any, 'onConnectionReady')

      ;(client as any).handleOpen()
      expect(readySpy).toHaveBeenCalled()
      readySpy.mockRestore()
      const secureClient = new WebSocketClient({ secure: true })
      const secureReadySpy = vi.spyOn(secureClient as any, 'onConnectionReady')

      ;(secureClient as any).handleOpen()
      expect(secureReadySpy).not.toHaveBeenCalled()
      secureReadySpy.mockRestore()
      secureClient.disconnect()
    })
  })
  describe('disconnect', () => {
    it('closes WebSocket connection', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.disconnect()
      expect(client.isConnected()).toBe(false)
    })
    it('clears all timers and state', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.disconnect()
      expect((client as any).ws).toBe(null)
      expect((client as any).heartbeatTimer).toBe(null)
      expect((client as any).throttleTimer).toBe(null)
    })
    it('skips clearing timers when none are set', () => {
      const existingThrottle = (client as any).throttleTimer

      if (existingThrottle) {
        clearInterval(existingThrottle)
      }

      ;(client as any).heartbeatTimer = null
      ;(client as any).throttleTimer = null
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

      client.disconnect()
      expect(clearIntervalSpy).not.toHaveBeenCalled()
      clearIntervalSpy.mockRestore()
    })
    it('clears timers only when present', () => {
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

      ;(client as any).heartbeatTimer = null
      ;(client as any).throttleTimer = null
      client.disconnect()
      expect(clearIntervalSpy).not.toHaveBeenCalled()
      clearIntervalSpy.mockClear()
      ;(client as any).heartbeatTimer = setInterval(() => {}, 1000)
      ;(client as any).throttleTimer = setInterval(() => {}, 1000)
      client.disconnect()
      expect(clearIntervalSpy).toHaveBeenCalled()
      clearIntervalSpy.mockRestore()
    })
    it('does not reconnect after intentional disconnect', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.disconnect()
      expect((client as any).isReconnecting).toBe(false)
      expect((client as any).intentionalDisconnect).toBe(false)
      await vi.advanceTimersByTimeAsync(5000)
      expect((client as any).ws).toBe(null)
    })
  })
  describe('errors', () => {
    it('logs websocket errors', async () => {
      const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const errorEvent = new Event('error')

      mockWs.onerror?.(errorEvent)
      expect(consoleErrorSpy).toHaveBeenCalledWith('WebSocket error:', errorEvent)
      consoleErrorSpy.mockRestore()
    })
  })
  describe('send', () => {
    it('sends string messages', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send('test message')
      expect((client as any).ws.send).toHaveBeenCalledWith('test message')
    })
    it('serializes object messages', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send({ type: 'test', data: 'value' })
      expect((client as any).ws.send).toHaveBeenCalledWith('{"type":"test","data":"value"}')
    })
    it('does not send when not connected', () => {
      client.send('test')
    })
    it('skips sending when ws is missing even if connected', () => {
      const isConnectedSpy = vi.spyOn(client, 'isConnected').mockReturnValue(true)

      ;(client as any).ws = null
      client.send('test message')
      expect(isConnectedSpy).toHaveBeenCalled()
      isConnectedSpy.mockRestore()
    })
    it('covers send connection branches', () => {
      const warnSpy = vi.mocked(console.warn)

      client.send('offline')
      expect(warnSpy).toHaveBeenCalled()
      const isConnectedSpy = vi.spyOn(client, 'isConnected').mockReturnValue(true)

      ;(client as any).ws = null
      client.send('skip')
      const ws = { readyState: WebSocket.OPEN, send: vi.fn(), close: vi.fn() }

      ;(client as any).ws = ws
      client.send('online')
      expect(ws.send).toHaveBeenCalledWith('online')
      isConnectedSpy.mockRestore()
    })
  })
  describe('subscribe', () => {
    it('subscribes to topics', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.subscribe(['topic1', 'topic2'])
      await Promise.resolve()
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('subscribe')
      expect(parsed.topics).toEqual(['topic1', 'topic2'])
      expect(parsed.public_id).toBeDefined()
      expect(parsed.session_id).toBe('test-session-id')
      expect(parsed.sequence_id).toEqual(expect.any(Number))
    })
    it('tracks subscribed topics', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.subscribe(['topic1'])
      expect(client.getSubscribedTopics()).toContain('topic1')
    })
    it('does not send if not connected', () => {
      client.subscribe(['topic1'])
      expect(client.getSubscribedTopics()).toContain('topic1')
    })
    it('does not send subscription when disconnected', () => {
      const sendSpy = vi.spyOn(client, 'send')
      const isConnectedSpy = vi.spyOn(client, 'isConnected').mockReturnValue(false)

      client.subscribe(['topic2'])
      expect(sendSpy).not.toHaveBeenCalled()
      isConnectedSpy.mockRestore()
      sendSpy.mockRestore()
    })
    it('covers subscribe connection branch', async () => {
      const sendSpy = vi.spyOn(client, 'send')
      const isConnectedSpy = vi.spyOn(client, 'isConnected')

      isConnectedSpy.mockReturnValue(false)
      client.subscribe(['topicA'])
      await Promise.resolve()
      expect(sendSpy).not.toHaveBeenCalled()
      isConnectedSpy.mockReturnValue(true)
      client.subscribe(['topicB'])
      await Promise.resolve()
      expect(sendSpy).toHaveBeenCalledWith({ type: 'subscribe', topics: ['topicB'] })
      isConnectedSpy.mockRestore()
      sendSpy.mockRestore()
    })
  })
  describe('unsubscribe', () => {
    it('unsubscribes from topics', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.subscribe(['topic1', 'topic2'])
      await Promise.resolve()
      ;(client as any).ws.send.mockClear()
      client.unsubscribe(['topic1'])
      await Promise.resolve()
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('unsubscribe')
      expect(parsed.topics).toEqual(['topic1'])
      expect(parsed.public_id).toBeDefined()
      expect(parsed.session_id).toBe('test-session-id')
      expect(client.getSubscribedTopics()).not.toContain('topic1')
      expect(client.getSubscribedTopics()).toContain('topic2')
    })
    it('removes topics without sending when disconnected', () => {
      client.subscribe(['topic1', 'topic2'])
      client.unsubscribe(['topic1'])
      expect(client.getSubscribedTopics()).not.toContain('topic1')
      expect(client.getSubscribedTopics()).toContain('topic2')
    })
  })
  describe('onMessage', () => {
    it('registers message handler', async () => {
      const handler = vi.fn()

      client.onMessage('heartbeat', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const heartbeatMsg = createHeartbeat()

      mockWs.onmessage?.({ data: JSON.stringify(heartbeatMsg) })
      expect(handler).toHaveBeenCalledWith(heartbeatMsg)
    })
    it('returns unsubscribe function', async () => {
      const handler = vi.fn()
      const unsubscribe = client.onMessage('heartbeat', handler)

      unsubscribe()
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createHeartbeat()) })
      expect(handler).not.toHaveBeenCalled()
    })
    it('supports multiple handlers for the same type', async () => {
      const handlerA = vi.fn()
      const handlerB = vi.fn()

      client.onMessage('heartbeat', handlerA)
      client.onMessage('heartbeat', handlerB)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const heartbeatMsg = createHeartbeat()

      mockWs.onmessage?.({ data: JSON.stringify(heartbeatMsg) })
      expect(handlerA).toHaveBeenCalledWith(heartbeatMsg)
      expect(handlerB).toHaveBeenCalledWith(heartbeatMsg)
    })
    it('ignores handler registration when list is missing', async () => {
      const handler = vi.fn()

      ;(client as any).messageHandlers.set('heartbeat', undefined)
      client.onMessage('heartbeat', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createHeartbeat()) })
      expect(handler).not.toHaveBeenCalled()
    })
    it('handles repeated unsubscription safely', () => {
      const handler = vi.fn()
      const unsubscribe = client.onMessage('candle', handler)

      unsubscribe()
      unsubscribe()
    })
    it('handles unsubscription when handler is missing', () => {
      const handler = vi.fn()
      const unsubscribe = client.onMessage('tick', handler)
      const handlers = (client as any).messageHandlers.get('tick')

      handlers?.pop()
      expect(() => unsubscribe()).not.toThrow()
    })
    it('covers handler map branch paths', () => {
      const handler = vi.fn()
      const unsubscribe = client.onMessage('tick', handler)

      client.onMessage('tick', vi.fn())
      ;(client as any).messageHandlers.set('missing', undefined)
      client.onMessage('missing', vi.fn())
      ;(client as any).messageHandlers.delete('tick')
      expect(() => unsubscribe()).not.toThrow()
    })
    it('supports wildcard handler', async () => {
      const handler = vi.fn()

      client.onMessage('*', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const heartbeatMsg = createHeartbeat()

      mockWs.onmessage?.({ data: JSON.stringify(heartbeatMsg) })
      expect(handler).toHaveBeenCalled()
    })
  })
  describe('onConnection', () => {
    it('registers connection handler', async () => {
      const handler = vi.fn()

      client.onConnection(handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      expect(handler).toHaveBeenCalledWith(true)
    })
    it('returns unsubscribe function', async () => {
      const handler = vi.fn()
      const unsubscribe = client.onConnection(handler)

      unsubscribe()
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      expect(handler).not.toHaveBeenCalled()
    })
    it('handles repeated unsubscription safely', () => {
      const handler = vi.fn()
      const unsubscribe = client.onConnection(handler)

      unsubscribe()
      unsubscribe()
    })
    it('handles unsubscription when handler is missing', () => {
      const handler = vi.fn()
      const unsubscribe = client.onConnection(handler)

      ;(client as any).connectionHandlers = []
      expect(() => unsubscribe()).not.toThrow()
    })
    it('removes handler when unsubscribed and tolerates missing handler', () => {
      const handler = vi.fn()
      const unsubscribe = client.onConnection(handler)

      expect((client as any).connectionHandlers).toContain(handler)
      unsubscribe()
      expect((client as any).connectionHandlers).not.toContain(handler)
      unsubscribe()
    })
  })
  describe('topic subscription helpers', () => {
    beforeEach(async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
    })
    it('subscribeToCandles with instrument', () => {
      client.subscribeToCandles('BTC-USD')
      expect(client.getSubscribedTopics()).toContain('market:candles:BTC-USD')
    })
    it('subscribeToCandles without instrument', () => {
      client.subscribeToCandles()
      expect(client.getSubscribedTopics()).toContain('market.')
    })
    it('subscribeToTicks with instrument', () => {
      client.subscribeToTicks('ETH-USD')
      expect(client.getSubscribedTopics()).toContain('market:ticks:ETH-USD')
    })
    it('subscribeToTicks without instrument', () => {
      client.subscribeToTicks()
      expect(client.getSubscribedTopics()).toContain('market.')
    })
    it('subscribeToOrders', () => {
      client.subscribeToOrders()
      expect(client.getSubscribedTopics()).toContain('orders.commands.')
      expect(client.getSubscribedTopics()).toContain('orders.events.')
    })
    it('subscribeToExecutions', () => {
      client.subscribeToExecutions()
      expect(client.getSubscribedTopics()).toContain('orders.events.')
    })
    it('subscribeToSignals', () => {
      client.subscribeToSignals()
      expect(client.getSubscribedTopics()).toContain('signals.')
    })
    it('subscribeToHeartbeats', () => {
      client.subscribeToHeartbeats()
      expect(client.getSubscribedTopics()).toContain('system.heartbeats.')
    })
    it('subscribeToAll', () => {
      client.subscribeToAll()
      expect(client.getSubscribedTopics()).toContain('market.')
      expect(client.getSubscribedTopics()).toContain('orders.commands.')
      expect(client.getSubscribedTopics()).toContain('orders.events.')
    })
  })
  describe('message handling', () => {
    it('routes auth control messages through auth handler', async () => {
      const authSpy = vi.spyOn(client as any, 'handleAuthMessage').mockResolvedValue(undefined)

      vi.mocked(isAuthControlMessage).mockReturnValueOnce(true)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createCandle()) })
      expect(authSpy).toHaveBeenCalled()
      authSpy.mockRestore()
    })
    it('treats auth messages as non-auth when control check is false', async () => {
      const handler = vi.fn()

      client.onMessage('auth_required', handler)
      vi.mocked(isAuthControlMessage).mockReturnValueOnce(false)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createAuthRequired()) })
      expect(handler).toHaveBeenCalled()
    })
    it('ignores pong when no ping was sent', async () => {
      const handler = vi.fn()

      client.onMessage('*', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createPong()) })
      expect(handler).not.toHaveBeenCalled()
    })
    it('computes RTT from pong when ping was sent', async () => {
      const handler = vi.fn()

      client.onMessage('pong', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      ;(client as any).pingSentAt = Date.now() - 42
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createPong()) })
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'pong', rtt_ms: expect.any(Number) })
      )
      expect((client as any).pingSentAt).toBeNull()
    })
    it('handles non-string message data gracefully', async () => {
      const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: new ArrayBuffer(8) })
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        'Failed to parse WebSocket message:',
        expect.any(Error)
      )
      consoleErrorSpy.mockRestore()
    })
    it('handles parse errors gracefully', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      expect(() => {
        mockWs.onmessage?.({ data: 'invalid json {' })
      }).not.toThrow()
    })
    it('ignores messages missing type', async () => {
      const handler = vi.fn()

      client.onMessage('*', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify({ foo: 'bar' }) })
      expect(handler).not.toHaveBeenCalled()
    })
    it('drops messages that fail schema validation', async () => {
      const handler = vi.fn()

      client.onMessage('candle', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const invalidMessage = {
        type: 'candle',
        timestamp: new Date().toISOString(),
        invalid: true,
      }

      mockWs.onmessage?.({ data: JSON.stringify(invalidMessage) })
      expect(handler).not.toHaveBeenCalled()
    })
    it('handles schema parser returning null', async () => {
      const schemaModule = await import('../schemas/ws')
      const parseSpy = vi.spyOn(schemaModule, 'parseWsMessage').mockReturnValue(null)

      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createCandle()) })
      expect(parseSpy).toHaveBeenCalled()
      parseSpy.mockRestore()
    })
    it('calls getMessageTopic for valid messages', async () => {
      const { getMessageTopic } = await import('./topics')

      vi.mocked(getMessageTopic).mockClear()
      ;(client as any).handleMessage({ data: JSON.stringify(createCandle()) } as MessageEvent)
      expect(vi.mocked(getMessageTopic)).toHaveBeenCalled()
    })
    it('queues throttled messages by topic', async () => {
      const handler = vi.fn()

      client.onMessage('candle', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws
      const barMsg = createCandle()

      mockWs.onmessage?.({ data: JSON.stringify(barMsg) })
      const expectedTopic = 'market.kraken.BTC-USD.candles.1m'

      expect(handler).not.toHaveBeenCalled()
      expect((client as any).pendingMessages.get(expectedTopic)).toEqual(barMsg)
    })
    it('covers auth control and secure gating branches', () => {
      const secureClient = new WebSocketClient({ secure: true })
      const notifySpy = vi.spyOn(secureClient as any, 'notifyHandlers')
      const authSpy = vi
        .spyOn(secureClient as any, 'handleAuthMessage')
        .mockResolvedValue(undefined)

      ;(secureClient as any).handleMessage({
        data: JSON.stringify(createPong()),
      } as MessageEvent)
      expect(notifySpy).not.toHaveBeenCalledWith(expect.objectContaining({ type: 'pong' }))
      ;(secureClient as any).pingSentAt = Date.now() - 10
      ;(secureClient as any).handleMessage({
        data: JSON.stringify(createPong()),
      } as MessageEvent)
      expect(notifySpy).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'pong', rtt_ms: expect.any(Number) })
      )
      notifySpy.mockClear()
      ;(secureClient as any).handleMessage({
        data: JSON.stringify(createAuthRequired()),
      } as MessageEvent)
      expect(authSpy).toHaveBeenCalled()
      ;(secureClient as any).isAuthenticated = false
      ;(secureClient as any).handleMessage({
        data: JSON.stringify(createHeartbeat()),
      } as MessageEvent)
      expect(notifySpy).not.toHaveBeenCalledWith(expect.objectContaining({ type: 'heartbeat' }))
      ;(secureClient as any).isAuthenticated = true
      ;(secureClient as any).handleMessage({
        data: JSON.stringify(createHeartbeat()),
      } as MessageEvent)
      expect(notifySpy).toHaveBeenCalled()
      notifySpy.mockRestore()
      authSpy.mockRestore()
      secureClient.disconnect()
    })
  })
  describe('authentication handling (non-secure)', () => {
    it('notifies auth_required without authenticating', async () => {
      const { getWsToken } = await import('./auth')
      const handler = vi.fn()

      client.onMessage('auth_required', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.onmessage?.({ data: JSON.stringify(createAuthRequired()) })
      expect(handler).toHaveBeenCalled()
      expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    })
    it('ignores reauth_required when not secure', async () => {
      vi.mocked(getWsToken).mockClear()
      await (client as any).handleReauthRequired(
        createReauthRequired({ deadline: new Date(Date.now() + 10 * 1000).toISOString() })
      )
      expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    })
  })
  describe('auth failure callbacks', () => {
    it('skips auth logout callback when missing', () => {
      const originalCallback = (window as any).authLogoutCallback

      ;(window as any).authLogoutCallback = undefined
      expect(() => (client as any).handleAuthFailure(createAuthFailed())).not.toThrow()
      ;(window as any).authLogoutCallback = originalCallback
    })
    it('skips window callback when window is undefined', () => {
      const closeSpy = vi.spyOn(client as any, 'closeForAuthFailure')

      vi.stubGlobal('window', undefined)
      expect(() => (client as any).handleAuthFailure(createAuthFailed())).not.toThrow()
      expect(closeSpy).toHaveBeenCalled()
      vi.unstubAllGlobals()
      closeSpy.mockRestore()
    })
    it('invokes auth logout callback when defined', () => {
      const authLogoutCallback = vi.fn()

      ;(window as any).authLogoutCallback = authLogoutCallback
      ;(client as any).handleAuthFailure(createAuthFailed())
      expect(authLogoutCallback).toHaveBeenCalled()
      delete (window as any).authLogoutCallback
    })
  })
  describe('authentication guard branches', () => {
    it('returns early for performAuthentication guard conditions', async () => {
      vi.mocked(getWsToken).mockClear()
      await (client as any).performAuthentication('authenticate')
      expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
      const secureClient = new WebSocketClient({ secure: true })

      await (secureClient as any).performAuthentication('authenticate')
      expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
      ;(secureClient as any).ws = { readyState: WebSocket.OPEN, send: vi.fn(), close: vi.fn() }
      ;(secureClient as any).reauthInProgress = true
      await (secureClient as any).performAuthentication('authenticate')
      expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
      secureClient.disconnect()
    })
    it('handles scheduleReauthFromMs and clearReauthTimer guards', () => {
      ;(client as any).scheduleReauthFromMs(Date.now() + 60000)
      expect((client as any).reauthTimer).toBe(null)
      const secureClient = new WebSocketClient({ secure: true })

      ;(secureClient as any).scheduleReauthFromMs(Date.now() - 60000)
      expect((secureClient as any).reauthTimer).toBe(null)
      expect((secureClient as any).reauthScheduledAt).toBe(null)
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')

      ;(secureClient as any).reauthTimer = null
      ;(secureClient as any).clearReauthTimer()
      expect(clearTimeoutSpy).not.toHaveBeenCalled()
      clearTimeoutSpy.mockRestore()
      secureClient.disconnect()
    })
    it('performs authentication when guards pass', async () => {
      const secureClient = new WebSocketClient({ secure: true })
      const sendSpy = vi.fn()

      ;(secureClient as any).ws = { readyState: WebSocket.OPEN, send: sendSpy, close: vi.fn() }
      await (secureClient as any).performAuthentication('authenticate')
      expect(vi.mocked(getWsToken)).toHaveBeenCalled()
      expect(sendSpy).toHaveBeenCalled()
      secureClient.disconnect()
    })
    it('covers handleAuthRequired secure guard', async () => {
      const secureClient = new WebSocketClient({ secure: true })
      const performSpy = vi
        .spyOn(secureClient as any, 'performAuthentication')
        .mockResolvedValue(undefined)

      await (secureClient as any).handleAuthRequired(createAuthRequired())
      expect(performSpy).toHaveBeenCalled()
      const nonSecureClient = new WebSocketClient({ secure: false })
      const nonSecureSpy = vi.spyOn(nonSecureClient as any, 'performAuthentication')

      await (nonSecureClient as any).handleAuthRequired(createAuthRequired())
      expect(nonSecureSpy).not.toHaveBeenCalled()
      performSpy.mockRestore()
      secureClient.disconnect()
      nonSecureClient.disconnect()
    })
    it('schedules reauth when delay is finite and clears timers', () => {
      const secureClient = new WebSocketClient({ secure: true })

      ;(secureClient as any).scheduleReauthFromMs(Date.now() + 120000)
      expect((secureClient as any).reauthTimer).not.toBe(null)
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')

      ;(secureClient as any).clearReauthTimer()
      expect(clearTimeoutSpy).toHaveBeenCalled()
      clearTimeoutSpy.mockRestore()
      secureClient.disconnect()
    })
    it('skips reauth when scheduled before deadline', async () => {
      const secureClient = new WebSocketClient({ secure: true })
      const performSpy = vi
        .spyOn(secureClient as any, 'performAuthentication')
        .mockResolvedValue(undefined)
      const now = Date.now()

      ;(secureClient as any).reauthScheduledAt = now + 5000
      await (secureClient as any).handleReauthRequired(
        createReauthRequired({ deadline: new Date(now + 6000).toISOString() })
      )
      expect(performSpy).not.toHaveBeenCalled()
      ;(secureClient as any).reauthScheduledAt = null
      ;(secureClient as any).lastReauthDeadlineMs = null
      await (secureClient as any).handleReauthRequired(
        createReauthRequired({ deadline: new Date(now + 7000).toISOString() })
      )
      expect(performSpy).toHaveBeenCalled()
      performSpy.mockRestore()
      secureClient.disconnect()
    })
  })
  describe('reconnect guard branches', () => {
    it('skips reconnect when shouldReconnect is false', async () => {
      const connectSpy = vi.spyOn(client as any, 'connect')

      vi.mocked(shouldReconnect).mockReturnValueOnce(false)
      ;(client as any).scheduleReconnect()
      expect(connectSpy).not.toHaveBeenCalled()
      connectSpy.mockRestore()
    })
    it('does not reconnect when isReconnecting is false at callback time', async () => {
      const connectSpy = vi.spyOn(client as any, 'connect')

      vi.mocked(shouldReconnect).mockReturnValueOnce(true)
      ;(client as any).scheduleReconnect()
      ;(client as any).isReconnecting = false
      await vi.advanceTimersByTimeAsync(200)
      expect(connectSpy).not.toHaveBeenCalled()
      connectSpy.mockRestore()
    })
    it('reconnects when still marked reconnecting', async () => {
      const connectSpy = vi.spyOn(client as any, 'connect').mockImplementation(() => {})

      vi.mocked(shouldReconnect).mockReturnValueOnce(true)
      ;(client as any).scheduleReconnect()
      await vi.advanceTimersByTimeAsync(200)
      expect(connectSpy).toHaveBeenCalled()
      connectSpy.mockRestore()
    })
    it('clears heartbeat timer in handleClose', () => {
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

      vi.mocked(shouldReconnect).mockReturnValueOnce(false).mockReturnValueOnce(false)
      ;(client as any).heartbeatTimer = setInterval(() => {}, 1000)
      ;(client as any).handleClose()
      ;(client as any).heartbeatTimer = null
      ;(client as any).handleClose()
      expect(clearIntervalSpy).toHaveBeenCalledTimes(1)
      clearIntervalSpy.mockRestore()
    })
  })
  describe('reconnection', () => {
    it('skips reconnect when shouldReconnect is false', async () => {
      const { shouldReconnect } = await import('./reconnect')

      vi.mocked(shouldReconnect).mockReturnValueOnce(false)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.readyState = MockWebSocket.CLOSED
      mockWs.onclose?.(new CloseEvent('close'))
      expect((client as any).isReconnecting).toBe(false)
    })
    it('schedules reconnect on close', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.readyState = MockWebSocket.CLOSED
      mockWs.onclose?.(new CloseEvent('close'))
      expect((client as any).isReconnecting).toBe(true)
    })
    it('skips heartbeat cleanup when closing without heartbeat timer', async () => {
      vi.mocked(shouldReconnect).mockReturnValueOnce(false)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      ;(client as any).heartbeatTimer = null
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

      mockWs.onclose?.(new CloseEvent('close'))
      expect(clearIntervalSpy).not.toHaveBeenCalled()
      clearIntervalSpy.mockRestore()
    })
    it('resubscribes topics after reconnect', async () => {
      client.subscribe(['topic1', 'topic2'])
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      expect((client as any).ws.send).toHaveBeenCalledWith(
        expect.stringContaining('"type":"subscribe"')
      )
    })
    it('does not reconnect if isReconnecting is set to false', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.readyState = MockWebSocket.CLOSED
      mockWs.onclose?.(new CloseEvent('close'))
      ;(client as any).isReconnecting = false
      await vi.advanceTimersByTimeAsync(200)
      expect((client as any).ws).toBe(mockWs)
    })
    it('reconnects after delay', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const oldWs = (client as any).ws

      oldWs.readyState = MockWebSocket.CLOSED
      oldWs.onclose?.(new CloseEvent('close'))
      await vi.advanceTimersByTimeAsync(200)
      expect((client as any).ws).not.toBe(oldWs)
    })
    it('clears existing heartbeat timer on startHeartbeat', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const firstTimer = (client as any).heartbeatTimer

      ;(client as any).startHeartbeat()
      const secondTimer = (client as any).heartbeatTimer

      expect(secondTimer).not.toBe(firstTimer)
    })
  })
  describe('getConnectionStats', () => {
    it('returns connection statistics', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.subscribe(['topic1'])
      const stats = client.getConnectionStats()

      expect(stats).toMatchObject({
        connected: true,
        reconnectAttempts: 0,
        subscriptions: ['topic1'],
        isReconnecting: false,
      })
    })
  })
  describe('getAvailableTopics', () => {
    it('rejects when not connected', async () => {
      await expect(client.getAvailableTopics()).rejects.toThrow('WebSocket not connected')
    })
    it('covers connected and disconnected branches', async () => {
      await expect(client.getAvailableTopics()).rejects.toThrow('WebSocket not connected')
      const isConnectedSpy = vi.spyOn(client, 'isConnected').mockReturnValue(true)
      const promise = client.getAvailableTopics()
      const handler = (client as any).messageHandlers.get('subscriptions_list')?.[0]

      handler?.({
        type: 'subscriptions_list',
        available_topics: ['topicX'],
        subscriptions: [],
        total_available: 1,
      })
      await expect(promise).resolves.toEqual(['topicX'])
      isConnectedSpy.mockRestore()
    })
    it('requests and returns available topics', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const promise = client.getAvailableTopics()
      const mockWs = (client as any).ws

      await vi.advanceTimersByTimeAsync(10)
      mockWs.onmessage?.({
        data: JSON.stringify(
          createSubscriptionsList({ available_topics: ['topic1', 'topic2'], total_available: 2 })
        ),
      })
      const topics = await promise

      expect(topics).toEqual(['topic1', 'topic2'])
    })
    it('returns empty list when available_topics is missing', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const promise = client.getAvailableTopics()
      const handler = (client as any).messageHandlers.get('subscriptions_list')?.[0]

      expect(handler).toBeDefined()
      handler?.({ type: 'subscriptions_list', subscriptions: [], total_available: 0 })
      const topics = await promise

      expect(topics).toEqual([])
    })
    it('resolves when forced connected', async () => {
      const isConnectedSpy = vi.spyOn(client, 'isConnected').mockReturnValue(true)
      const promise = client.getAvailableTopics()
      const handler = (client as any).messageHandlers.get('subscriptions_list')?.[0]

      handler?.({
        type: 'subscriptions_list',
        subscriptions: [],
        available_topics: ['topicA'],
        total_available: 1,
      })
      const topics = await promise

      expect(topics).toEqual(['topicA'])
      isConnectedSpy.mockRestore()
    })
    it('times out after 5 seconds', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const promise = client.getAvailableTopics()
      const [, result] = await Promise.all([
        vi.advanceTimersByTimeAsync(6000),
        promise.catch(e => e),
      ])

      expect(result).toBeInstanceOf(Error)
      expect((result as Error).message).toBe('Timeout waiting for topics response')
    })
  })
  describe('handler errors', () => {
    it('handles errors in message handlers gracefully', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      const errorHandler = vi.fn(() => {
        throw new Error('Handler error')
      })

      client.onMessage('heartbeat', errorHandler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      expect(() => {
        mockWs.onmessage?.({ data: JSON.stringify(createHeartbeat()) })
      }).not.toThrow()
      expect(consoleSpy).toHaveBeenCalledWith('Error in message handler:', expect.any(Error))
      consoleSpy.mockRestore()
    })
    it('handles errors in connection handlers gracefully', async () => {
      const errorHandler = vi.fn(() => {
        throw new Error('Handler error')
      })

      client.onConnection(errorHandler)
      client.connect()
      expect(() => vi.advanceTimersByTimeAsync(50)).not.toThrow()
    })
  })
  describe('throttling callback', () => {
    it('flushes throttled messages via callback', async () => {
      const { flushThrottledMessages } = await import('./reconnect')
      const handler = vi.fn()

      client.onMessage('*', handler)
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const calls = vi.mocked(flushThrottledMessages).mock.calls

      if (calls.length > 0) {
        const lastCall = calls[calls.length - 1]
        const callback = lastCall[3] as (msg: unknown) => void
        const tickMsg = createTick()

        callback(tickMsg)
        expect(handler).toHaveBeenCalledWith(tickMsg)
      }
    })
  })
  describe('heartbeat', () => {
    it('sends heartbeat when connected', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      const mockWs = (client as any).ws

      mockWs.send.mockClear()
      await vi.advanceTimersByTimeAsync(6000)
      expect(mockWs.send).toHaveBeenCalled()
    })
    it('sets pingSentAt when sending heartbeat', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      ;(client as any).pingSentAt = null
      await vi.advanceTimersByTimeAsync(6000)
      expect((client as any).pingSentAt).toBeTypeOf('number')
    })
    it('does not send heartbeat when not connected', async () => {
      ;(client as any).startHeartbeat()
      ;(client as any).ws = null
      const sendSpy = vi.spyOn(client, 'send')

      await vi.advanceTimersByTimeAsync(6000)
      expect(sendSpy).not.toHaveBeenCalled()
      sendSpy.mockRestore()
    })
    it('handles heartbeat timer replacement and connection checks', async () => {
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')
      const sendSpy = vi.spyOn(client, 'send')
      const isConnectedSpy = vi
        .spyOn(client, 'isConnected')
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(false)
        .mockReturnValue(false)

      ;(client as any).heartbeatTimer = null
      ;(client as any).startHeartbeat()
      const firstTimer = (client as any).heartbeatTimer

      ;(client as any).heartbeatTimer = setInterval(() => {}, 1000)
      ;(client as any).startHeartbeat()
      await vi.advanceTimersByTimeAsync(6000)
      await vi.advanceTimersByTimeAsync(6000)
      expect(clearIntervalSpy).toHaveBeenCalled()
      expect(sendSpy).toHaveBeenCalled()
      expect((client as any).heartbeatTimer).not.toBe(firstTimer)
      isConnectedSpy.mockRestore()
      sendSpy.mockRestore()
      clearIntervalSpy.mockRestore()
    })
  })
})
describe('WebSocketClient secure mode', () => {
  let client: WebSocketClient
  let originalWebSocket: typeof WebSocket

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    originalWebSocket = globalThis.WebSocket
    globalThis.WebSocket = MockWebSocket as unknown as typeof WebSocket
    client = new WebSocketClient({ secure: true })
  })
  afterEach(() => {
    client.disconnect()
    globalThis.WebSocket = originalWebSocket
    vi.useRealTimers()
  })
  it('does not mark as ready until authenticated', async () => {
    const connectionHandler = vi.fn()

    client.onConnection(connectionHandler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    expect(connectionHandler).not.toHaveBeenCalled()
  })
  it('does not call onConnectionReady on open when secure', async () => {
    const readySpy = vi.spyOn(client as any, 'onConnectionReady')

    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    expect(readySpy).not.toHaveBeenCalled()
    readySpy.mockRestore()
  })
  it('ignores non-auth messages before authentication completes', async () => {
    const handler = vi.fn()

    client.onMessage('candle', handler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({ data: JSON.stringify(createCandle()) })
    expect(handler).not.toHaveBeenCalled()
  })
  it('handles auth_required message', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(mockWs.send).toHaveBeenCalledWith(expect.stringContaining('"type":"authenticate"'))
  })
  it('handles auth_complete message', async () => {
    const connectionHandler = vi.fn()

    client.onConnection(connectionHandler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthComplete({ session_expires_at: '2024-01-01T00:00:00Z' })),
    })
    expect(connectionHandler).toHaveBeenCalledWith(true)
    expect((client as any).sessionExpiresAt).toBe('2024-01-01T00:00:00Z')
  })
  it('handles auth_complete with null session expiry', () => {
    const readySpy = vi.spyOn(client as any, 'onConnectionReady')
    const message = createAuthComplete()

    ;(message as { session_expires_at: string | null }).session_expires_at = null
    ;(client as any).handleAuthComplete(message)
    expect((client as any).sessionExpiresAt).toBe(null)
    expect(readySpy).toHaveBeenCalled()
    readySpy.mockRestore()
  })
  it('handles auth_failed message', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthFailed({ reason: 'Invalid token' })),
    })
    expect((client as any).isAuthenticated).toBe(false)
    expect((client as any).reconnectAttempts).toBeGreaterThanOrEqual(10)
  })
  it('handles auth_ok with expiration', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk()),
    })
    expect((client as any).reauthTimer).not.toBe(null)
  })
  it('handles auth_required via handleAuthMessage', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const performSpy = vi
      .spyOn(secureClient as any, 'performAuthentication')
      .mockResolvedValue(undefined)

    await (secureClient as any).handleAuthMessage(createAuthRequired())
    expect(performSpy).toHaveBeenCalledWith('authenticate')
  })
  it('skips auth_required via handleAuthMessage for non-secure clients', async () => {
    const nonSecureClient = new WebSocketClient({ secure: false })
    const performSpy = vi.spyOn(nonSecureClient as any, 'performAuthentication')

    await (nonSecureClient as any).handleAuthMessage(createAuthRequired())
    expect(performSpy).not.toHaveBeenCalled()
  })
  it('handles auth_ok via handleAuthMessage', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const scheduleSpy = vi.spyOn(secureClient as any, 'scheduleReauthFromMs')
    const expDate = new Date(Date.now() + 3600000)
    const expIso = expDate.toISOString()
    const expMs = expDate.getTime()

    await (secureClient as any).handleAuthMessage({ type: 'auth_ok', exp: expIso })
    expect(scheduleSpy).toHaveBeenCalledWith(expMs)
    expect((secureClient as any).pendingWsTokenExp).toBe(null)
  })
  it('handles auth_expired via handleAuthMessage', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const clearSpy = vi.spyOn(secureClient as any, 'clearReauthTimer')

    ;(secureClient as any).isAuthenticated = true
    ;(secureClient as any).sessionExpiresAt = '2024-01-01T00:00:00Z'
    ;(secureClient as any).reauthTimer = setTimeout(() => {}, 1000)
    await (secureClient as any).handleAuthMessage({ type: 'auth_expired' })
    expect(clearSpy).toHaveBeenCalled()
    expect((secureClient as any).isAuthenticated).toBe(false)
    expect((secureClient as any).sessionExpiresAt).toBe(null)
  })
  it('handles auth_success via handleAuthMessage', async () => {
    const notifySpy = vi.spyOn(client as any, 'notifyHandlers')

    await (client as any).handleAuthMessage({ type: 'auth_success' })
    expect(notifySpy).toHaveBeenCalledWith({ type: 'auth_success' })
  })
  it('logs unexpected auth messages via handleAuthMessage', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const notifySpy = vi.spyOn(client as any, 'notifyHandlers')
    const message = { type: 'auth_unexpected' }

    await (client as any).handleAuthMessage(message)
    expect(warnSpy).toHaveBeenCalledWith('Unexpected authentication message:', message)
    expect(notifySpy).toHaveBeenCalledWith(message)
    warnSpy.mockRestore()
  })
  it('handles auth_required directly for secure clients', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const performSpy = vi
      .spyOn(secureClient as any, 'performAuthentication')
      .mockResolvedValue(undefined)

    await (secureClient as any).handleAuthRequired(createAuthRequired())
    expect(performSpy).toHaveBeenCalledWith('authenticate')
  })
  it('skips auth_required directly for non-secure clients', async () => {
    const nonSecureClient = new WebSocketClient({ secure: false })
    const performSpy = vi.spyOn(nonSecureClient as any, 'performAuthentication')

    await (nonSecureClient as any).handleAuthRequired(createAuthRequired())
    expect(performSpy).not.toHaveBeenCalled()
  })
  it('falls back to pending exp when auth_ok omits exp', () => {
    const pendingExpSeconds = Math.floor(Date.now() / 1000) + 3600
    const pendingExpMs = pendingExpSeconds * 1000

    ;(client as any).pendingWsTokenExp = pendingExpSeconds
    const scheduleSpy = vi.spyOn(client as any, 'scheduleReauthFromMs')

    ;(client as any).handleAuthOk({ type: 'auth_ok' } as unknown as Record<string, unknown>)
    expect(scheduleSpy).toHaveBeenCalledWith(pendingExpMs)
    expect((client as any).pendingWsTokenExp).toBe(null)
  })
  it('skips schedule when auth_ok has no exp or pending value', () => {
    const scheduleSpy = vi.spyOn(client as any, 'scheduleReauthFromMs')

    ;(client as any).pendingWsTokenExp = null
    ;(client as any).handleAuthOk({ type: 'auth_ok' } as unknown as Record<string, unknown>)
    expect(scheduleSpy).not.toHaveBeenCalled()
  })
  it('handles reauth_required message', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthComplete()),
    })
    mockWs.send.mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(mockWs.send).toHaveBeenCalledWith(expect.stringContaining('"type":"reauth"'))
  })
  it('handles reauth_ok message', async () => {
    const handler = vi.fn()

    client.onMessage('reauth_ok', handler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createReauthOk()),
    })
    expect(handler).toHaveBeenCalled()
  })
  it('handles reauth_ok when exp is missing', () => {
    const scheduleSpy = vi.spyOn(client as any, 'scheduleReauthFromMs')
    const pendingExpSeconds = Math.floor(Date.now() / 1000) + 3600
    const pendingExpMs = pendingExpSeconds * 1000

    ;(client as any).pendingWsTokenExp = pendingExpSeconds
    ;(client as any).handleReauthOk({ type: 'reauth_ok' } as Record<string, unknown>)
    expect(scheduleSpy).toHaveBeenCalledWith(pendingExpMs)
    scheduleSpy.mockClear()
    ;(client as any).pendingWsTokenExp = null
    ;(client as any).handleReauthOk({ type: 'reauth_ok' } as Record<string, unknown>)
    expect(scheduleSpy).not.toHaveBeenCalled()
    scheduleSpy.mockRestore()
  })
  it('handles auth_expired message', async () => {
    const handler = vi.fn()

    client.onMessage('auth_expired', handler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthComplete()),
    })
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthExpired()),
    })
    expect(handler).toHaveBeenCalled()
    expect((client as any).isAuthenticated).toBe(false)
  })
  it('ignores messages before authentication', async () => {
    const handler = vi.fn()

    client.onMessage('candle', handler)
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createCandle()),
    })
    expect(handler).not.toHaveBeenCalled()
  })
  it('ignores duplicate reauth_required with same deadline', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthComplete()),
    })
    mockWs.send.mockClear()
    const deadline = new Date(Date.now() + 60 * 1000).toISOString()

    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired({ deadline })),
    })
    await vi.advanceTimersByTimeAsync(100)
    const firstCallCount = mockWs.send.mock.calls.length

    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired({ deadline })),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(mockWs.send.mock.calls.length).toBe(firstCallCount)
  })
  it('skips reauth_required when scheduled reauth is before deadline', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken).mockResolvedValueOnce({
      token: 'test-token',
      exp: Math.floor(Date.now() / 1000) + 3600,
    })
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk({ exp: new Date(Date.now() + 3600 * 1000).toISOString() })),
    })
    const initialCalls = vi.mocked(getWsToken).mock.calls.length
    const deadline = new Date(Date.now() + 7200 * 1000).toISOString()

    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired({ deadline })),
    })
    await vi.advanceTimersByTimeAsync(10)
    expect(vi.mocked(getWsToken).mock.calls.length).toBe(initialCalls)
    secureClient.disconnect()
  })
  it('returns early when scheduled reauth is already pending', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const performSpy = vi.spyOn(secureClient as any, 'performAuthentication')
    const now = 1700000000000
    const dateSpy = vi.spyOn(Date, 'now').mockReturnValue(now)

    ;(secureClient as any).reauthScheduledAt = now + 1000
    ;(secureClient as any).lastReauthDeadlineMs = null
    await (secureClient as any).handleReauthRequired({
      type: 'reauth_required',
      deadline: new Date(now + 5000).toISOString(),
    })
    dateSpy.mockRestore()
    expect(performSpy).not.toHaveBeenCalled()
  })
  it('skips clearing timer when no reauth timer is set', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const performSpy = vi.spyOn(secureClient as any, 'performAuthentication')

    ;(secureClient as any).reauthTimer = null
    ;(secureClient as any).reauthScheduledAt = null
    ;(secureClient as any).lastReauthDeadlineMs = null
    await (secureClient as any).handleReauthRequired({
      type: 'reauth_required',
      deadline: new Date(Date.now() + 30 * 1000).toISOString(),
    })
    expect(performSpy).toHaveBeenCalled()
  })
  it('clears existing reauth timer before reauth', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const clearSpy = vi.spyOn(secureClient as any, 'clearReauthTimer')
    const performSpy = vi
      .spyOn(secureClient as any, 'performAuthentication')
      .mockResolvedValue(undefined)

    ;(secureClient as any).reauthTimer = setTimeout(() => {}, 1000)
    ;(secureClient as any).reauthScheduledAt = null
    ;(secureClient as any).lastReauthDeadlineMs = null
    await (secureClient as any).handleReauthRequired({
      type: 'reauth_required',
      deadline: new Date(Date.now() + 60 * 1000).toISOString(),
    })
    expect(clearSpy).toHaveBeenCalled()
    expect(performSpy).toHaveBeenCalledWith('reauth')
  })
  it('handles reauth_required with default deadline', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthComplete()),
    })
    mockWs.send.mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(mockWs.send).toHaveBeenCalledWith(expect.stringContaining('"type":"reauth"'))
  })
  it('resets deadline tracking when deadline is invalid', async () => {
    const secureClient = new WebSocketClient({ secure: true })
    const performSpy = vi
      .spyOn(secureClient as any, 'performAuthentication')
      .mockResolvedValue(undefined)

    ;(secureClient as any).lastReauthDeadlineMs = Date.now()
    await (secureClient as any).handleReauthRequired({
      type: 'reauth_required',
      deadline: Number.NaN,
    })
    expect((secureClient as any).lastReauthDeadlineMs).toBe(null)
    expect(performSpy).toHaveBeenCalledWith('reauth')
    secureClient.disconnect()
  })
  it('blocks auth_ok without required exp field', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify({
        type: 'auth_ok',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
      }),
    })
    expect((client as any).reauthTimer).toBe(null)
  })
  it('handles scheduleReauthFromMs with past expiration (negative delay)', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws
    const pastExpiration = new Date(Date.now() - 60000).toISOString()

    mockWs.onmessage?.({
      data: JSON.stringify({
        type: 'auth_ok',
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        exp: pastExpiration,
      }),
    })
    expect((client as any).reauthScheduledAt).toBe(null)
  })
  it('handles closeForAuthFailure when WS is not open', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.readyState = MockWebSocket.CLOSED
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthFailed({ reason: 'Test' })),
    })
    expect((client as any).reconnectAttempts).toBeGreaterThanOrEqual(10)
  })
  it('triggers window authLogoutCallback on auth failure', async () => {
    const authLogoutCallback = vi.fn()

    ;(window as any).authLogoutCallback = authLogoutCallback
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthFailed({ reason: 'Invalid' })),
    })
    expect(authLogoutCallback).toHaveBeenCalled()
    delete (window as any).authLogoutCallback
  })
  it('handles getWsToken failure during initial authentication', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken).mockRejectedValueOnce(new Error('Token fetch failed'))
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(consoleSpy).toHaveBeenCalledWith(
      'Failed to perform WebSocket authentication:',
      expect.any(Error)
    )
    expect((secureClient as any).reconnectAttempts).toBeGreaterThanOrEqual(10)
    consoleSpy.mockRestore()
    secureClient.disconnect()
  })
  it('handles getWsToken failure during reauth', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken)
      .mockResolvedValueOnce({ token: 'test-token', exp: Date.now() / 1000 + 3600 })
      .mockRejectedValueOnce(new Error('Reauth token fetch failed'))
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(consoleSpy).toHaveBeenCalledWith(
      'Failed to perform WebSocket authentication:',
      expect.any(Error)
    )
    expect((secureClient as any).reconnectAttempts).toBeLessThan(10)
    consoleSpy.mockRestore()
    secureClient.disconnect()
  })
  it('handles non-Error exception during getWsToken', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken).mockRejectedValueOnce('string error')
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(consoleSpy).toHaveBeenCalledWith(
      'Failed to perform WebSocket authentication:',
      'string error'
    )
    consoleSpy.mockRestore()
    secureClient.disconnect()
  })
  it('executes reauth timer callback', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken)
      .mockResolvedValueOnce({ token: 'test-token', exp: Date.now() / 1000 + 50 })
      .mockResolvedValueOnce({ token: 'reauth-token', exp: Date.now() / 1000 + 3600 })
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk({ exp: new Date(Date.now() + 50000).toISOString() })),
    })
    await vi.advanceTimersByTimeAsync(10000)
    expect(vi.mocked(getWsToken)).toHaveBeenCalledTimes(2)
    expect(mockWs.send).toHaveBeenCalledWith(expect.stringContaining('"type":"reauth"'))
    secureClient.disconnect()
  })
  it('handles auth_error alias for auth_failed', async () => {
    client.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (client as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthFailed({ reason: 'Server error' })),
    })
    expect((client as any).isAuthenticated).toBe(false)
  })
  it('skips reauth when existing schedule is within deadline', async () => {
    ;(client as any).reauthScheduledAt = Date.now() + 5000
    const performSpy = vi.spyOn(client as any, 'performAuthentication').mockResolvedValue(undefined)

    await (client as any).handleReauthRequired(
      createReauthRequired({ deadline: new Date(Date.now() + 10 * 1000).toISOString() })
    )
    expect(performSpy).not.toHaveBeenCalled()
    performSpy.mockRestore()
  })
  it('skips authentication for non-secure client', async () => {
    const { getWsToken } = await import('./auth')

    vi.mocked(getWsToken).mockClear()
    const nonSecureClient = new WebSocketClient({ secure: false })

    nonSecureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    expect((nonSecureClient as any).isAuthenticated).toBe(true)
    expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    nonSecureClient.disconnect()
  })
  it('skips performAuthentication when client is not secure', async () => {
    const { getWsToken } = await import('./auth')

    vi.mocked(getWsToken).mockClear()
    const nonSecureClient = new WebSocketClient({ secure: false })

    await (nonSecureClient as any).performAuthentication('authenticate')
    expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    nonSecureClient.disconnect()
  })
  it('skips authentication when websocket is not open', async () => {
    const { getWsToken } = await import('./auth')

    vi.mocked(getWsToken).mockClear()
    const secureClient = new WebSocketClient({ secure: true })

    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.readyState = MockWebSocket.CLOSED
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    secureClient.disconnect()
  })
  it('skips authentication when reauthInProgress is true', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    let resolveAuth: (value: { token: string; exp: number }) => void = () => {}

    vi.mocked(getWsToken).mockImplementationOnce(
      () =>
        new Promise(resolve => {
          resolveAuth = resolve
        })
    )
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)
    expect((secureClient as any).reauthInProgress).toBe(true)
    vi.mocked(getWsToken).mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)
    expect(vi.mocked(getWsToken)).not.toHaveBeenCalled()
    resolveAuth({ token: 'test-token', exp: Date.now() / 1000 + 3600 })
    await vi.advanceTimersByTimeAsync(10)
    secureClient.disconnect()
  })
  it('ignores reauth_required for non-secure client', async () => {
    const nonSecureClient = new WebSocketClient({ secure: false })

    nonSecureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (nonSecureClient as any).ws

    mockWs.send.mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired()),
    })
    await vi.advanceTimersByTimeAsync(100)
    const reauthCalls = mockWs.send.mock.calls.filter((call: string[]) =>
      call[0].includes('reauth')
    )

    expect(reauthCalls.length).toBe(0)
    nonSecureClient.disconnect()
  })
  it('handles reauth_required when no timer is scheduled', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken).mockResolvedValue({
      token: 'test-token',
      exp: Date.now() / 1000 + 3600,
    })
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk()),
    })
    await vi.advanceTimersByTimeAsync(0)
    ;(secureClient as any).clearReauthTimer()
    expect((secureClient as any).reauthTimer).toBe(null)
    expect((secureClient as any).reauthScheduledAt).toBe(null)
    ;(secureClient as any).lastReauthDeadlineMs = null
    vi.mocked(getWsToken).mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(createReauthRequired()),
    })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(100)
    expect(vi.mocked(getWsToken)).toHaveBeenCalled()
    secureClient.disconnect()
  })
  it('clears scheduled reauth timer on immediate reauth_required', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken)
      .mockResolvedValueOnce({
        token: 'test-token',
        exp: Math.floor(Date.now() / 1000) + 3600,
      })
      .mockResolvedValueOnce({
        token: 'reauth-token',
        exp: Math.floor(Date.now() / 1000) + 3600,
      })
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk()),
    })
    expect((secureClient as any).reauthTimer).not.toBe(null)
    vi.mocked(getWsToken).mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(
        createReauthRequired({ deadline: new Date(Date.now() + 5 * 1000).toISOString() })
      ),
    })
    await vi.advanceTimersByTimeAsync(50)
    expect((secureClient as any).reauthTimer).toBe(null)
    expect(vi.mocked(getWsToken)).toHaveBeenCalled()
    secureClient.disconnect()
  })
  it('clears existing reauth timer when new reauth_required arrives with earlier deadline', async () => {
    const { getWsToken } = await import('./auth')
    const secureClient = new WebSocketClient({ secure: true })

    vi.mocked(getWsToken).mockResolvedValue({
      token: 'test-token',
      exp: Date.now() / 1000 + 3600,
    })
    secureClient.connect()
    await vi.advanceTimersByTimeAsync(50)
    const mockWs = (secureClient as any).ws

    mockWs.onmessage?.({
      data: JSON.stringify(createAuthRequired()),
    })
    await vi.advanceTimersByTimeAsync(10)
    mockWs.onmessage?.({
      data: JSON.stringify(createAuthOk()),
    })
    expect((secureClient as any).reauthTimer).not.toBe(null)
    vi.mocked(getWsToken).mockClear()
    mockWs.send.mockClear()
    mockWs.onmessage?.({
      data: JSON.stringify(
        createReauthRequired({ deadline: new Date(Date.now() + 10 * 1000).toISOString() })
      ),
    })
    await vi.advanceTimersByTimeAsync(100)
    expect(vi.mocked(getWsToken)).toHaveBeenCalled()
    secureClient.disconnect()
  })
  it('flushes throttled messages via throttle callback', async () => {
    vi.useFakeTimers()
    const reconnectModule = await import('./reconnect')
    const flushSpy = vi.mocked(reconnectModule.flushThrottledMessages)
    let capturedCallback: ((msg: unknown) => void) | undefined

    flushSpy.mockImplementation(
      (
        _pending: Map<string, unknown>,
        _lastTime: Map<string, number>,
        _interval: number,
        callback: (msg: unknown) => void
      ) => {
        capturedCallback = callback
      }
    )
    const localClient = new WebSocketClient()

    vi.advanceTimersByTime(250)
    expect(capturedCallback).toBeDefined()
    capturedCallback?.({ type: 'test' })
    localClient.disconnect()
    vi.useRealTimers()
  })
  it('handles WebSocket constructor error in connect', () => {
    const originalWebSocket = globalThis.WebSocket
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    vi.mocked(shouldReconnect).mockReturnValue(true)
    const ThrowingWebSocket = Object.assign(
      function () {
        throw new Error('Connection refused')
      },
      { CONNECTING: 0, OPEN: 1, CLOSING: 2, CLOSED: 3 }
    ) as unknown as typeof WebSocket

    globalThis.WebSocket = ThrowingWebSocket
    const localClient = new WebSocketClient()

    localClient.connect()
    expect(consoleSpy).toHaveBeenCalledWith('WebSocket connection error:', expect.any(Error))
    globalThis.WebSocket = originalWebSocket
    consoleSpy.mockRestore()
    localClient.disconnect()
  })
  it('sends unsubscribe message when connected', async () => {
    const localClient = new WebSocketClient()

    localClient.connect()
    const mockWs = (localClient as any).ws

    mockWs.readyState = WebSocket.OPEN
    localClient.subscribe(['topic1'])
    localClient.unsubscribe(['topic1'])
    await Promise.resolve()
    const sent = mockWs.send.mock.calls.find((c: string[]) => c[0].includes('"unsubscribe"'))

    expect(sent).toBeDefined()
    const parsed = JSON.parse((sent as string[])[0])

    expect(parsed.type).toBe('unsubscribe')
    expect(parsed.topics).toEqual(['topic1'])
    localClient.disconnect()
  })
  describe('provenance stamping', () => {
    it('stamps subscribe messages with provenance fields', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.subscribe(['test.topic'])
      await Promise.resolve()
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('subscribe')
      expect(parsed.public_id).toBeDefined()
      expect(parsed.session_id).toBe('test-session-id')
      expect(parsed.sequence_id).toEqual(expect.any(Number))
    })
    it('stamps ping messages as telemetry', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send({ type: 'ping' })
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('ping')
      expect(parsed.public_id).toBeDefined()
      expect(parsed.session_id).toBe('test-session-id')
      expect(parsed.sequence_id).toEqual(expect.any(Number))
    })
    it('stamps authenticate messages as control', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send({ type: 'authenticate', ws_token: 'tok' })
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('authenticate')
      expect(parsed.public_id).toBeDefined()
      expect(parsed.session_id).toBe('test-session-id')
    })
    it('does not stamp unknown message types', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send({ type: 'custom_event', data: 'value' })
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.type).toBe('custom_event')
      expect(parsed.public_id).toBeUndefined()
      expect(parsed.session_id).toBeUndefined()
      expect(parsed.sequence_id).toBeUndefined()
    })
    it('does not stamp string messages', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send('raw string')
      expect((client as any).ws.send).toHaveBeenCalledWith('raw string')
    })
    it('does not stamp messages without type field', async () => {
      client.connect()
      await vi.advanceTimersByTimeAsync(50)
      client.send({ data: 'no-type' })
      const sent = (client as any).ws.send.mock.calls[0][0]
      const parsed = JSON.parse(sent)

      expect(parsed.public_id).toBeUndefined()
      expect(parsed.session_id).toBeUndefined()
    })
  })
})
