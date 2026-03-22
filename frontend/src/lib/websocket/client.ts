import type {
  WebSocketMessages,
  PongWithRtt,
  WSAuthRequiredResponse,
  WSAuthOkResponse,
  WSAuthCompleteResponse,
  WSAuthFailedResponse,
  WSReauthRequiredResponse,
  WSReauthOkResponse,
} from '../../types/ws'
import { parseWsMessage } from '../schemas/ws'
import {
  WebSocketClientOptions,
  MessageHandler,
  ConnectionHandler,
  TypedMessageHandler,
  WebSocketMessageType,
} from './types'
import { v7 as uuid7 } from 'uuid'
import {
  getMessageTopic,
  shouldThrottle,
  buildMarketTopic,
  MARKET_TOPIC_PREFIX,
  ORDERS_COMMANDS_PREFIX,
  ORDERS_EVENTS_PREFIX,
  SIGNALS_TOPIC_PREFIX,
  HEARTBEATS_TOPIC_PREFIX,
  getSubscriptionTopics,
} from './topics'
import {
  calculateReconnectDelay,
  shouldReconnect,
  createHeartbeatMessage,
  flushThrottledMessages,
  buildWebSocketUrl,
} from './reconnect'
import { getWsToken, isAuthControlMessage } from './auth'
import { getTracker } from '../sequenceTracker'

const TELEMETRY_TYPES = new Set(['ping'])
const CONTROL_TYPES = new Set([
  'authenticate',
  'reauth',
  'subscribe',
  'unsubscribe',
  'get_subscriptions',
])

class WebSocketClient {
  private ws: WebSocket | null = null
  private readonly url: string
  private readonly reconnectInterval: number
  private readonly maxReconnectAttempts: number
  private readonly heartbeatInterval: number
  private readonly throttleInterval: number
  private readonly secure: boolean
  private reconnectAttempts = 0
  private isReconnecting = false
  private intentionalDisconnect = false
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private throttleTimer: ReturnType<typeof setInterval> | null = null
  private isAuthenticated = false
  private sessionExpiresAt: string | null = null
  private pendingWsTokenExp: number | null = null
  private reauthTimer: ReturnType<typeof setTimeout> | null = null
  private reauthScheduledAt: number | null = null
  private lastReauthDeadlineMs: number | null = null
  private reauthInProgress = false
  private readonly reauthLeadTimeMs = 45000
  private readonly messageHandlers = new Map<string, MessageHandler[]>()
  private readonly connectionHandlers: ConnectionHandler[] = []
  private readonly subscribedTopics = new Set<string>()
  private readonly pendingMessages = new Map<string, WebSocketMessages>()
  private readonly lastMessageTime = new Map<string, number>()
  private pingSentAt: number | null = null
  private readonly pendingSubscribes = new Set<string>()
  private readonly pendingUnsubscribes = new Set<string>()
  private subscriptionFlushScheduled = false
  constructor(options: WebSocketClientOptions = {}) {
    this.secure = options.secure || false
    this.url = options.url || buildWebSocketUrl()
    this.reconnectInterval = options.reconnectInterval || 3000
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10
    this.heartbeatInterval = options.heartbeatInterval || 5000
    this.throttleInterval = options.throttleInterval || 200
    this.setupThrottling()
  }
  private setupThrottling(): void {
    this.throttleTimer = setInterval(() => {
      flushThrottledMessages(
        this.pendingMessages,
        this.lastMessageTime,
        this.throttleInterval,
        message => this.notifyHandlers(message)
      )
    }, this.throttleInterval)
  }
  connect(): void {
    if (this.ws?.readyState === WebSocket.CONNECTING || this.ws?.readyState === WebSocket.OPEN) {
      return
    }

    if (!this.throttleTimer) {
      this.setupThrottling()
    }

    this.intentionalDisconnect = false
    this.sessionExpiresAt = null
    this.pendingWsTokenExp = null
    this.clearReauthTimer()
    this.lastReauthDeadlineMs = null

    try {
      this.ws = new WebSocket(this.url)
      this.ws.onopen = this.handleOpen.bind(this)
      this.ws.onmessage = this.handleMessage.bind(this)
      this.ws.onclose = this.handleClose.bind(this)
      this.ws.onerror = this.handleError.bind(this)
    } catch (error) {
      console.error('WebSocket connection error:', error)
      this.scheduleReconnect()
    }
  }
  disconnect(): void {
    this.intentionalDisconnect = true
    this.isReconnecting = false
    this.reconnectAttempts = 0
    this.isAuthenticated = false
    this.sessionExpiresAt = null
    this.pendingWsTokenExp = null
    this.clearReauthTimer()
    this.lastReauthDeadlineMs = null
    this.pendingMessages.clear()
    this.lastMessageTime.clear()

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }

    if (this.throttleTimer) {
      clearInterval(this.throttleTimer)
      this.throttleTimer = null
    }

    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
  }
  private handleOpen(): void {
    this.reconnectAttempts = 0
    this.isReconnecting = false
    this.isAuthenticated = false

    if (!this.secure) {
      this.onConnectionReady()
    }
  }
  private onConnectionReady(): void {
    this.isAuthenticated = true
    this.resubscribeTopics()
    this.notifyConnectionHandlers(true)
    this.startHeartbeat()
  }
  private handleMessage(event: MessageEvent): void {
    try {
      const parsed = JSON.parse(event.data)

      if (!parsed || typeof parsed !== 'object' || !('type' in parsed)) {
        console.warn('Received malformed WebSocket message (missing type):', parsed)

        return
      }

      const validated = parseWsMessage(parsed)

      if (!validated) {
        console.warn('Received invalid WebSocket message structure:', parsed)

        return
      }

      const message = validated

      if (isAuthControlMessage(message)) {
        void this.handleAuthMessage(message)

        return
      }

      if (message.type === 'pong') {
        if (this.pingSentAt !== null) {
          const rtt = Date.now() - this.pingSentAt

          this.pingSentAt = null
          const pongWithRtt: PongWithRtt = { ...message, rtt_ms: rtt }

          this.notifyHandlers(pongWithRtt)
        }

        return
      }

      if (this.secure && !this.isAuthenticated) {
        console.debug('Ignoring message received before authentication is complete:', message.type)

        return
      }

      const topic = getMessageTopic(message)

      if (topic && shouldThrottle(message.type)) {
        this.pendingMessages.set(topic, message)
      } else {
        this.notifyHandlers(message)
      }
    } catch (error) {
      console.error('Failed to parse WebSocket message:', error)
    }
  }
  private handleClose(): void {
    this.isAuthenticated = false
    this.sessionExpiresAt = null
    this.pendingWsTokenExp = null
    this.clearReauthTimer()
    this.lastReauthDeadlineMs = null
    this.notifyConnectionHandlers(false)

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }

    const wasIntentional = this.intentionalDisconnect

    this.intentionalDisconnect = false

    if (!wasIntentional) {
      this.scheduleReconnect()
    }
  }
  private handleError(error: Event): void {
    console.error('WebSocket error:', error)
  }
  private async handleAuthMessage(message: WebSocketMessages): Promise<void> {
    switch (message.type) {
      case 'auth_required':
        this.notifyHandlers(message)
        await this.handleAuthRequired(message)
        break
      case 'auth_ok':
        this.handleAuthOk(message)
        break
      case 'auth_complete':
        this.handleAuthComplete(message)
        break
      case 'auth_failed':
        this.handleAuthFailure(message)
        break
      case 'reauth_required':
        this.notifyHandlers(message)
        await this.handleReauthRequired(message)
        break
      case 'reauth_ok':
        this.handleReauthOk(message)
        break
      case 'auth_expired':
        this.notifyHandlers(message)
        this.handleAuthExpired()
        break
      default:
        console.warn('Unexpected authentication message:', message)
        this.notifyHandlers(message)
    }
  }
  private async handleAuthRequired(_message: WSAuthRequiredResponse): Promise<void> {
    if (!this.secure) {
      return
    }

    await this.performAuthentication('authenticate')
  }
  private resolveExpirationMs(exp: unknown): number | null {
    if (typeof exp === 'string') return new Date(exp).getTime()
    if (typeof this.pendingWsTokenExp === 'number') return this.pendingWsTokenExp * 1000

    return null
  }
  private handleAuthOk(message: WSAuthOkResponse): void {
    const expirationMs = this.resolveExpirationMs(message.exp)

    this.pendingWsTokenExp = null

    if (typeof expirationMs === 'number' && !Number.isNaN(expirationMs)) {
      this.scheduleReauthFromMs(expirationMs)
    }

    this.notifyHandlers({ ...message, type: 'auth_ok' } as WebSocketMessages)
  }
  private handleAuthComplete(message: WSAuthCompleteResponse): void {
    this.sessionExpiresAt = message.session_expires_at ?? null
    this.isAuthenticated = true
    this.onConnectionReady()
    this.notifyHandlers({ ...message, type: 'auth_complete' } as WebSocketMessages)
  }
  private handleAuthFailure(message: WSAuthFailedResponse): void {
    console.error('WebSocket authentication failed:', message)
    this.sessionExpiresAt = null
    this.isAuthenticated = false
    this.clearReauthTimer()
    this.reconnectAttempts = this.maxReconnectAttempts
    this.notifyHandlers({ ...message, type: 'auth_failed' } as WebSocketMessages)

    const authCallback = (globalThis as { authLogoutCallback?: () => void }).authLogoutCallback

    if (authCallback) {
      authCallback()
    }

    this.closeForAuthFailure()
  }
  private async handleReauthRequired(message: WSReauthRequiredResponse): Promise<void> {
    if (!this.secure) {
      return
    }

    const deadlineMs =
      typeof message.deadline === 'string' ? new Date(message.deadline).getTime() : null

    if (deadlineMs !== null && !Number.isNaN(deadlineMs)) {
      if (this.lastReauthDeadlineMs !== null && deadlineMs <= this.lastReauthDeadlineMs) {
        return
      }

      this.lastReauthDeadlineMs = deadlineMs
    } else {
      this.lastReauthDeadlineMs = null
    }

    const scheduledAt = this.reauthScheduledAt
    const now = Date.now()

    if (
      deadlineMs !== null &&
      scheduledAt !== null &&
      scheduledAt > now &&
      scheduledAt <= deadlineMs
    ) {
      return
    }

    if (this.reauthTimer !== null) {
      this.clearReauthTimer()
    }

    await this.performAuthentication('reauth')
  }
  private handleReauthOk(message: WSReauthOkResponse): void {
    const expirationMs = this.resolveExpirationMs(message.exp)

    this.pendingWsTokenExp = null

    if (typeof expirationMs === 'number' && !Number.isNaN(expirationMs)) {
      this.scheduleReauthFromMs(expirationMs)
    }

    this.notifyHandlers({ ...message, type: 'reauth_ok' } as WebSocketMessages)
  }
  private handleAuthExpired(): void {
    console.warn('WebSocket authentication expired - awaiting reconnect')
    this.isAuthenticated = false
    this.sessionExpiresAt = null
    this.clearReauthTimer()
    this.lastReauthDeadlineMs = null
  }
  private async performAuthentication(mode: 'authenticate' | 'reauth'): Promise<void> {
    if (!this.secure) {
      return
    }

    if (this.ws?.readyState !== WebSocket.OPEN) {
      return
    }

    if (this.reauthInProgress) {
      return
    }

    this.reauthInProgress = true

    try {
      const { token: wsToken, exp: wsTokenExp } = await getWsToken()

      this.pendingWsTokenExp = wsTokenExp
      const payload =
        mode === 'authenticate'
          ? { type: 'authenticate', ws_token: wsToken }
          : { type: 'reauth', ws_token: wsToken }

      this.send(payload)
    } catch (error) {
      console.error('Failed to perform WebSocket authentication:', error)

      if (mode === 'authenticate') {
        this.reconnectAttempts = this.maxReconnectAttempts
        this.closeForAuthFailure()
      }
    } finally {
      this.reauthInProgress = false
    }
  }
  private scheduleReauthFromMs(expirationMs: number): void {
    if (!this.secure) {
      return
    }

    this.clearReauthTimer()
    const now = Date.now()
    const delay = expirationMs - now - this.reauthLeadTimeMs

    if (!Number.isFinite(delay) || delay < 0) {
      this.reauthScheduledAt = null

      return
    }

    this.reauthScheduledAt = expirationMs - this.reauthLeadTimeMs
    this.reauthTimer = setTimeout(() => {
      this.reauthTimer = null
      this.reauthScheduledAt = null
      void this.performAuthentication('reauth')
    }, delay)
  }
  private clearReauthTimer(): void {
    if (this.reauthTimer !== null) {
      clearTimeout(this.reauthTimer)
      this.reauthTimer = null
    }

    this.reauthScheduledAt = null
  }
  private closeForAuthFailure(): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.close(4401, 'Authentication failure')
    } else {
      this.scheduleReconnect()
    }
  }
  private scheduleReconnect(): void {
    if (!shouldReconnect(this.isReconnecting, this.reconnectAttempts, this.maxReconnectAttempts)) {
      return
    }

    this.isReconnecting = true
    this.reconnectAttempts++
    const delay = calculateReconnectDelay(this.reconnectAttempts, this.reconnectInterval)

    setTimeout(() => {
      if (this.isReconnecting) {
        this.connect()
      }
    }, delay)
  }
  private startHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
    }

    this.heartbeatTimer = setInterval(() => {
      if (this.isConnected()) {
        this.pingSentAt = Date.now()
        this.send(createHeartbeatMessage())
      }
    }, this.heartbeatInterval)
  }
  private resubscribeTopics(): void {
    if (this.subscribedTopics.size > 0) {
      this.subscribe(Array.from(this.subscribedTopics))
    }
  }
  private notifyHandlers(message: WebSocketMessages): void {
    const handlers = this.messageHandlers.get(message.type) || []
    const globalHandlers = this.messageHandlers.get('*') || []
    const allHandlers = [...handlers, ...globalHandlers]

    allHandlers.forEach(handler => {
      try {
        handler(message)
      } catch (error) {
        console.error('Error in message handler:', error)
      }
    })
  }
  private notifyConnectionHandlers(connected: boolean): void {
    this.connectionHandlers.forEach(handler => {
      try {
        handler(connected)
      } catch (error) {
        console.error('Error in connection handler:', error)
      }
    })
  }
  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN
  }
  send(message: string | object): void {
    if (!this.isConnected()) {
      console.warn('WebSocket not connected, cannot send message')

      return
    }

    let data: string

    if (typeof message === 'string') {
      data = message
    } else {
      const stamped = this.stampProvenance(message)

      data = JSON.stringify(stamped)
    }

    if (this.ws) {
      this.ws.send(data)
    }
  }
  private stampProvenance(message: object): object {
    const msg = message as { type?: string }
    const type = msg.type

    if (!type) return message

    let counterKey: string | null = null

    if (TELEMETRY_TYPES.has(type)) {
      counterKey = 'telemetry'
    } else if (CONTROL_TYPES.has(type)) {
      counterKey = 'control'
    }

    if (!counterKey) return message

    const tracker = getTracker()

    return {
      ...message,
      public_id: uuid7(),
      session_id: tracker.sessionId,
      sequence_id: tracker.nextSequence(counterKey),
      timestamp: new Date().toISOString(),
    }
  }
  subscribe(topics: string[]): void {
    topics.forEach(topic => {
      this.subscribedTopics.add(topic)
      this.pendingSubscribes.add(topic)
      this.pendingUnsubscribes.delete(topic)
    })
    this.scheduleSubscriptionFlush()
  }
  unsubscribe(topics: string[]): void {
    topics.forEach(topic => {
      this.subscribedTopics.delete(topic)
      this.pendingUnsubscribes.add(topic)
      this.pendingSubscribes.delete(topic)
    })
    this.scheduleSubscriptionFlush()
  }
  private scheduleSubscriptionFlush(): void {
    if (this.subscriptionFlushScheduled) {
      return
    }

    this.subscriptionFlushScheduled = true
    queueMicrotask(() => {
      this.subscriptionFlushScheduled = false
      this.flushSubscriptionChanges()
    })
  }
  private flushSubscriptionChanges(): void {
    if (!this.isConnected()) {
      this.pendingSubscribes.clear()
      this.pendingUnsubscribes.clear()

      return
    }

    if (this.pendingSubscribes.size > 0) {
      const topics = Array.from(this.pendingSubscribes)

      this.pendingSubscribes.clear()
      this.send({ type: 'subscribe', topics })
    }

    if (this.pendingUnsubscribes.size > 0) {
      const topics = Array.from(this.pendingUnsubscribes)

      this.pendingUnsubscribes.clear()
      this.send({ type: 'unsubscribe', topics })
    }
  }
  onMessage<T extends WebSocketMessageType>(type: T, handler: TypedMessageHandler<T>): () => void
  onMessage(type: string, handler: MessageHandler): () => void
  onMessage(type: string, handler: MessageHandler): () => void {
    if (!this.messageHandlers.has(type)) {
      this.messageHandlers.set(type, [])
    }

    const handlers = this.messageHandlers.get(type)

    if (handlers) {
      handlers.push(handler)
    }

    return () => {
      const handlers = this.messageHandlers.get(type)

      if (handlers) {
        const index = handlers.indexOf(handler)

        if (index >= 0) {
          handlers.splice(index, 1)
        }
      }
    }
  }
  onConnection(handler: ConnectionHandler): () => void {
    this.connectionHandlers.push(handler)

    return () => {
      const index = this.connectionHandlers.indexOf(handler)

      if (index >= 0) {
        this.connectionHandlers.splice(index, 1)
      }
    }
  }
  getSubscribedTopics(): string[] {
    return Array.from(this.subscribedTopics)
  }
  subscribeToCandles(instrument?: string): void {
    if (instrument) {
      const topic = buildMarketTopic('candles', instrument)

      this.subscribe([topic])
    } else {
      this.subscribe([MARKET_TOPIC_PREFIX])
    }
  }
  subscribeToTicks(instrument?: string): void {
    if (instrument) {
      const topic = buildMarketTopic('ticks', instrument)

      this.subscribe([topic])
    } else {
      this.subscribe([MARKET_TOPIC_PREFIX])
    }
  }
  subscribeToOrders(): void {
    this.subscribe([ORDERS_COMMANDS_PREFIX, ORDERS_EVENTS_PREFIX])
  }
  subscribeToExecutions(): void {
    this.subscribe([ORDERS_EVENTS_PREFIX])
  }
  subscribeToSignals(): void {
    this.subscribe([SIGNALS_TOPIC_PREFIX])
  }
  subscribeToHeartbeats(): void {
    this.subscribe([HEARTBEATS_TOPIC_PREFIX])
  }
  subscribeToAll(): void {
    this.subscribe(getSubscriptionTopics())
  }
  async getAvailableTopics(): Promise<string[]> {
    return new Promise((resolve, reject) => {
      if (!this.isConnected()) {
        reject(new Error('WebSocket not connected'))

        return
      }

      const timeout = setTimeout(() => {
        reject(new Error('Timeout waiting for topics response'))
      }, 5000)
      const unsubscribe = this.onMessage('subscriptions_list', (message: WebSocketMessages) => {
        clearTimeout(timeout)
        unsubscribe()
        const listMessage = message as { available_topics?: string[] }

        resolve(listMessage.available_topics || [])
      })

      this.send({ type: 'get_subscriptions' })
    })
  }
  getConnectionStats(): object {
    return {
      connected: this.isConnected(),
      reconnectAttempts: this.reconnectAttempts,
      subscriptions: Array.from(this.subscribedTopics),
      isReconnecting: this.isReconnecting,
      sessionExpiresAt: this.sessionExpiresAt,
    }
  }
}

export default WebSocketClient
