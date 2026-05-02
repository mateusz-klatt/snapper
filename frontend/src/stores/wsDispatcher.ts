import { QueryClient } from '@tanstack/react-query'
import WebSocketClient from '../lib/websocket/client'
import { useMarketStore } from './market'
import { useAppStore } from './app'
import { useAuthStore } from './auth'
import {
  type WebSocketMessages,
  type PongWithRtt,
  type CandleData,
  type OrderData,
  type ExecutionData,
  type SignalData,
  type AiReviewCapsViolationFrameData,
  type AiReviewDecisionAckFrameData,
  type AiReviewRequestFrameData,
  isOrder,
  isExecution,
  isSignal,
  isCandle,
  isTick,
  isTrade,
  isHeartbeat,
  isAiReviewRequest,
  isAiReviewDecisionAck,
  isAiReviewCapsViolation,
} from '../types/ws'
import {
  orderDataFromEnvelope,
  executionDataFromEnvelope,
  signalDataFromEnvelope,
} from '../lib/transforms'

export type AiReviewActivityFrame =
  | AiReviewRequestFrameData
  | AiReviewDecisionAckFrameData
  | AiReviewCapsViolationFrameData

export const AI_REVIEW_ACTIVITY_QUERY_KEY_ROOT = 'ai-review-activity'
export const AI_REVIEW_ACTIVITY_RING_CAP = 1024

/**
 * Compose the per-user cache key for the AI review activity ring buffer.
 *
 * Defense-in-depth atop B#3 (`feddb225` 2026-05-01) — the QueryClient
 * is wiped via `queryClient.clear()` in both logout paths
 * (`logout` + `silentLogout`), so the primary cross-user leak vector
 * is closed there. The user public_id is folded into the activity key
 * as a belt-and-suspenders guard: even a hypothetical bug in the
 * logout teardown order could not leak delegate A's frames into
 * delegate B's view.
 *
 * Per-key user scoping is intentionally NOT applied to
 * orders/signals/executions/positions (per B#3 decision) — those
 * caches are owned by the QueryClient.clear() teardown plus REST
 * refetch on every mount; adding per-key scoping there would be a
 * risky defensive overlap with no marginal safety gain.
 *
 * ``null`` is the "unauthenticated" sentinel; frames written under
 * this key are intentionally orphaned because no logged-in delegate
 * should ever read them.
 */
export function aiReviewActivityQueryKey(userPublicId: string | null): readonly unknown[] {
  return [AI_REVIEW_ACTIVITY_QUERY_KEY_ROOT, userPublicId]
}

/**
 * Dedup key — `(type, review_public_id, dispatch_version)`.
 *
 * The triple is the protocol-level uniqueness contract: two frames with
 * the same triple are by-construction the same logical event (e.g. a
 * caps-violation re-fanout from a different worker). Collapsing on
 * `review_public_id` alone would incorrectly merge a v0 request with
 * a later v1 re-dispatch.
 */
function aiReviewActivityDedupKey(frame: AiReviewActivityFrame): string {
  return `${frame.type}|${frame.review_public_id}|${frame.dispatch_version}`
}

type UnsubscribeFn = () => void
interface DispatcherConfig {
  queryClient: QueryClient
  topics?: string[]
  maxCandles?: number
}

const DEFAULT_MAX_CANDLES = 100

export class WSDispatcher {
  private wsClient: WebSocketClient | null = null
  private readonly queryClient: QueryClient
  private unsubscribers: UnsubscribeFn[] = []
  private readonly topics: string[]
  private readonly maxCandles: number
  private readonly candleBuffers: Map<string, CandleData[]> = new Map()
  private orderBuffer: OrderData[] | null = null
  private executionBuffer: ExecutionData[] | null = null
  private signalBuffer: SignalData[] | null = null
  constructor(config: DispatcherConfig) {
    this.queryClient = config.queryClient
    this.maxCandles = config.maxCandles ?? DEFAULT_MAX_CANDLES
    this.topics = config.topics ?? []
  }
  attach(client: WebSocketClient): void {
    this.detach()
    this.wsClient = client
    this.unsubscribers.push(
      client.onMessage('order', this.handleOrderMessage.bind(this)),
      client.onMessage('execution', this.handleExecutionMessage.bind(this)),
      client.onMessage('signal', this.handleSignalMessage.bind(this)),
      client.onMessage('candle', this.handleCandleMessage.bind(this)),
      client.onMessage('tick', this.handleTickMessage.bind(this)),
      client.onMessage('trade', this.handleTradeMessage.bind(this)),
      client.onMessage('heartbeat', this.handleHeartbeatMessage.bind(this)),
      client.onMessage('pong', this.handlePongMessage.bind(this)),
      client.onMessage('ai_review.request', this.handleAiReviewActivityMessage.bind(this)),
      client.onMessage('ai_review.decision_ack', this.handleAiReviewActivityMessage.bind(this)),
      client.onMessage('ai_review.caps_violation', this.handleAiReviewActivityMessage.bind(this)),
      client.onMessage('subscription_success', () => {
        useAppStore.getState().setSubscribedTopics(client.getSubscribedTopics())
      }),
      client.onConnection((connected: boolean) => {
        if (connected && this.topics.length > 0) {
          const existing = new Set(client.getSubscribedTopics())
          const newTopics = this.topics.filter(t => !existing.has(t))

          if (newTopics.length > 0) {
            client.subscribe(newTopics)
          }

          useAppStore.getState().setSubscribedTopics(client.getSubscribedTopics())
        } else if (!connected) {
          useAppStore.getState().setSubscribedTopics([])
        }

        useAppStore.getState().setConnected(connected)
      })
    )

    if (client.isConnected()) {
      if (this.topics.length > 0) {
        client.subscribe(this.topics)
      }

      useAppStore.getState().setConnected(true)
      useAppStore.getState().setSubscribedTopics(client.getSubscribedTopics())
    }
  }
  detach(): void {
    this.unsubscribers.forEach(unsub => unsub())
    this.unsubscribers = []
    this.wsClient = null
  }
  private handleOrderMessage(message: WebSocketMessages): void {
    if (!isOrder(message)) return

    this.mergeOrderIntoCache(message)
  }
  private handleExecutionMessage(message: WebSocketMessages): void {
    if (!isExecution(message)) return

    this.mergeExecutionIntoCache(message)
  }
  private handleSignalMessage(message: WebSocketMessages): void {
    if (!isSignal(message)) return

    this.mergeSignalIntoCache(message)
  }
  private handleCandleMessage(message: WebSocketMessages): void {
    if (!isCandle(message)) return

    if (message.close !== undefined && message.close !== null) {
      useMarketStore.getState().updateLastPrice(message.close)
    }

    if (message.instrument && message.exchange && message.timeframe) {
      this.mergeCandleIntoCache(message)
    }
  }
  startBuffering(instrument: string, exchange: string, timeframe: string): void {
    const bufferKey = `${instrument}:${exchange}:${timeframe}`

    this.candleBuffers.set(bufferKey, [])
  }
  flushBuffer(instrument: string, exchange: string, timeframe: string): void {
    const bufferKey = `${instrument}:${exchange}:${timeframe}`
    const buffered = this.candleBuffers.get(bufferKey)

    this.candleBuffers.delete(bufferKey)

    if (!buffered || buffered.length === 0) {
      return
    }

    for (const candle of buffered) {
      this.mergeCandleIntoCache(candle)
    }
  }
  stopBuffering(instrument: string, exchange: string, timeframe: string): void {
    const bufferKey = `${instrument}:${exchange}:${timeframe}`

    this.candleBuffers.delete(bufferKey)
  }
  startTradeBuffering(): void {
    this.orderBuffer = []
    this.executionBuffer = []
    this.signalBuffer = []
  }
  flushTradeBuffer(): void {
    const orders = this.orderBuffer
    const executions = this.executionBuffer
    const signals = this.signalBuffer

    this.orderBuffer = null
    this.executionBuffer = null
    this.signalBuffer = null

    if (orders) {
      for (const o of orders) {
        this.mergeOrderIntoCache(o)
      }
    }

    if (executions) {
      for (const e of executions) {
        this.mergeExecutionIntoCache(e)
      }
    }

    if (signals) {
      for (const s of signals) {
        this.mergeSignalIntoCache(s)
      }
    }
  }
  stopTradeBuffering(): void {
    this.orderBuffer = null
    this.executionBuffer = null
    this.signalBuffer = null
  }
  private mergeCandleIntoCache(envelope: CandleData): void {
    const queryKey = ['candles', envelope.instrument, envelope.exchange, envelope.timeframe, null]
    const existing = this.queryClient.getQueryData<CandleData[]>(queryKey)

    if (!existing) {
      const bufferKey = `${envelope.instrument}:${envelope.exchange}:${envelope.timeframe}`
      const buffer = this.candleBuffers.get(bufferKey)

      if (buffer) {
        buffer.push(envelope)
      }

      return
    }

    const incomingTime = new Date(envelope.open_at).getTime()
    const lastCandle = existing[existing.length - 1]
    const lastTime = lastCandle ? new Date(lastCandle.open_at).getTime() : 0

    if (incomingTime === lastTime) {
      const updated = [...existing]

      updated[updated.length - 1] = envelope
      this.queryClient.setQueryData<CandleData[]>(queryKey, updated)
    } else if (incomingTime > lastTime) {
      const appended = [...existing, envelope]
      const trimmed =
        appended.length > this.maxCandles ? appended.slice(-this.maxCandles) : appended

      this.queryClient.setQueryData<CandleData[]>(queryKey, trimmed)
    }
  }
  private mergeOrderIntoCache(envelope: OrderData): void {
    const queries = this.queryClient
      .getQueriesData<OrderData[]>({ queryKey: ['orders'] })
      .filter(([key]) => key[key.length - 1] === null)

    if (queries.every(([, data]) => !data)) {
      if (this.orderBuffer) {
        this.orderBuffer.push(envelope)
      }

      return
    }

    const data = orderDataFromEnvelope(envelope)

    for (const [queryKey, existing] of queries) {
      if (!existing) continue

      const idx = existing.findIndex(o => o.client_order_id === data.client_order_id)

      if (idx >= 0) {
        const updated = [...existing]

        updated[idx] = data
        this.queryClient.setQueryData<OrderData[]>(queryKey, updated)
      } else {
        this.queryClient.setQueryData<OrderData[]>(queryKey, [data, ...existing])
      }
    }
  }
  private mergeExecutionIntoCache(envelope: ExecutionData): void {
    const queries = this.queryClient
      .getQueriesData<ExecutionData[]>({ queryKey: ['executions'] })
      .filter(([key]) => key[key.length - 1] === null)

    if (queries.every(([, data]) => !data)) {
      if (this.executionBuffer) {
        this.executionBuffer.push(envelope)
      }

      return
    }

    const data = executionDataFromEnvelope(envelope)

    for (const [queryKey, existing] of queries) {
      if (!existing) continue

      const isDuplicate = existing.some(e => e.public_id === data.public_id)

      if (!isDuplicate) {
        this.queryClient.setQueryData<ExecutionData[]>(queryKey, [data, ...existing])
      }
    }
  }
  private mergeSignalIntoCache(envelope: SignalData): void {
    const queries = this.queryClient
      .getQueriesData<SignalData[]>({ queryKey: ['signals'] })
      .filter(([key]) => key[key.length - 1] === null)

    if (queries.every(([, data]) => !data)) {
      if (this.signalBuffer) {
        this.signalBuffer.push(envelope)
      }

      return
    }

    const data = signalDataFromEnvelope(envelope)

    for (const [queryKey, existing] of queries) {
      if (!existing) continue

      const isDuplicate = existing.some(
        s =>
          s.strategy_name === data.strategy_name &&
          s.fired_at === data.fired_at &&
          s.instrument === data.instrument &&
          s.exchange === data.exchange
      )

      if (!isDuplicate) {
        this.queryClient.setQueryData<SignalData[]>(queryKey, [data, ...existing])
      }
    }
  }
  private handleTickMessage(message: WebSocketMessages): void {
    if (!isTick(message)) return

    let lastPrice: number | null = message.last ?? null

    if (
      lastPrice === null &&
      message.bid !== null &&
      message.bid !== undefined &&
      message.ask !== null &&
      message.ask !== undefined
    ) {
      lastPrice = (message.bid + message.ask) / 2
    }

    if (lastPrice !== null) {
      useMarketStore.getState().updateLastPrice(lastPrice)
    }
  }
  private handleTradeMessage(message: WebSocketMessages): void {
    if (!isTrade(message)) return

    useMarketStore.getState().updateLastPrice(message.price)
  }
  private handleHeartbeatMessage(message: WebSocketMessages): void {
    if (!isHeartbeat(message)) return

    useAppStore.getState().updateLastUpdate()
  }
  private handlePongMessage(message: WebSocketMessages): void {
    const rtt = (message as PongWithRtt).rtt_ms

    if (rtt !== undefined) {
      useAppStore.getState().setConnectionLag(rtt)
    }
  }
  private handleAiReviewActivityMessage(message: WebSocketMessages): void {
    if (
      !isAiReviewRequest(message) &&
      !isAiReviewDecisionAck(message) &&
      !isAiReviewCapsViolation(message)
    ) {
      return
    }

    this.mergeAiReviewActivity(message)
  }
  private mergeAiReviewActivity(frame: AiReviewActivityFrame): void {
    const userPublicId = useAuthStore.getState().user?.public_id ?? null
    const queryKey = aiReviewActivityQueryKey(userPublicId)
    const existing = this.queryClient.getQueryData<AiReviewActivityFrame[]>(queryKey) ?? []
    const dedupKey = aiReviewActivityDedupKey(frame)

    if (existing.some(item => aiReviewActivityDedupKey(item) === dedupKey)) {
      return
    }

    const appended = [...existing, frame]
    const trimmed =
      appended.length > AI_REVIEW_ACTIVITY_RING_CAP
        ? appended.slice(-AI_REVIEW_ACTIVITY_RING_CAP)
        : appended

    this.queryClient.setQueryData<AiReviewActivityFrame[]>(queryKey, trimmed)
  }
  getClient(): WebSocketClient | null {
    return this.wsClient
  }
  isAttached(): boolean {
    return this.wsClient !== null
  }
}
let dispatcherInstance: WSDispatcher | null = null

export function getDispatcher(queryClient: QueryClient): WSDispatcher {
  dispatcherInstance ??= new WSDispatcher({ queryClient })

  return dispatcherInstance
}

export function resetDispatcher(): void {
  if (dispatcherInstance) {
    dispatcherInstance.detach()
    dispatcherInstance = null
  }
}
