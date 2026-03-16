import { QueryClient } from '@tanstack/react-query'
import WebSocketClient from '../lib/websocket/client'
import { useTradeStore } from './trade'
import { useMarketStore } from './market'
import { useAppStore } from './app'
import { useProcessStore } from './process'
import {
  type WebSocketMessages,
  type PongWithRtt,
  type CandleData,
  type OrderData,
  type ExecutionData,
  type SignalData,
  isOrder,
  isExecution,
  isSignal,
  isCandle,
  isTick,
  isTrade,
  isHeartbeat,
} from '../types/ws'
import { ProcessStatus } from '../types/ui'
import {
  orderFromWS,
  executionFromWS,
  signalFromWS,
  orderDataFromEnvelope,
  executionDataFromEnvelope,
  signalDataFromEnvelope,
} from '../lib/transforms'

type UnsubscribeFn = () => void
interface DispatcherConfig {
  queryClient: QueryClient
  topics?: string[]
  directStoreUpdates?: boolean
  maxCandles?: number
}

const DEFAULT_MAX_CANDLES = 100

export class WSDispatcher {
  private wsClient: WebSocketClient | null = null
  private readonly queryClient: QueryClient
  private unsubscribers: UnsubscribeFn[] = []
  private readonly topics: string[]
  private readonly maxCandles: number
  private readonly directStoreUpdates: boolean
  private readonly candleBuffers: Map<string, CandleData[]> = new Map()
  private orderBuffer: OrderData[] | null = null
  private executionBuffer: ExecutionData[] | null = null
  private signalBuffer: SignalData[] | null = null
  constructor(config: DispatcherConfig) {
    this.queryClient = config.queryClient
    this.maxCandles = config.maxCandles ?? DEFAULT_MAX_CANDLES
    this.topics = config.topics ?? []
    this.directStoreUpdates = config.directStoreUpdates ?? true
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
      client.onConnection((connected: boolean) => {
        if (connected && this.topics.length > 0) {
          const existing = new Set(client.getSubscribedTopics())
          const newTopics = this.topics.filter(t => !existing.has(t))

          if (newTopics.length > 0) {
            client.subscribe(newTopics)
          }

          useAppStore.getState().setSubscribedTopics(this.topics)
        } else if (!connected) {
          useAppStore.getState().setSubscribedTopics([])
        }

        useAppStore.getState().setConnected(connected)
      })
    )

    if (client.isConnected()) {
      if (this.topics.length > 0) {
        client.subscribe(this.topics)
        useAppStore.getState().setSubscribedTopics(this.topics)
      }

      useAppStore.getState().setConnected(true)
    }
  }
  detach(): void {
    this.unsubscribers.forEach(unsub => unsub())
    this.unsubscribers = []
    this.wsClient = null
  }
  private handleOrderMessage(message: WebSocketMessages): void {
    if (!isOrder(message)) return

    if (this.directStoreUpdates) {
      const store = useTradeStore.getState()
      const order = orderFromWS(message)
      const existingOrder = store.orders.find(o => o.clientOrderId === order.clientOrderId)

      if (existingOrder) {
        store.updateOrder(order.clientOrderId, order)
      } else {
        store.addOrder(order)
      }
    }

    this.mergeOrderIntoCache(message)
  }
  private handleExecutionMessage(message: WebSocketMessages): void {
    if (!isExecution(message)) return

    if (this.directStoreUpdates) {
      const store = useTradeStore.getState()

      store.addExecution(executionFromWS(message))
    }

    this.mergeExecutionIntoCache(message)
  }
  private handleSignalMessage(message: WebSocketMessages): void {
    if (!isSignal(message)) return

    if (this.directStoreUpdates) {
      const store = useTradeStore.getState()

      store.addSignal(signalFromWS(message))
    }

    this.mergeSignalIntoCache(message)
  }
  private handleCandleMessage(message: WebSocketMessages): void {
    if (!isCandle(message)) return

    if (this.directStoreUpdates) {
      const store = useMarketStore.getState()

      if (message.close !== undefined && message.close !== null) {
        store.updateLastPrice(message.close)
      }
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
    const queryKey = ['candles', envelope.instrument, envelope.exchange, envelope.timeframe]
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
    const queries = this.queryClient.getQueriesData<OrderData[]>({ queryKey: ['orders'] })

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
    const queries = this.queryClient.getQueriesData<ExecutionData[]>({
      queryKey: ['executions'],
    })

    if (queries.every(([, data]) => !data)) {
      if (this.executionBuffer) {
        this.executionBuffer.push(envelope)
      }

      return
    }

    const data = executionDataFromEnvelope(envelope)

    for (const [queryKey, existing] of queries) {
      if (!existing) continue

      const isDuplicate = existing.some(
        e => e.client_order_id === data.client_order_id && e.executed_at === data.executed_at
      )

      if (!isDuplicate) {
        this.queryClient.setQueryData<ExecutionData[]>(queryKey, [data, ...existing])
      }
    }
  }
  private mergeSignalIntoCache(envelope: SignalData): void {
    const queries = this.queryClient.getQueriesData<SignalData[]>({ queryKey: ['signals'] })

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
        s => s.strategy_name === data.strategy_name && s.fired_at === data.fired_at
      )

      if (!isDuplicate) {
        this.queryClient.setQueryData<SignalData[]>(queryKey, [data, ...existing])
      }
    }
  }
  private handleTickMessage(message: WebSocketMessages): void {
    if (!isTick(message)) return

    if (this.directStoreUpdates) {
      const store = useMarketStore.getState()
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
        store.updateLastPrice(lastPrice)
      }
    }
  }
  private handleTradeMessage(message: WebSocketMessages): void {
    if (!isTrade(message)) return

    if (this.directStoreUpdates) {
      const store = useMarketStore.getState()

      store.updateLastPrice(message.price)
    }
  }
  private handleHeartbeatMessage(message: WebSocketMessages): void {
    if (!isHeartbeat(message)) return

    const component = message.component

    if (component) {
      const processStore = useProcessStore.getState()
      const status: ProcessStatus = {
        running: message.status === 'healthy',
        lastHeartbeat: message.timestamp ? new Date(message.timestamp).getTime() : Date.now(),
        details: { lag_ms: message.lag_ms ?? undefined },
      }
      const separatorIndex = component.includes('_')
        ? component.indexOf('_')
        : component.indexOf('.')
      const baseComponent =
        separatorIndex === -1 ? component : component.substring(0, separatorIndex)
      const suffix = separatorIndex === -1 ? 'default' : component.substring(separatorIndex + 1)
      const componentKey = suffix === 'default' ? baseComponent : `${baseComponent}_${suffix}`

      switch (baseComponent) {
        case 'feed':
          processStore.updateFeedStatus(componentKey, status)
          break
        case 'executor':
          processStore.updateExecutorStatus(componentKey, status)
          break
        case 'broker':
          processStore.updateBrokerStatus(componentKey, status)
          break
        case 'strategy':
          processStore.updateStrategyStatus(componentKey, status)
          break
      }
    }

    useAppStore.getState().updateLastUpdate()
  }
  private handlePongMessage(message: WebSocketMessages): void {
    const rtt = (message as PongWithRtt).rtt_ms

    if (rtt !== undefined) {
      useAppStore.getState().setConnectionLag(rtt)
    }
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
