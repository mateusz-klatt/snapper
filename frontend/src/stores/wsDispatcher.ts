import { QueryClient } from '@tanstack/react-query'
import WebSocketClient from '../lib/websocket/client'
import { useTradeStore } from './trade'
import { useMarketStore } from './market'
import { useAppStore } from './app'
import { useProcessStore } from './process'
import {
  WebSocketMessages,
  isOrder,
  isExecution,
  isSignal,
  isCandle,
  isTick,
  isTrade,
  isHeartbeat,
} from '../types/ws'
import { ProcessStatus } from '../types/ui'
import { orderFromWS, executionFromWS, signalFromWS } from '../lib/transforms'

type UnsubscribeFn = () => void
interface DispatcherConfig {
  queryClient: QueryClient
  topics?: string[]
  directStoreUpdates?: boolean
}

export class WSDispatcher {
  private wsClient: WebSocketClient | null = null
  private readonly queryClient: QueryClient
  private unsubscribers: UnsubscribeFn[] = []
  private readonly topics: string[]
  private readonly directStoreUpdates: boolean
  constructor(config: DispatcherConfig) {
    this.queryClient = config.queryClient
    this.topics = config.topics ?? [
      'orders.',
      'executions.',
      'signals.',
      'market.',
      'system.heartbeats.',
    ]
    this.directStoreUpdates = config.directStoreUpdates ?? true
  }
  attach(client: WebSocketClient): void {
    this.detach()
    this.wsClient = client
    this.unsubscribers.push(
      client.onMessage('order_status', this.handleOrderMessage.bind(this)),
      client.onMessage('fill', this.handleExecutionMessage.bind(this)),
      client.onMessage('signal', this.handleSignalMessage.bind(this)),
      client.onMessage('bar', this.handleCandleMessage.bind(this)),
      client.onMessage('tick', this.handleTickMessage.bind(this)),
      client.onMessage('trade', this.handleTradeMessage.bind(this)),
      client.onMessage('heartbeat', this.handleHeartbeatMessage.bind(this)),
      client.onMessage('pong', this.handlePongMessage.bind(this)),
      client.onConnection((connected: boolean) => {
        if (connected && this.topics.length > 0) {
          client.subscribe(this.topics)
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
      const existingOrder = store.orders.find(o => o.id === order.id)

      if (existingOrder) {
        store.updateOrder(order.id, order)
      } else {
        store.addOrder(order)
      }
    }

    this.queryClient.invalidateQueries({
      predicate: query => query.queryKey[0] === 'orders',
    })
  }
  private handleExecutionMessage(message: WebSocketMessages): void {
    if (!isExecution(message)) return

    if (this.directStoreUpdates) {
      const store = useTradeStore.getState()

      store.addExecution(executionFromWS(message))
    }

    this.queryClient.invalidateQueries({
      predicate: query => query.queryKey[0] === 'executions',
    })
  }
  private handleSignalMessage(message: WebSocketMessages): void {
    if (!isSignal(message)) return

    if (this.directStoreUpdates) {
      const store = useTradeStore.getState()

      store.addSignal(signalFromWS(message))
    }

    this.queryClient.invalidateQueries({
      predicate: query => query.queryKey[0] === 'signals',
    })
  }
  private handleCandleMessage(message: WebSocketMessages): void {
    if (!isCandle(message)) return

    if (this.directStoreUpdates) {
      const store = useMarketStore.getState()

      if (message.close !== undefined && message.close !== null) {
        store.updateLastPrice(message.close)
      }
    }

    const instrument = message.instrument
    const exchange = message.exchange
    const timeframe = message.timeframe

    if (instrument && exchange && timeframe) {
      this.queryClient.invalidateQueries({
        queryKey: ['candles', instrument, exchange, timeframe],
      })
    } else {
      this.queryClient.invalidateQueries({
        predicate: query => query.queryKey[0] === 'candles',
      })
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
    const rtt = (message as WebSocketMessages & { rtt_ms?: number }).rtt_ms

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
