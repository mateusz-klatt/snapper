import { useEffect, useState } from 'react'
import { useWebSocketStore } from '../stores/websocket'
import { useAppStore } from '../stores/app'
import { buildMarketTopic } from '../lib/websocket/topics'
import type { WSDispatcher } from '../stores/wsDispatcher'

interface MarketSubscriptionOptions {
  instrument: string | null
  exchange: string | null
  timeframe: string
  dispatcher?: WSDispatcher | null
}

export function useMarketSubscription(options: MarketSubscriptionOptions): boolean {
  const { instrument, exchange, timeframe, dispatcher } = options
  const { wsClient, isConnected } = useWebSocketStore()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const [subscribed, setSubscribed] = useState(false)

  useEffect(() => {
    if (isTimeTraveling || !isConnected || !wsClient || !instrument || !exchange) {
      setSubscribed(false)

      return
    }

    const topic = buildMarketTopic('candles', instrument, exchange, timeframe)

    if (dispatcher) {
      dispatcher.startBuffering(instrument, exchange, timeframe)
    }

    const unsubHandler = wsClient.onMessage('subscription_success', () => {
      setSubscribed(true)
    })

    wsClient.subscribe([topic])

    return () => {
      unsubHandler()
      wsClient.unsubscribe([topic])

      if (dispatcher) {
        dispatcher.stopBuffering(instrument, exchange, timeframe)
      }

      setSubscribed(false)
    }
  }, [instrument, exchange, timeframe, isConnected, wsClient, dispatcher, isTimeTraveling])

  return subscribed
}
