import { useState, useEffect, useCallback } from 'react'
import { useWebSocketStore } from '../stores/websocket'
import { useAppStore } from '../stores/app'
import { HEARTBEAT_STALE_THRESHOLD_MS, HEARTBEAT_PRUNE_INTERVAL_MS } from '../lib/constants'

export interface HeartbeatData {
  status: 'healthy' | 'warning' | 'error'
  lag_ms?: number
  timestamp: number
  healthy: boolean
}

export function useHeartbeats(topics: string[]): Record<string, HeartbeatData> {
  const [allHeartbeats, setAllHeartbeats] = useState<Record<string, HeartbeatData>>({})
  const { wsClient } = useWebSocketStore()
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  useEffect(() => {
    if (isTimeTraveling || !wsClient) {
      return
    }

    wsClient.subscribe(topics)
    const unsubscribeConnection = wsClient.onConnection((connected: boolean) => {
      if (connected) {
        wsClient.subscribe(topics)
      }
    })
    const unsubscribeHeartbeat = wsClient.onMessage('heartbeat', message => {
      const heartbeat: HeartbeatData = {
        status: message.status,
        lag_ms: message.lag_ms || 0,
        timestamp: Date.now(),
        healthy: message.status === 'healthy',
      }

      setAllHeartbeats(prev => ({ ...prev, [message.component]: heartbeat }))
    })

    return () => {
      unsubscribeConnection()
      unsubscribeHeartbeat()
      wsClient.unsubscribe(topics)
    }
  }, [wsClient, topics, isTimeTraveling])

  const pruneStaleHeartbeats = useCallback(() => {
    const now = Date.now()

    setAllHeartbeats(prev => {
      const updated = { ...prev }

      Object.keys(updated).forEach(key => {
        if (now - updated[key].timestamp > HEARTBEAT_STALE_THRESHOLD_MS) {
          delete updated[key]
        }
      })

      return updated
    })
  }, [])

  useEffect(() => {
    const interval = setInterval(pruneStaleHeartbeats, HEARTBEAT_PRUNE_INTERVAL_MS)

    return () => clearInterval(interval)
  }, [pruneStaleHeartbeats])

  return allHeartbeats
}
