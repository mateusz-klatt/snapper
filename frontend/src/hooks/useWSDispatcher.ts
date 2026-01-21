import { useEffect, useState, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useWebSocketStore } from '../stores/websocket'
import { WSDispatcher, getDispatcher, resetDispatcher } from '../stores/wsDispatcher'

interface UseWSDispatcherOptions {
  enabled?: boolean
  topics?: string[]
}

export function useWSDispatcher(options: UseWSDispatcherOptions = {}): WSDispatcher | null {
  const { enabled = true, topics } = options
  const queryClient = useQueryClient()
  const { wsClient, isConnected } = useWebSocketStore()
  const [dispatcher, setDispatcher] = useState<WSDispatcher | null>(null)
  const dispatcherRef = useRef<WSDispatcher | null>(null)

  useEffect(() => {
    if (!enabled) {
      setDispatcher(null)

      return
    }

    let newDispatcher: WSDispatcher

    if (topics) {
      resetDispatcher()
      newDispatcher = new WSDispatcher({
        queryClient,
        topics,
      })
    } else {
      newDispatcher = getDispatcher(queryClient)
    }

    dispatcherRef.current = newDispatcher
    setDispatcher(newDispatcher)

    return () => {
      if (dispatcherRef.current) {
        dispatcherRef.current.detach()
      }
    }
  }, [enabled, queryClient, topics])
  useEffect(() => {
    if (!enabled || !dispatcherRef.current) {
      return
    }

    if (wsClient && isConnected) {
      dispatcherRef.current.attach(wsClient)
    } else {
      dispatcherRef.current.detach()
    }
  }, [enabled, wsClient, isConnected])

  return dispatcher
}
