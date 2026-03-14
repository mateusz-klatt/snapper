import { useEffect, useRef, useCallback } from 'react'
import type { WSDispatcher } from '../stores/wsDispatcher'

interface TradeSubscriptionOptions {
  dispatcher?: WSDispatcher | null
  enabled?: boolean
}

interface TradeSubscriptionResult {
  flush: () => void
}

export function useTradeSubscription(options: TradeSubscriptionOptions): TradeSubscriptionResult {
  const { dispatcher, enabled = true } = options
  const flushedRef = useRef(false)

  useEffect(() => {
    if (!dispatcher || !enabled) return

    dispatcher.startTradeBuffering()
    flushedRef.current = false

    return () => {
      dispatcher.stopTradeBuffering()
    }
  }, [dispatcher, enabled])

  const flush = useCallback(() => {
    if (!dispatcher || flushedRef.current) return

    dispatcher.flushTradeBuffer()
    flushedRef.current = true
  }, [dispatcher])

  return { flush }
}
