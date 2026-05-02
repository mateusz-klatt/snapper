import { useEffect, useState } from 'react'
import { useWebSocketStore } from '../../../stores/websocket'
import { useAuthStore } from '../../../stores/auth'
import type { BacktestProgressData } from '../../../types/ws'

export type BacktestProgressSnapshot = BacktestProgressData

/**
 * Live backtest progress subscription.
 *
 * Owns the WS subscription lifecycle scoped to a single
 * (wallet, run) pair. Subscribes to the 4-segment topic prefix
 * `backtest.{wallet}.{run}.` on mount; unsubscribes on unmount.
 *
 * The wallet is read from `useAuth().user.active_wallet_public_id`
 * — the SAME field the backend authorises against (round-tripped
 * through JWT claims via /auth/refresh). When the field is `null`
 * the hook no-ops so the subscribe call never reaches the bridge
 * (server would deny anyway).
 *
 * Returns the latest progress snapshot or `null` when no event has
 * been received yet.
 */
export function useBacktestProgressSubscription(
  runPublicId: string | null
): BacktestProgressSnapshot | null {
  const { wsClient } = useWebSocketStore()
  const walletId = useAuthStore(s => s.user?.active_wallet_public_id ?? null)
  const [snapshot, setSnapshot] = useState<BacktestProgressSnapshot | null>(null)

  useEffect(() => {
    if (!wsClient || !runPublicId || !walletId) {
      return
    }

    const prefix = `backtest.${walletId}.${runPublicId}.`

    wsClient.subscribe([prefix])
    const unsubscribeConnection = wsClient.onConnection((connected: boolean) => {
      if (connected) {
        wsClient.subscribe([prefix])
      }
    })
    const unsubscribeProgress = wsClient.onMessage('backtest_progress', message => {
      if (message.type !== 'backtest_progress') return
      const progress: BacktestProgressData = message

      if (progress.run_public_id === runPublicId) {
        setSnapshot(progress)
      }
    })

    return () => {
      unsubscribeConnection()
      unsubscribeProgress()
      wsClient.unsubscribe([prefix])
    }
  }, [wsClient, runPublicId, walletId])

  return snapshot
}
