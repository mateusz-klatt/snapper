import { useAppStore } from '../stores/app'
import { useWebSocketConnection } from '../stores/websocket'
import { useWSDispatcher } from './useWSDispatcher'

interface AppShellState {
  isConnected: boolean
  connectionLag: number
  subscribedTopicsCount: number
}

export function useAppShell(): AppShellState {
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)

  useWSDispatcher({ enabled: !isTimeTraveling })
  useWebSocketConnection(undefined, { autoDisconnect: true })
  const { isConnected, connectionLag, subscribedTopics } = useAppStore()

  return {
    isConnected,
    connectionLag,
    subscribedTopicsCount: subscribedTopics.length,
  }
}
