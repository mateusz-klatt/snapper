import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { AppState } from '../types/ui'

const DARK_MODE_KEY = 'snapper-dark-mode'

const loadDarkModePreference = (): boolean => {
  const stored = localStorage.getItem(DARK_MODE_KEY)

  if (stored !== null) {
    return stored === 'true'
  }

  return false
}

interface AppStore extends AppState {
  setConnected: (connected: boolean) => void
  setConnectionLag: (lag: number) => void
  addSubscribedTopic: (topic: string) => void
  removeSubscribedTopic: (topic: string) => void
  setSubscribedTopics: (topics: string[]) => void
  updateLastUpdate: () => void
  toggleDarkMode: () => void
}

export const useAppStore = create<AppStore>()(
  subscribeWithSelector((set, get) => ({
    isConnected: false,
    connectionLag: 0,
    subscribedTopics: [],
    lastUpdate: new Date().toISOString(),
    isDarkMode: loadDarkModePreference(),
    setConnected: connected => set({ isConnected: connected }),
    setConnectionLag: lag => set({ connectionLag: lag }),
    addSubscribedTopic: topic => {
      const current = get().subscribedTopics

      if (!current.includes(topic)) {
        set({ subscribedTopics: [...current, topic] })
        import('../lib/websocket/instance').then(({ wsClient }) => {
          wsClient.subscribe([topic])
        })
      }
    },
    removeSubscribedTopic: topic => {
      const current = get().subscribedTopics
      const newTopics = current.filter(t => t !== topic)

      set({ subscribedTopics: newTopics })
      import('../lib/websocket/instance').then(({ wsClient }) => {
        wsClient.unsubscribe([topic])
      })
    },
    setSubscribedTopics: topics => set({ subscribedTopics: topics }),
    updateLastUpdate: () => set({ lastUpdate: new Date().toISOString() }),
    toggleDarkMode: () =>
      set(state => {
        const next = !state.isDarkMode

        localStorage.setItem(DARK_MODE_KEY, String(next))

        return { isDarkMode: next }
      }),
  }))
)
