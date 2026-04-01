import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { apiClient } from '../lib/apiClient'
import { queryClient } from '../lib/queryClient'
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
  setSubscribedTopics: (topics: string[]) => void
  updateLastUpdate: () => void
  toggleDarkMode: () => void
  setAsOf: (asOf: string) => void
  clearAsOf: () => void
}

export const useAppStore = create<AppStore>()(
  subscribeWithSelector((set, _get) => ({
    isConnected: false,
    connectionLag: 0,
    subscribedTopics: [],
    lastUpdate: new Date().toISOString(),
    isDarkMode: loadDarkModePreference(),
    asOf: null,
    isTimeTraveling: false,
    setConnected: connected => set({ isConnected: connected }),
    setConnectionLag: lag => set({ connectionLag: lag }),
    setSubscribedTopics: topics => set({ subscribedTopics: topics }),
    updateLastUpdate: () => set({ lastUpdate: new Date().toISOString() }),
    toggleDarkMode: () =>
      set(state => {
        const next = !state.isDarkMode

        localStorage.setItem(DARK_MODE_KEY, String(next))

        return { isDarkMode: next }
      }),
    setAsOf: (asOf: string) => {
      apiClient.setTimeTravelAsOf(asOf)
      set({ asOf, isTimeTraveling: true })
    },
    clearAsOf: () => {
      apiClient.setTimeTravelAsOf(null)
      set({ asOf: null, isTimeTraveling: false })
      queryClient.invalidateQueries()
    },
  }))
)
