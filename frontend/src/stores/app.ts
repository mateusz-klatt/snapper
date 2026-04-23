import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'
import { apiClient } from '../lib/apiClient'
import { queryClient } from '../lib/queryClient'
import { useAuthStore } from './auth'
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
  setCurrentOperatorPublicId: (id: string | null) => void
  setCurrentWalletPublicId: (id: string | null) => void
  selectWalletAndRefresh: (nextWalletId: string | null) => Promise<void>
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
    currentOperatorPublicId: null,
    currentWalletPublicId: null,
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
    setCurrentOperatorPublicId: (id: string | null) => {
      apiClient.setOperatorScope(id)
      apiClient.setWalletScope(null)
      set({ currentOperatorPublicId: id, currentWalletPublicId: null })
      queryClient.invalidateQueries()
    },
    setCurrentWalletPublicId: (id: string | null) => {
      apiClient.setWalletScope(id)
      set({ currentWalletPublicId: id })
      queryClient.invalidateQueries()
    },
    selectWalletAndRefresh: async (nextWalletId: string | null) => {
      // Wallet-picker sync: mint a new
      // JWT with the chosen wallet claim BEFORE swapping the client
      // scope, so the next REST/WS call authorises against the new
      // wallet. Using useAuthStore.getState() (not useAuth() hook —
      // Zustand actions run outside React's hook context).
      //
      // On refresh failure the picker state is left unchanged so the
      // user stays on the previous wallet; the refresh failure handler
      // in auth store logs + throws so the caller sees the error.
      const hint = nextWalletId === null ? { clear: true as const } : { walletId: nextWalletId }

      await useAuthStore.getState().refreshToken(hint)
      apiClient.setWalletScope(nextWalletId)
      set({ currentWalletPublicId: nextWalletId })
      queryClient.invalidateQueries()
    },
  }))
)
