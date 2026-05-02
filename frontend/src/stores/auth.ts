import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type { Components } from '../types/api.generated'
import type { LoginBody } from '../types/api'
import { RESOURCE_ACCESS, ROLE_PERMISSIONS } from '../types/permissions.generated'
import type { Permission } from '../types/permissions.generated'
import { apiClient } from '../lib/apiClient'
import { queryClient } from '../lib/queryClient'
import { storeWsTicket } from '../lib/wsTicketCache'

type User = Components['schemas']['UserProfile']
type UserRole = Components['schemas']['UserRole']

/**
 * Tagged-union arg for ``refreshToken``.
 *
 * - ``undefined`` — the three historical zero-body callers
 *   (``apiClient.refreshAndRetry``, WS ticket refresh, the original
 *   ``useAuth().refreshToken()`` call sites): POSTs no body,
 *   preserving the existing JWT claims byte-identically.
 * - ``{ walletId }`` — mint new tokens scoped to that wallet; server
 *   validates membership and returns 404 on foreign wallet.
 * - ``{ clear: true }`` — explicitly clear the JWT wallet claim to
 *   ``null`` ("All wallets" picker option).
 */
type RefreshWalletHint = { walletId: string } | { clear: true } | undefined
type WindowWithCallbacks = typeof globalThis & {
  authLogoutCallback?: () => void
  wsDisconnectCallback?: () => void
}
interface AuthState {
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
  error: string | null
  csrfToken: string | null
  login: (credentials: LoginBody) => Promise<void>
  logout: () => Promise<void>
  silentLogout: () => void
  refreshToken: (nextWallet?: RefreshWalletHint) => Promise<void>
  clearError: () => void
  setLoading: (loading: boolean) => void
  hasRole: (role: UserRole) => boolean
  hasPermission: (permission: Permission) => boolean
  canAccess: (resource: string) => boolean
}
const ROLE_HIERARCHY: Record<UserRole, number> = {
  ai_delegate: -1,
  viewer: 1,
  operator: 2,
  admin: 3,
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => {
      const win = globalThis as WindowWithCallbacks

      win.authLogoutCallback = () => {
        get().logout()
      }

      return {
        user: null,
        isAuthenticated: false,
        isLoading: false,
        error: null,
        csrfToken: null,
        login: async (credentials: LoginBody) => {
          try {
            set({ isLoading: true, error: null })
            const response = await apiClient.post('/api/auth/login', credentials, {
              skipCSRF: true,
            })

            if (!response.ok) {
              const errorData = await response.json()

              throw new Error(errorData.detail || 'Login failed')
            }

            const envelope: { payload: { user: User; csrf_token?: string } } = await response.json()
            const data = envelope.payload

            apiClient.setCsrfToken(data.csrf_token ?? null)
            set({
              user: data.user,
              isAuthenticated: true,
              csrfToken: data.csrf_token ?? null,
              isLoading: false,
              error: null,
            })
            localStorage.setItem('auth_user_id', data.user.username)
          } catch (error) {
            set({
              isLoading: false,
              error: error instanceof Error ? error.message : 'Login failed',
              isAuthenticated: false,
              user: null,
              csrfToken: null,
            })
            throw error
          }
        },
        logout: async () => {
          try {
            set({ isLoading: true })
            set({ isAuthenticated: false })
            const win = globalThis as WindowWithCallbacks

            if (win.wsDisconnectCallback) {
              win.wsDisconnectCallback()
            }

            await apiClient.post('/api/auth/logout', undefined, {
              skipRetry: true,
              skipCSRF: true,
            })
          } catch (error) {
            console.error('Logout request failed:', error)
          } finally {
            apiClient.clearCSRFToken()
            storeWsTicket(null)
            queryClient.clear()
            set({
              user: null,
              isAuthenticated: false,
              csrfToken: null,
              isLoading: false,
              error: null,
            })
          }
        },
        silentLogout: () => {
          set({ isAuthenticated: false })
          const win = globalThis as WindowWithCallbacks

          if (win.wsDisconnectCallback) {
            win.wsDisconnectCallback()
          }

          apiClient.clearCSRFToken()
          storeWsTicket(null)
          queryClient.clear()
          set({
            user: null,
            isAuthenticated: false,
            csrfToken: null,
            isLoading: false,
            error: null,
          })
        },
        refreshToken: async (nextWallet?: RefreshWalletHint) => {
          try {
            let body: { active_wallet_public_id?: string; clear_active_wallet?: true } | undefined

            if (nextWallet !== undefined) {
              body =
                'clear' in nextWallet
                  ? { clear_active_wallet: true }
                  : { active_wallet_public_id: nextWallet.walletId }
            }

            const envelope: {
              payload: {
                message?: string
                ws_token?: string
                ws_token_exp?: string
                csrf_token?: string
                user?: User
              }
            } =
              body === undefined
                ? await apiClient.postJSON('/api/auth/refresh')
                : await apiClient.postJSON('/api/auth/refresh', body)
            const data = envelope.payload

            if (typeof data.ws_token === 'string' && typeof data.ws_token_exp === 'string') {
              const expSeconds = Math.floor(new Date(data.ws_token_exp).getTime() / 1000)

              storeWsTicket({ token: data.ws_token, exp: expSeconds })
            } else {
              storeWsTicket(null)
            }

            apiClient.setCsrfToken(data.csrf_token ?? null)
            let userData = data.user ?? get().user

            if (!userData) {
              const userResponse = await apiClient.get('/api/auth/me')

              if (!userResponse.ok) {
                throw new Error('Failed to get user info')
              }

              const meEnvelope: { payload: User } = await userResponse.json()

              userData = meEnvelope.payload
            }

            set({
              user: userData,
              isAuthenticated: true,
              csrfToken: data.csrf_token ?? null,
            })

            if (userData) {
              localStorage.setItem('auth_user_id', userData.username)
            }
          } catch (error) {
            console.error('Token refresh failed:', error)
            get().logout()
            throw error
          }
        },
        clearError: () => set({ error: null }),
        setLoading: (loading: boolean) => set({ isLoading: loading }),
        hasRole: (role: UserRole) => {
          const { user } = get()

          if (!user) return false

          return ROLE_HIERARCHY[user.role] >= ROLE_HIERARCHY[role]
        },
        hasPermission: (permission: Permission) => {
          const { user } = get()

          if (!user) return false

          if (user.role === 'admin') {
            return true
          }

          const userPermissions: readonly Permission[] = ROLE_PERMISSIONS[user.role] || []

          return userPermissions.includes(permission)
        },
        canAccess: (resource: string) => {
          const { user } = get()

          if (!user) return false
          const allowedRoles = RESOURCE_ACCESS[resource] || []

          return allowedRoles.includes(user.role)
        },
      }
    },
    {
      name: 'snapper-auth',
      partialize: (state: AuthState) => ({
        user: state.user,
      }),
    }
  )
)

export const useAuth = () => {
  const authStore = useAuthStore()

  return {
    user: authStore.user,
    isAuthenticated: authStore.isAuthenticated,
    isLoading: authStore.isLoading,
    error: authStore.error,
    login: authStore.login,
    logout: authStore.logout,
    silentLogout: authStore.silentLogout,
    refreshToken: authStore.refreshToken,
    clearError: authStore.clearError,
    hasRole: authStore.hasRole,
    hasPermission: authStore.hasPermission,
    canAccess: authStore.canAccess,
  }
}
