import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useAuth, useAuthStore } from './auth'
import { apiClient } from '../lib/apiClient'
import { queryClient } from '../lib/queryClient'

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    post: vi.fn(),
    postJSON: vi.fn(),
    get: vi.fn(),
    setCsrfToken: vi.fn(),
    clearCSRFToken: vi.fn(),
  },
}))
vi.mock('../lib/wsTicketCache', () => ({
  storeWsTicket: vi.fn(),
}))
vi.mock('../lib/queryClient', () => ({
  queryClient: {
    clear: vi.fn(),
  },
}))
describe('auth store', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAuthStore.setState({
      user: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,
      csrfToken: null,
    })
  })
  it('initializes with unauthenticated state', () => {
    const state = useAuthStore.getState()

    expect(state.isAuthenticated).toBe(false)
    expect(state.user).toBeNull()
  })
  it('provides login function', () => {
    const state = useAuthStore.getState()

    expect(typeof state.login).toBe('function')
  })
  it('provides logout function', () => {
    const state = useAuthStore.getState()

    expect(typeof state.logout).toBe('function')
  })
  it('provides clearError function', () => {
    const state = useAuthStore.getState()

    useAuthStore.setState({ error: 'Test error' })
    state.clearError()
    expect(useAuthStore.getState().error).toBeNull()
  })
  it('provides setLoading function', () => {
    const state = useAuthStore.getState()

    state.setLoading(true)
    expect(useAuthStore.getState().isLoading).toBe(true)
  })
  it('provides hasRole helper', () => {
    const state = useAuthStore.getState()

    expect(typeof state.hasRole).toBe('function')
  })
  it('provides hasPermission helper', () => {
    const state = useAuthStore.getState()

    expect(typeof state.hasPermission).toBe('function')
  })
  it('provides canAccess helper', () => {
    const state = useAuthStore.getState()

    expect(typeof state.canAccess).toBe('function')
  })
  it('useAuth exposes store selectors', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'viewer',
        role: 'viewer',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
      isLoading: false,
      error: null,
    })
    const { result } = renderHook(() => useAuth())

    expect(result.current.user?.role).toBe('viewer')
    expect(result.current.isAuthenticated).toBe(true)
    expect(typeof result.current.login).toBe('function')
    expect(typeof result.current.hasPermission).toBe('function')
  })
  it('hasRole returns false when no user', () => {
    const state = useAuthStore.getState()

    expect(state.hasRole('admin')).toBe(false)
  })
  it('hasRole returns true for user role', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasRole('admin')).toBe(true)
  })
  it('hasRole supports role hierarchy - admin has operator role', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasRole('operator')).toBe(true)
    expect(state.hasRole('viewer')).toBe(true)
  })
  it('hasRole supports role hierarchy - operator has viewer role', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'operator',
        role: 'operator',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasRole('viewer')).toBe(true)
    expect(state.hasRole('admin')).toBe(false)
  })
  it('hasPermission returns false when no user', () => {
    const state = useAuthStore.getState()

    expect(state.hasPermission('manage:users')).toBe(false)
  })
  it('hasPermission returns true for admin with all permissions', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasPermission('manage:users')).toBe(true)
    expect(state.hasPermission('read:market_data')).toBe(true)
    expect(state.hasPermission('configure:system')).toBe(true)
  })
  it('hasPermission checks operator permissions', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'operator',
        role: 'operator',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasPermission('read:market_data')).toBe(true)
    expect(state.hasPermission('create:orders')).toBe(true)
    expect(state.hasPermission('manage:users')).toBe(false)
  })
  it('hasPermission checks viewer permissions', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'viewer',
        role: 'viewer',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasPermission('read:market_data')).toBe(true)
    expect(state.hasPermission('create:orders')).toBe(false)
    expect(state.hasPermission('manage:users')).toBe(false)
  })
  it('hasPermission falls back to empty permissions for unknown role', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'mystery',
        role: 'unknown' as unknown as 'viewer',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.hasPermission('read:market_data')).toBe(false)
  })
  it('canAccess returns false when no user', () => {
    const state = useAuthStore.getState()

    expect(state.canAccess('admin')).toBe(false)
  })
  it('canAccess checks admin resource access', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.canAccess('admin')).toBe(true)
    expect(state.canAccess('settings')).toBe(true)
    expect(state.canAccess('overview')).toBe(true)
  })
  it('canAccess checks operator resource access', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'operator',
        role: 'operator',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.canAccess('overview')).toBe(true)
    expect(state.canAccess('orders')).toBe(true)
    expect(state.canAccess('admin')).toBe(false)
    expect(state.canAccess('settings')).toBe(false)
  })
  it('canAccess checks viewer resource access', () => {
    useAuthStore.setState({
      user: {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'viewer',
        role: 'viewer',
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      },
      isAuthenticated: true,
    })
    const state = useAuthStore.getState()

    expect(state.canAccess('overview')).toBe(true)
    expect(state.canAccess('market')).toBe(true)
    expect(state.canAccess('orders')).toBe(true)
    expect(state.canAccess('strategies')).toBe(true)
    expect(state.canAccess('signals')).toBe(true)
    expect(state.canAccess('health')).toBe(true)
    expect(state.canAccess('processes')).toBe(false)
    expect(state.canAccess('admin')).toBe(false)
  })
  describe('login', () => {
    it('successfully logs in user', async () => {
      const mockUser = {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'testuser',
        role: 'admin' as const,
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      }

      vi.mocked(apiClient.post).mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ payload: { user: mockUser, csrf_token: 'test-csrf' } }),
      } as Response)
      const state = useAuthStore.getState()

      await state.login({ username: 'testuser', password: 'password', remember_me: false })
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(true)
      expect(updatedState.user).toEqual(mockUser)
      expect(updatedState.csrfToken).toBe('test-csrf')
      expect(apiClient.setCsrfToken).toHaveBeenCalledWith('test-csrf')
    })
    it('handles login failure', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({
        ok: false,
        json: () => Promise.resolve({ detail: 'Invalid credentials' }),
      } as Response)
      const state = useAuthStore.getState()

      await expect(
        state.login({ username: 'testuser', password: 'wrong', remember_me: false })
      ).rejects.toThrow('Invalid credentials')
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.error).toBe('Invalid credentials')
    })
    it('handles login failure without detail', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({
        ok: false,
        json: () => Promise.resolve({}),
      } as Response)
      const state = useAuthStore.getState()

      await expect(
        state.login({ username: 'testuser', password: 'wrong', remember_me: false })
      ).rejects.toThrow('Login failed')
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.error).toBe('Login failed')
    })
    it('handles non-Error rejection during login', async () => {
      vi.mocked(apiClient.post).mockRejectedValueOnce('boom')
      const state = useAuthStore.getState()

      await expect(
        state.login({ username: 'testuser', password: 'wrong', remember_me: false })
      ).rejects.toBe('boom')
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.error).toBe('Login failed')
    })
    it('sets loading state during login', async () => {
      vi.mocked(apiClient.post).mockImplementation(
        () =>
          new Promise(resolve => {
            expect(useAuthStore.getState().isLoading).toBe(true)
            resolve({
              ok: true,
              json: () =>
                Promise.resolve({
                  payload: {
                    user: {
                      type: 'user_profile' as const,
                      sequence_id: 0,
                      public_id: 'test-pid',
                      timestamp: '2024-01-01T00:00:00Z',
                      session_id: 'test-sid',
                      username: 'test',
                      role: 'admin',
                      is_active: true,
                      created_at: '2026-01-01T00:00:00Z',
                    },
                  },
                }),
            } as Response)
          })
      )
      const state = useAuthStore.getState()

      await state.login({ username: 'test', password: 'pass', remember_me: false })
      expect(useAuthStore.getState().isLoading).toBe(false)
    })
  })
  describe('logout', () => {
    beforeEach(() => {
      useAuthStore.setState({
        user: {
          type: 'user_profile' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
        isAuthenticated: true,
        csrfToken: 'test-csrf',
      })
    })
    it('successfully logs out user', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({ ok: true } as Response)
      const state = useAuthStore.getState()

      await state.logout()
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.user).toBeNull()
      expect(updatedState.csrfToken).toBeNull()
      expect(apiClient.clearCSRFToken).toHaveBeenCalled()
    })
    it('clears state even if logout request fails', async () => {
      vi.mocked(apiClient.post).mockRejectedValueOnce(new Error('Network error'))
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const state = useAuthStore.getState()

      await state.logout()
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.user).toBeNull()
    })
    it('calls wsDisconnectCallback when available during logout', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({ ok: true } as Response)
      const mockWsDisconnect = vi.fn()

      ;(window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback =
        mockWsDisconnect
      const state = useAuthStore.getState()

      await state.logout()
      expect(mockWsDisconnect).toHaveBeenCalled()
      delete (window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback
    })
    it('clears React Query cache on successful logout', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({ ok: true } as Response)
      const state = useAuthStore.getState()

      await state.logout()
      expect(vi.mocked(queryClient.clear)).toHaveBeenCalled()
    })
    it('clears React Query cache even when logout request fails', async () => {
      vi.mocked(apiClient.post).mockRejectedValueOnce(new Error('Network error'))
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const state = useAuthStore.getState()

      await state.logout()
      expect(vi.mocked(queryClient.clear)).toHaveBeenCalled()
    })
  })
  describe('refreshToken', () => {
    beforeEach(() => {
      useAuthStore.setState({
        user: {
          type: 'user_profile' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
        isAuthenticated: true,
      })
    })
    it('successfully refreshes token', async () => {
      const mockUser = {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin' as const,
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      }

      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'new-ws-token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'new-csrf',
          user: mockUser,
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(apiClient.setCsrfToken).toHaveBeenCalledWith('new-csrf')
      expect(useAuthStore.getState().csrfToken).toBe('new-csrf')
    })
    it('fetches user info if not in response', async () => {
      useAuthStore.setState({ user: null, isAuthenticated: true })
      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'csrf',
        },
      })
      vi.mocked(apiClient.get).mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            payload: {
              type: 'user_profile' as const,
              sequence_id: 0,
              public_id: 'test-pid',
              timestamp: '2024-01-01T00:00:00Z',
              session_id: 'test-sid',
              username: 'admin',
              role: 'admin',
              is_active: true,
              created_at: '2026-01-01T00:00:00Z',
            },
          }),
      } as Response)
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(apiClient.get).toHaveBeenCalledWith('/api/auth/me')
    })
    it('handles refresh failure by logging out', async () => {
      vi.mocked(apiClient.postJSON).mockRejectedValueOnce(new Error('Refresh failed'))
      vi.mocked(apiClient.post).mockResolvedValueOnce({ ok: true } as Response)
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const state = useAuthStore.getState()

      await expect(state.refreshToken()).rejects.toThrow('Refresh failed')
      expect(useAuthStore.getState().isAuthenticated).toBe(false)
    })
    it('stores null ws ticket when ws_token is missing', async () => {
      const { storeWsTicket } = await import('../lib/wsTicketCache')

      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          csrf_token: 'csrf',
          user: {
            type: 'user_profile' as const,
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            username: 'admin',
            role: 'admin',
            is_active: true,
            created_at: '2026-01-01T00:00:00Z',
          },
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(storeWsTicket).toHaveBeenCalledWith(null)
    })
    it('stores null ws ticket when ws_token_exp is missing', async () => {
      const { storeWsTicket } = await import('../lib/wsTicketCache')

      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          csrf_token: 'csrf',
          ws_token: 'token',
          user: {
            type: 'user_profile' as const,
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            username: 'admin',
            role: 'admin',
            is_active: true,
            created_at: '2026-01-01T00:00:00Z',
          },
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(storeWsTicket).toHaveBeenCalledWith(null)
    })
    it('sets null csrf token when refresh response omits it', async () => {
      const mockUser = {
        type: 'user_profile' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        username: 'admin',
        role: 'admin' as const,
        is_active: true,
        created_at: '2026-01-01T00:00:00Z',
      }

      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          user: mockUser,
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(apiClient.setCsrfToken).toHaveBeenCalledWith(null)
      expect(useAuthStore.getState().csrfToken).toBeNull()
    })
    it('throws error when fetching user info fails', async () => {
      useAuthStore.setState({ user: null, isAuthenticated: true })
      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'csrf',
        },
      })
      vi.mocked(apiClient.get).mockResolvedValueOnce({
        ok: false,
        status: 401,
      } as Response)
      vi.mocked(apiClient.post).mockResolvedValueOnce({ ok: true } as Response)
      vi.spyOn(console, 'error').mockImplementation(() => {})
      const state = useAuthStore.getState()

      await expect(state.refreshToken()).rejects.toThrow('Failed to get user info')
    })
    it('does not update localStorage when userData is null', async () => {
      useAuthStore.setState({ user: null, isAuthenticated: true })
      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'csrf',
        },
      })
      vi.mocked(apiClient.get).mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ payload: null }),
      } as unknown as Response)
      const localStorageSpy = vi.spyOn(Storage.prototype, 'setItem')
      const state = useAuthStore.getState()

      await state.refreshToken()
      expect(localStorageSpy).not.toHaveBeenCalledWith('auth_user_id', expect.anything())
      localStorageSpy.mockRestore()
    })

    it('forwards walletId hint as active_wallet_public_id body', async () => {
      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'csrf',
          user: {
            type: 'user_profile' as const,
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            username: 'admin',
            role: 'admin' as const,
            is_active: true,
            created_at: '2026-01-01T00:00:00Z',
          },
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken({ walletId: 'wallet-7' })
      expect(apiClient.postJSON).toHaveBeenCalledWith('/api/auth/refresh', {
        active_wallet_public_id: 'wallet-7',
      })
    })

    it('forwards clear hint as clear_active_wallet body', async () => {
      vi.mocked(apiClient.postJSON).mockResolvedValueOnce({
        payload: {
          ws_token: 'token',
          ws_token_exp: new Date(Date.now() + 3600000).toISOString(),
          csrf_token: 'csrf',
          user: {
            type: 'user_profile' as const,
            sequence_id: 0,
            public_id: 'test-pid',
            timestamp: '2024-01-01T00:00:00Z',
            session_id: 'test-sid',
            username: 'admin',
            role: 'admin' as const,
            is_active: true,
            created_at: '2026-01-01T00:00:00Z',
          },
        },
      })
      const state = useAuthStore.getState()

      await state.refreshToken({ clear: true })
      expect(apiClient.postJSON).toHaveBeenCalledWith('/api/auth/refresh', {
        clear_active_wallet: true,
      })
    })
  })
  describe('useAuth hook', () => {
    it('is exported and defined', async () => {
      const authModule = await import('./auth')

      expect(authModule.useAuth).toBeDefined()
      expect(typeof authModule.useAuth).toBe('function')
    })
  })
  describe('silentLogout', () => {
    beforeEach(() => {
      useAuthStore.setState({
        user: {
          type: 'user_profile' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
        isAuthenticated: true,
        csrfToken: 'test-csrf',
      })
    })
    it('clears auth state without API call', () => {
      const state = useAuthStore.getState()

      state.silentLogout()
      const updatedState = useAuthStore.getState()

      expect(updatedState.isAuthenticated).toBe(false)
      expect(updatedState.user).toBeNull()
      expect(updatedState.csrfToken).toBeNull()
      expect(updatedState.isLoading).toBe(false)
      expect(updatedState.error).toBeNull()
      expect(apiClient.post).not.toHaveBeenCalled()
      expect(apiClient.clearCSRFToken).toHaveBeenCalled()
    })
    it('calls wsDisconnectCallback when available during silentLogout', () => {
      const mockWsDisconnect = vi.fn()

      ;(window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback =
        mockWsDisconnect
      const state = useAuthStore.getState()

      state.silentLogout()
      expect(mockWsDisconnect).toHaveBeenCalled()
      delete (window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback
    })
    it('clears React Query cache on silentLogout', () => {
      const state = useAuthStore.getState()

      state.silentLogout()
      expect(vi.mocked(queryClient.clear)).toHaveBeenCalled()
    })
  })
  describe('canAccess with unknown resource', () => {
    it('returns false for unknown resource', () => {
      useAuthStore.setState({
        user: {
          type: 'user_profile' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
        isAuthenticated: true,
      })
      const state = useAuthStore.getState()

      expect(state.canAccess('unknown_resource')).toBe(false)
    })
  })
  describe('global logout callback', () => {
    it('registers global logout callback on window', () => {
      expect(
        (window as Window & { authLogoutCallback?: () => void }).authLogoutCallback
      ).toBeDefined()
      expect(
        typeof (window as Window & { authLogoutCallback?: () => void }).authLogoutCallback
      ).toBe('function')
    })
    it('global callback triggers logout', async () => {
      vi.mocked(apiClient.post).mockResolvedValueOnce({
        ok: true,
        status: 200,
      } as Response)
      useAuthStore.setState({
        user: {
          type: 'user_profile' as const,
          sequence_id: 0,
          public_id: 'test-pid',
          timestamp: '2024-01-01T00:00:00Z',
          session_id: 'test-sid',
          username: 'admin',
          role: 'admin',
          is_active: true,
          created_at: '2026-01-01T00:00:00Z',
        },
        isAuthenticated: true,
      })
      const callback = (window as Window & { authLogoutCallback?: () => void }).authLogoutCallback

      if (callback) {
        await Promise.resolve(callback())
      }

      const state = useAuthStore.getState()

      expect(state.isAuthenticated).toBe(false)
      expect(state.user).toBeNull()
    })
    it('skips callback registration when window is undefined', async () => {
      vi.stubGlobal('window', undefined)
      vi.resetModules()
      const authModule = await import('./auth')

      expect(authModule.useAuthStore.getState).toBeDefined()
      vi.unstubAllGlobals()
      vi.resetModules()
    })
  })
})
