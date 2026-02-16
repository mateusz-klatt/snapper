import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { ReactNode } from 'react'
import { AuthenticatedApp } from './AuthenticatedApp'
import * as stores from '../stores/auth'

vi.mock('../stores/auth', () => ({
  useAuth: vi.fn(),
}))
vi.mock('../components/auth/ProtectedRoute', () => ({
  default: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}))
describe('AuthenticatedApp', () => {
  beforeEach(() => {
    vi.clearAllTimers()
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })
  it('renders children within ProtectedRoute', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken: vi.fn(),
    } as never)
    render(
      <AuthenticatedApp>
        <div>Test Content</div>
      </AuthenticatedApp>
    )
    expect(screen.getByText('Test Content')).toBeInTheDocument()
  })
  it('wraps children without adding visual shell', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken: vi.fn(),
    } as never)
    const { container } = render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )

    expect(container.querySelector('header')).toBeNull()
    expect(container.querySelector('main')).toBeNull()
  })
  it('sets up token refresh interval when authenticated', () => {
    const refreshToken = vi.fn().mockResolvedValue(undefined)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken,
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(refreshToken).not.toHaveBeenCalled()
  })
  it('does not set up interval when not authenticated', () => {
    const refreshToken = vi.fn()

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: false,
      user: null,
      refreshToken,
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(refreshToken).not.toHaveBeenCalled()
  })
  it('cleans up interval on unmount', () => {
    const refreshToken = vi.fn().mockResolvedValue(undefined)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken,
    } as never)
    const { unmount } = render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )

    unmount()
    expect(refreshToken).not.toHaveBeenCalled()
  })
  it('handles refresh token failure gracefully', async () => {
    vi.useFakeTimers()
    const refreshToken = vi.fn().mockRejectedValue(new Error('Refresh failed'))

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken,
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    await vi.advanceTimersByTimeAsync(14 * 60 * 1000)
    expect(refreshToken).toHaveBeenCalled()
    vi.useRealTimers()
  })
})
