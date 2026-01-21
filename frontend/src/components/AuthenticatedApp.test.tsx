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
vi.mock('../components/auth/UserProfile', () => ({
  default: () => <div data-testid='user-profile'>User Profile</div>,
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
  it('renders header with title', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken: vi.fn(),
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(screen.getByText('Snapper Trading Dashboard')).toBeInTheDocument()
  })
  it('renders user profile component', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken: vi.fn(),
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(screen.getByTestId('user-profile')).toBeInTheDocument()
  })
  it('displays user role badge when user is authenticated', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { role: 'admin', username: 'testuser' },
      refreshToken: vi.fn(),
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(screen.getByText(/Connected as admin/i)).toBeInTheDocument()
  })
  it('does not display role badge when user is null', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: null,
      refreshToken: vi.fn(),
    } as never)
    render(
      <AuthenticatedApp>
        <div>Content</div>
      </AuthenticatedApp>
    )
    expect(screen.queryByText(/Connected as/i)).not.toBeInTheDocument()
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
  it('applies correct layout classes', () => {
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
    const mainContainer = container.querySelector('.h-screen')

    expect(mainContainer).toHaveClass('flex', 'flex-col', 'bg-gray-50', 'dark:bg-gray-900')
    const header = container.querySelector('header')

    expect(header).toHaveClass('flex-shrink-0', 'bg-white', 'dark:bg-gray-800')
    const main = container.querySelector('main')

    expect(main).toHaveClass('flex-1', 'overflow-hidden')
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
