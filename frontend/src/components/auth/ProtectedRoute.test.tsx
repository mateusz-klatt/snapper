import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import ProtectedRoute from './ProtectedRoute'
import * as stores from '../../stores/auth'

vi.mock('./LoginForm', () => ({
  default: () => <div data-testid='login-form'>Login Form</div>,
}))
vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(),
}))
describe('ProtectedRoute', () => {
  it('shows login form when not authenticated', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: false,
      user: null,
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByTestId('login-form')).toBeInTheDocument()
    expect(screen.queryByText('Protected Content')).not.toBeInTheDocument()
  })
  it('shows fallback when not authenticated and fallback provided', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: false,
      user: null,
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute fallback={<div>Custom Fallback</div>}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Custom Fallback')).toBeInTheDocument()
    expect(screen.queryByTestId('login-form')).not.toBeInTheDocument()
  })
  it('renders children when authenticated and no requirements', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'viewer' },
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })
  it('shows access denied when role requirement not met', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'viewer' },
      hasRole: vi.fn().mockReturnValue(false),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredRole='admin'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Access Denied')).toBeInTheDocument()
    expect(screen.getByText(/need admin access or higher/i)).toBeInTheDocument()
    expect(screen.getByText('viewer')).toBeInTheDocument()
  })
  it('renders children when role requirement met', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole: vi.fn().mockReturnValue(true),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredRole='admin'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })
  it('shows insufficient permissions when permission requirement not met', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'viewer' },
      hasRole: vi.fn(),
      hasPermission: vi.fn().mockReturnValue(false),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredPermission='manage:users'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Insufficient Permissions')).toBeInTheDocument()
    expect(screen.getByText('manage:users')).toBeInTheDocument()
  })
  it('renders children when permission requirement met', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole: vi.fn(),
      hasPermission: vi.fn().mockReturnValue(true),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredPermission='manage:users'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })
  it('shows resource restricted when resource access denied', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'viewer' },
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn().mockReturnValue(false),
    } as never)
    render(
      <ProtectedRoute resource='admin-panel'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Resource Restricted')).toBeInTheDocument()
    expect(screen.getByText(/admin-panel/i)).toBeInTheDocument()
  })
  it('renders children when resource access allowed', () => {
    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn().mockReturnValue(true),
    } as never)
    render(
      <ProtectedRoute resource='admin-panel'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })
  it('checks role requirement with hasRole function', () => {
    const hasRole = vi.fn().mockReturnValue(true)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole,
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredRole='operator'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(hasRole).toHaveBeenCalledWith('operator')
  })
  it('checks permission requirement with hasPermission function', () => {
    const hasPermission = vi.fn().mockReturnValue(true)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole: vi.fn(),
      hasPermission,
      canAccess: vi.fn(),
    } as never)
    render(
      <ProtectedRoute requiredPermission={'write:data' as never}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(hasPermission).toHaveBeenCalledWith('write:data')
  })
  it('checks resource access with canAccess function', () => {
    const canAccess = vi.fn().mockReturnValue(true)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess,
    } as never)
    render(
      <ProtectedRoute resource='settings'>
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(canAccess).toHaveBeenCalledWith('settings')
  })
  it('applies multiple requirements together', () => {
    const hasRole = vi.fn().mockReturnValue(true)
    const hasPermission = vi.fn().mockReturnValue(true)
    const canAccess = vi.fn().mockReturnValue(true)

    vi.mocked(stores.useAuth).mockReturnValue({
      isAuthenticated: true,
      user: { username: 'testuser', role: 'admin' },
      hasRole,
      hasPermission,
      canAccess,
    } as never)
    render(
      <ProtectedRoute
        requiredRole='admin'
        requiredPermission={'write:data' as never}
        resource='settings'
      >
        <div>Protected Content</div>
      </ProtectedRoute>
    )
    expect(hasRole).toHaveBeenCalledWith('admin')
    expect(hasPermission).toHaveBeenCalledWith('write:data')
    expect(canAccess).toHaveBeenCalledWith('settings')
    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })
})
