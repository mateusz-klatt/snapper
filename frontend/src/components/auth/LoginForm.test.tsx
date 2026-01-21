import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import LoginForm from './LoginForm'
import { useAuth } from '../../stores/auth'

vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(),
}))
const mockUseAuth = vi.mocked(useAuth)

const renderWithMocks = (ui: ReactNode) => {
  return render(ui)
}

describe('LoginForm', () => {
  const mockLogin = vi.fn()
  const mockClearError = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockUseAuth.mockReturnValue({
      login: mockLogin,
      isLoading: false,
      error: null,
      clearError: mockClearError,
      user: null,
      logout: vi.fn(),
      silentLogout: vi.fn(),
      isAuthenticated: false,
      refreshToken: vi.fn(),
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
  })
  it('renders login form', () => {
    renderWithMocks(<LoginForm />)
    expect(screen.getByLabelText(/username/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/password/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /sign in/i })).toBeInTheDocument()
  })
  it('disables submit button when fields are empty', () => {
    renderWithMocks(<LoginForm />)
    expect(screen.getByRole('button', { name: /sign in/i })).toBeDisabled()
  })
  it('enables submit button when fields are filled', async () => {
    const user = userEvent.setup()

    renderWithMocks(<LoginForm />)
    await user.type(screen.getByLabelText(/username/i), 'testuser')
    await user.type(screen.getByLabelText(/password/i), 'password123')
    expect(screen.getByRole('button', { name: /sign in/i })).not.toBeDisabled()
  })
  it('calls login on form submit', async () => {
    const user = userEvent.setup()
    const onSuccess = vi.fn()

    renderWithMocks(<LoginForm onSuccess={onSuccess} />)
    await user.type(screen.getByLabelText(/username/i), 'testuser')
    await user.type(screen.getByLabelText(/password/i), 'password123')
    await user.click(screen.getByRole('button', { name: /sign in/i }))
    await waitFor(() => {
      expect(mockClearError).toHaveBeenCalled()
      expect(mockLogin).toHaveBeenCalledWith({
        username: 'testuser',
        password: 'password123',
        remember_me: false,
      })
    })
  })
  it('displays error message', () => {
    mockUseAuth.mockReturnValue({
      login: mockLogin,
      isLoading: false,
      error: 'Invalid credentials',
      clearError: mockClearError,
      user: null,
      logout: vi.fn(),
      silentLogout: vi.fn(),
      isAuthenticated: false,
      refreshToken: vi.fn(),
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    renderWithMocks(<LoginForm />)
    expect(screen.getByText('Invalid credentials')).toBeInTheDocument()
  })
  it('shows loading state', () => {
    mockUseAuth.mockReturnValue({
      login: mockLogin,
      isLoading: true,
      error: null,
      clearError: mockClearError,
      user: null,
      logout: vi.fn(),
      silentLogout: vi.fn(),
      isAuthenticated: false,
      refreshToken: vi.fn(),
      hasRole: vi.fn(),
      hasPermission: vi.fn(),
      canAccess: vi.fn(),
    } as never)
    renderWithMocks(<LoginForm />)
    expect(screen.getByText(/signing in/i)).toBeInTheDocument()
  })
  it('handles login error gracefully', async () => {
    const loginError = new Error('Network error')

    mockLogin.mockRejectedValue(loginError)
    renderWithMocks(<LoginForm />)
    await userEvent.type(screen.getByLabelText(/username/i), 'testuser')
    await userEvent.type(screen.getByLabelText(/password/i), 'password123')
    await userEvent.click(screen.getByRole('button', { name: /sign in/i }))
    await waitFor(() => {
      expect(mockLogin).toHaveBeenCalled()
    })
  })
})
