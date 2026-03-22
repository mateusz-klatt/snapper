import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import UserProfile from './UserProfile'
import { useAuth } from '../../stores/auth'
import { apiClient } from '../../lib/apiClient'

vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(),
}))
vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    changePassword: vi.fn(),
  },
}))
const mockUseAuth = vi.mocked(useAuth)
const mockChangePassword = vi.mocked(apiClient.changePassword)

const renderWithMocks = (ui: ReactNode) => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const createMockAuth = (overrides = {}) => ({
  user: null,
  logout: vi.fn(),
  isLoading: false,
  error: null,
  login: vi.fn(),
  clearError: vi.fn(),
  silentLogout: vi.fn(),
  isAuthenticated: false,
  refreshToken: vi.fn(),
  hasRole: vi.fn(),
  hasPermission: vi.fn(),
  canAccess: vi.fn(),
  ...overrides,
})

afterEach(() => {
  vi.useRealTimers()
})
describe('UserProfile', () => {
  const mockLogout = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'testuser',
          role: 'admin',
          is_active: true,
        },
        logout: mockLogout,
        isAuthenticated: true,
      }) as never
    )
  })
  it('renders nothing when no user', () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: null,
        logout: mockLogout,
      }) as never
    )
    const { container } = renderWithMocks(<UserProfile />)

    expect(container.firstChild).toBeNull()
  })
  it('renders user profile button', () => {
    renderWithMocks(<UserProfile />)
    expect(screen.getByText('testuser')).toBeInTheDocument()
  })
  it('shows dropdown on click', async () => {
    const user = userEvent.setup()

    renderWithMocks(<UserProfile />)
    await user.click(screen.getByText('testuser'))
    expect(screen.getByText('Permissions:')).toBeInTheDocument()
  })
  it('shows admin permissions in dropdown', async () => {
    const user = userEvent.setup()

    renderWithMocks(<UserProfile />)
    await user.click(screen.getByText('testuser'))
    expect(screen.getByText(/Full system administration/i)).toBeInTheDocument()
  })
  it('does not show admin permissions for operator', async () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'operator',
          role: 'operator',
          is_active: true,
        },
        logout: mockLogout,
        isAuthenticated: true,
      }) as never
    )
    const user = userEvent.setup()

    renderWithMocks(<UserProfile />)
    await user.click(screen.getByText('operator'))
    expect(screen.queryByText(/Full system administration/i)).not.toBeInTheDocument()
  })
  it('displays correct role color for admin', () => {
    renderWithMocks(<UserProfile />)
    expect(screen.getByText('Admin')).toHaveClass('bg-loss-100')
  })
  it('displays correct role color for operator', () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'operator',
          role: 'operator',
          is_active: true,
        },
        logout: mockLogout,
        isAuthenticated: true,
      }) as never
    )
    renderWithMocks(<UserProfile />)
    expect(screen.getByText('Operator')).toHaveClass('bg-brand-100')
  })
  it('displays correct role color for viewer', () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'viewer',
          role: 'viewer',
          is_active: true,
        },
        logout: mockLogout,
        isAuthenticated: true,
      }) as never
    )
    renderWithMocks(<UserProfile />)
    expect(screen.getByText('Viewer')).toHaveClass('bg-accent-100')
  })
  it('calls logout on sign out', async () => {
    const user = userEvent.setup()

    renderWithMocks(<UserProfile />)
    await user.click(screen.getByText('testuser'))
    await user.click(screen.getByText('Sign out'))
    expect(mockLogout).toHaveBeenCalled()
  })
  it('shows loading state during logout', async () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'testuser',
          role: 'admin',
          is_active: true,
        },
        logout: mockLogout,
        isLoading: true,
        isAuthenticated: true,
      }) as never
    )
    renderWithMocks(<UserProfile />)
    const button = screen.getByText('testuser')

    await userEvent.click(button)
    expect(screen.getByText('Signing out...')).toBeInTheDocument()
  })
  it('displays default role color for unknown role', () => {
    mockUseAuth.mockReturnValue(
      createMockAuth({
        user: {
          username: 'unknown',
          role: 'custom_role',
          is_active: true,
        },
        logout: mockLogout,
        isAuthenticated: true,
      }) as never
    )
    renderWithMocks(<UserProfile />)
    expect(screen.getByText('Custom_role')).toHaveClass('bg-muted-200')
  })
  it('handles logout error gracefully', async () => {
    const logoutError = new Error('Logout failed')

    mockLogout.mockRejectedValue(logoutError)
    const user = userEvent.setup()

    renderWithMocks(<UserProfile />)
    await user.click(screen.getByText('testuser'))
    await user.click(screen.getByText('Sign out'))
    expect(mockLogout).toHaveBeenCalled()
  })
  it('closes dropdown when backdrop is clicked', async () => {
    const user = userEvent.setup()
    const { container } = renderWithMocks(<UserProfile />)

    await user.click(screen.getByText('testuser'))
    expect(screen.getByText('Sign out')).toBeInTheDocument()
    const backdrop = container.querySelector('.fixed.inset-0')

    expect(backdrop).toBeInTheDocument()

    if (backdrop) {
      await user.click(backdrop)
    }

    await vi.waitFor(() => {
      expect(screen.queryByText('Sign out')).not.toBeInTheDocument()
    })
  })
  describe('Change Password', () => {
    beforeEach(() => {
      mockChangePassword.mockReset()
    })
    it('shows change password form when clicking Change password button', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      expect(screen.getByRole('heading', { name: 'Change Password' })).toBeInTheDocument()
      expect(screen.getByLabelText('Current Password')).toBeInTheDocument()
      expect(screen.getByLabelText('New Password')).toBeInTheDocument()
      expect(screen.getByLabelText('Confirm New Password')).toBeInTheDocument()
    })
    it('closes form when clicking Cancel', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.click(screen.getByText('Cancel'))
      await waitFor(() => {
        expect(screen.queryByText('Current Password')).not.toBeInTheDocument()
      })
    })
    it('shows error when passwords do not match', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'different123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      expect(screen.getByText('New passwords do not match')).toBeInTheDocument()
    })
    it('shows error when password is too short', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'short')
      await user.type(screen.getByLabelText('Confirm New Password'), 'short')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      expect(screen.getByText('Password must be at least 8 characters')).toBeInTheDocument()
    })
    it('calls changePassword API on successful submit', async () => {
      mockChangePassword.mockResolvedValue({ payload: 'Password changed' })
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await waitFor(() => {
        expect(screen.getByLabelText('Current Password')).toBeInTheDocument()
      })
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      await waitFor(() => {
        expect(mockChangePassword).toHaveBeenCalledWith('testuser', 'oldpassword', 'newpassword123')
      })
      await waitFor(() => {
        expect(screen.getByText('Password changed successfully')).toBeInTheDocument()
      })
    })
    it('closes password form after successful change timeout', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true })
      mockChangePassword.mockResolvedValue({ payload: 'Password changed' })
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await waitFor(() => {
        expect(screen.getByLabelText('Current Password')).toBeInTheDocument()
      })
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      await waitFor(() => {
        expect(screen.getByText('Password changed successfully')).toBeInTheDocument()
      })
      await act(async () => {
        vi.advanceTimersByTime(2100)
      })
      await waitFor(() => {
        expect(screen.queryByLabelText('Current Password')).not.toBeInTheDocument()
      })
    })
    it('shows error on API failure', async () => {
      mockChangePassword.mockRejectedValue(new Error('Invalid current password or user not found'))
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      await waitFor(() => {
        expect(screen.getByText('Invalid current password or user not found')).toBeInTheDocument()
      })
    })
    it('shows fallback error for non-Error rejection', async () => {
      mockChangePassword.mockRejectedValue('string error')
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      await waitFor(() => {
        expect(screen.getByText('Failed to change password')).toBeInTheDocument()
      })
    })
    it('shows API error message directly', async () => {
      mockChangePassword.mockRejectedValue(new Error('Invalid current password or user not found'))
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'wrongpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      await waitFor(() => {
        expect(screen.getByText('Invalid current password or user not found')).toBeInTheDocument()
      })
    })
    it('shows loading state while changing password', async () => {
      let resolvePromise: ((value: { payload: string }) => void) | undefined

      mockChangePassword.mockImplementation(
        () =>
          new Promise(resolve => {
            resolvePromise = resolve
          })
      )
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Change password'))
      await user.type(screen.getByLabelText('Current Password'), 'oldpassword')
      await user.type(screen.getByLabelText('New Password'), 'newpassword123')
      await user.type(screen.getByLabelText('Confirm New Password'), 'newpassword123')
      await user.click(screen.getByRole('button', { name: 'Change Password' }))
      expect(screen.getByText('Changing...')).toBeInTheDocument()

      if (resolvePromise) {
        resolvePromise({ payload: 'Done' })
      }

      await waitFor(() => {
        expect(screen.queryByText('Changing...')).not.toBeInTheDocument()
      })
    })
    it('closes dropdown when opening password form', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      expect(screen.getByText('Sign out')).toBeInTheDocument()
      await user.click(screen.getByText('Change password'))
      expect(screen.getByLabelText('Current Password')).toBeInTheDocument()
    })
  })
  describe('Help Dialog', () => {
    it('opens help dialog when clicking Help & Documentation', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Help & Documentation'))
      expect(screen.getByText('Role Capabilities')).toBeInTheDocument()
      expect(screen.getByText('Quick Reference')).toBeInTheDocument()
      expect(screen.getByText('API Documentation')).toBeInTheDocument()
    })
    it('closes help dialog via close button', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      await user.click(screen.getByText('Help & Documentation'))
      expect(screen.getByText('Role Capabilities')).toBeInTheDocument()
      await user.click(screen.getByLabelText('Close'))
      await waitFor(() => {
        expect(screen.queryByText('Role Capabilities')).not.toBeInTheDocument()
      })
    })
    it('closes dropdown when help dialog opens', async () => {
      const user = userEvent.setup()

      renderWithMocks(<UserProfile />)
      await user.click(screen.getByText('testuser'))
      expect(screen.getByText('Sign out')).toBeInTheDocument()
      await user.click(screen.getByText('Help & Documentation'))
      expect(screen.queryByText('Sign out')).not.toBeInTheDocument()
    })
  })
})
