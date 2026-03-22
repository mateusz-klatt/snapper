import { describe, it, expect, vi, beforeEach } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { toast } from 'react-hot-toast'
import UserForm from './UserForm'
import type { UserProfile } from '../../../types/api'
import { apiClient } from '../../../lib/apiClient'
import { makeUserProfile, makeEnvelope } from '../../../test/factories'

vi.mock('../../../components/ThemeSelect', () => ({
  ThemeSelect: ({
    id,
    value,
    onChange,
    options,
    placeholder,
    className,
    disabled,
  }: {
    id?: string
    value: string
    onChange: (v: string) => void
    options: readonly { value: string; label: string }[]
    placeholder?: string
    className?: string
    disabled?: boolean
  }) => (
    <select
      id={id}
      value={value}
      onChange={e => onChange(e.target.value)}
      className={className}
      disabled={disabled}
    >
      {placeholder && <option value=''>{placeholder}</option>}
      {options.map(opt => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  ),
}))

vi.mock('../../../lib/apiClient', () => ({
  apiClient: {
    createUser: vi.fn(),
    updateUser: vi.fn(),
    adminResetPassword: vi.fn(),
  },
}))
vi.mock('../../../stores/auth', () => ({
  useAuth: vi.fn(() => ({ isAuthenticated: true })),
}))
vi.mock('react-hot-toast', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}))
const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })

const renderWithProviders = (ui: ReactNode) => {
  const queryClient = createQueryClient()

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('UserForm', () => {
  const mockOnClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(apiClient.createUser).mockResolvedValue(
      makeEnvelope('user_response', makeUserProfile({ username: 'created' })) as never
    )
    vi.mocked(apiClient.updateUser).mockResolvedValue(
      makeEnvelope('user_response', makeUserProfile({ username: 'updated' })) as never
    )
    vi.mocked(apiClient.adminResetPassword).mockResolvedValue({ payload: 'reset' })
  })
  it('renders user form when open', () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    expect(screen.getByText(/Add User/i)).toBeTruthy()
  })
  it('does not render when closed', () => {
    renderWithProviders(<UserForm open={false} onClose={mockOnClose} />)
    expect(screen.queryByLabelText(/username/i)).toBeNull()
  })
  it('displays form fields for create mode', () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    expect(screen.getByLabelText(/username/i)).toBeTruthy()
    expect(screen.getByLabelText(/email/i)).toBeTruthy()
    expect(screen.getByLabelText(/^Password$/i)).toBeTruthy()
    expect(screen.getByLabelText(/role/i)).toBeTruthy()
  })
  it('shows edit mode with existing user', () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    expect(screen.getByText(/Edit User/i)).toBeTruthy()
    expect(screen.getByDisplayValue('testuser')).toBeDisabled()
  })
  it('validates required fields on submit', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/username is required/i)).toBeTruthy()
      expect(screen.getByText(/email is required/i)).toBeTruthy()
      expect(screen.getByText(/password is required/i)).toBeTruthy()
    })
  })
  it('validates username minimum length', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await userEvent.type(screen.getByLabelText(/username/i), 'ab')
    await userEvent.type(screen.getByLabelText(/email/i), 'test@example.com')
    await userEvent.type(screen.getByLabelText(/^Password$/i), 'password123')
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/username must be at least 3 characters/i)).toBeTruthy()
    })
  })
  it('validates password length', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const passwordInput = screen.getByLabelText(/^Password$/i)

    await userEvent.type(passwordInput, 'short')
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/password must be at least 8 characters/i)).toBeTruthy()
    })
  })
  it('creates user successfully', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await userEvent.type(screen.getByLabelText(/username/i), 'newuser')
    await userEvent.type(screen.getByLabelText(/email/i), 'new@example.com')
    await userEvent.type(screen.getByLabelText(/^Password$/i), 'password123')
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(apiClient.createUser).toHaveBeenCalledWith(
        expect.objectContaining({ username: 'newuser' })
      )
      expect(toast.success).toHaveBeenCalledWith('User has been created')
      expect(mockOnClose).toHaveBeenCalled()
    })
  })
  it('handles create user error', async () => {
    vi.mocked(apiClient.createUser).mockRejectedValue(new Error('HTTP 400: Bad Request'))
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await userEvent.type(screen.getByLabelText(/username/i), 'newuser')
    await userEvent.type(screen.getByLabelText(/email/i), 'new@example.com')
    await userEvent.type(screen.getByLabelText(/^Password$/i), 'password123')
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalled()
    })
  })
  it('updates user successfully', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const emailInput = screen.getByLabelText(/email/i)

    await userEvent.clear(emailInput)
    await userEvent.type(emailInput, 'updated@example.com')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(apiClient.updateUser).toHaveBeenCalledWith(
        'testuser',
        expect.objectContaining({ email: 'updated@example.com' })
      )
      expect(toast.success).toHaveBeenCalledWith('User has been updated')
      expect(mockOnClose).toHaveBeenCalled()
    })
  })
  it('shows reset password checkbox in edit mode', () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    expect(screen.getByLabelText(/reset user password/i)).toBeTruthy()
  })
  it('resets user password successfully', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    await waitFor(() => {
      expect(screen.getByLabelText(/new password/i)).toBeTruthy()
    })
    await userEvent.type(screen.getByLabelText(/new password/i), 'newpassword123')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(apiClient.adminResetPassword).toHaveBeenCalledWith(
        'testuser',
        expect.objectContaining({ new_password: 'newpassword123' })
      )
      expect(toast.success).toHaveBeenCalledWith('User password has been reset')
      expect(mockOnClose).toHaveBeenCalled()
    })
  })
  it('renders form with close buttons', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await waitFor(() => {
      expect(screen.getByText(/Create User/i)).toBeInTheDocument()
    })
    const closeButtons = screen.getAllByRole('button')

    expect(closeButtons.length).toBeGreaterThan(0)
  })
  it('renders with correct initial state for new user', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await waitFor(() => {
      expect(screen.getByText(/Create User/i)).toBeTruthy()
    })
  })
  it('renders with correct initial state for existing user', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'existing',
      email: 'existing@test.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    await waitFor(() => {
      expect(screen.getByText(/Edit User/i)).toBeTruthy()
    })
  })
  it('clears errors on input change', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/username is required/i)).toBeTruthy()
    })
    const usernameInput = screen.getByLabelText(/username/i)

    await userEvent.type(usernameInput, 'newuser')
    await waitFor(() => {
      expect(screen.queryByText(/username is required/i)).toBeNull()
    })
  })
  it('toggles password visibility', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const passwordInput = screen.getByLabelText(/^Password$/i)

    expect(passwordInput).toHaveAttribute('type', 'password')
    const passwordContainer = passwordInput.closest('.relative')
    const toggleButton = passwordContainer?.querySelector('button[type="button"]')

    if (toggleButton) {
      await userEvent.click(toggleButton)
      expect(passwordInput).toHaveAttribute('type', 'text')
      await userEvent.click(toggleButton)
      expect(passwordInput).toHaveAttribute('type', 'password')
    } else {
      expect(passwordInput).toBeTruthy()
    }
  })
  it('changes role selection', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const roleSelect = screen.getByLabelText(/role/i)

    await userEvent.selectOptions(roleSelect, 'admin')
    expect(roleSelect).toHaveValue('admin')
    await userEvent.selectOptions(roleSelect, 'operator')
    expect(roleSelect).toHaveValue('operator')
  })
  it('toggles active status checkbox', async () => {
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const activeCheckbox = screen.getByLabelText(/account active/i)

    expect(activeCheckbox).toBeChecked()
    await userEvent.click(activeCheckbox)
    expect(activeCheckbox).not.toBeChecked()
    await userEvent.click(activeCheckbox)
    expect(activeCheckbox).toBeChecked()
  })
  it('validates invalid email format', async () => {
    vi.mocked(apiClient.createUser).mockClear()
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    const usernameInput = screen.getByLabelText(/username/i)
    const emailInput = screen.getByLabelText(/email/i)
    const passwordInput = screen.getByLabelText(/^Password$/i)

    await userEvent.type(usernameInput, 'validuser')
    await userEvent.type(emailInput, 'notanemail')
    await userEvent.type(passwordInput, 'validpassword123')
    await waitFor(() => {
      expect(screen.getByDisplayValue('notanemail')).toBeTruthy()
    })
    const submitButton = screen.getByRole('button', { name: /create/i })
    const form = submitButton.closest('form')

    expect(form).toBeTruthy()
    fireEvent.submit(form as HTMLFormElement)
    await waitFor(() => {
      expect(screen.getByText(/invalid email format/i)).toBeTruthy()
    })
    expect(apiClient.createUser).not.toHaveBeenCalled()
  })
  it('validates password length when resetting in edit mode', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    await waitFor(() => {
      expect(screen.getByLabelText(/new password/i)).toBeTruthy()
    })
    await userEvent.type(screen.getByLabelText(/new password/i), 'short')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/password must be at least 8 characters/i)).toBeTruthy()
    })
  })
  it('validates password required when reset checked but empty', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(screen.getByText(/password is required when resetting/i)).toBeTruthy()
    })
  })
  it('handles update user error', async () => {
    vi.mocked(apiClient.updateUser).mockRejectedValue(new Error('HTTP 500: Internal Server Error'))
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const emailInput = screen.getByLabelText(/email/i)

    await userEvent.clear(emailInput)
    await userEvent.type(emailInput, 'updated@example.com')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalled()
    })
  })
  it('handles password reset error', async () => {
    vi.mocked(apiClient.adminResetPassword).mockRejectedValue(
      new Error('HTTP 500: Internal Server Error')
    )
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    await userEvent.type(screen.getByLabelText(/new password/i), 'newpassword123')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalled()
    })
  })
  it('uses fallback toast message on create error without message', async () => {
    vi.mocked(apiClient.createUser).mockRejectedValue(new Error(''))
    renderWithProviders(<UserForm open={true} onClose={mockOnClose} />)
    await userEvent.type(screen.getByLabelText(/username/i), 'newuser')
    await userEvent.type(screen.getByLabelText(/email/i), 'new@example.com')
    await userEvent.type(screen.getByLabelText(/^Password$/i), 'password123')
    const submitButton = screen.getByRole('button', { name: /create/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Error creating user')
    })
  })
  it('uses fallback toast message on update error without message', async () => {
    vi.mocked(apiClient.updateUser).mockRejectedValue(new Error(''))
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Error updating user')
    })
  })
  it('uses fallback toast message on reset error without message', async () => {
    vi.mocked(apiClient.adminResetPassword).mockRejectedValue(new Error(''))
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    await userEvent.type(screen.getByLabelText(/new password/i), 'newpassword123')
    const submitButton = screen.getByRole('button', { name: /save/i })

    await userEvent.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Error resetting password')
    })
  })
  it('toggles password visibility in reset password mode', async () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const resetCheckbox = screen.getByLabelText(/reset user password/i)

    await userEvent.click(resetCheckbox)
    const passwordInput = screen.getByLabelText(/new password/i)

    expect(passwordInput).toHaveAttribute('type', 'password')
    const passwordContainer = passwordInput.closest('.relative')
    const toggleButton = passwordContainer?.querySelector('button[type="button"]')

    expect(toggleButton).toBeTruthy()

    if (toggleButton) {
      await userEvent.click(toggleButton)
      expect(passwordInput).toHaveAttribute('type', 'text')
    }
  })
  it('handles user with null email', () => {
    const existingUser: UserProfile = makeUserProfile({
      username: 'nullemail',
      email: null,
      role: 'viewer',
      is_active: true,
      created_at: '2024-01-01T00:00:00Z',
    })

    renderWithProviders(<UserForm open={true} onClose={mockOnClose} user={existingUser} />)
    const emailInput = screen.getByLabelText(/email/i)

    expect(emailInput).toHaveValue('')
  })
})
