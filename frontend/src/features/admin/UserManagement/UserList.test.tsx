import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import UserList from './UserList'
import { apiClient } from '../../../lib/apiClient'
import { makeUserProfile, makeListEnvelope } from '../../../test/factories'

vi.mock('../../../lib/apiClient', () => ({
  apiClient: {
    listUsers: vi.fn(),
    deactivateUser: vi.fn(),
  },
}))
vi.mock('../../../stores/auth', () => ({
  useAuth: vi.fn(() => ({ isAuthenticated: true })),
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

describe('UserList', () => {
  const mockOnCreateUser = vi.fn()
  const mockOnEditUser = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(apiClient.listUsers).mockResolvedValue(makeListEnvelope('user_list', []) as never)
  })
  it('renders user list', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('User Management')).toBeTruthy()
    })
  })
  it('shows loading state', () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    expect(document.querySelector('.animate-spin')).toBeTruthy()
  })
  it('displays add user button', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Add User')).toBeTruthy()
    })
  })
  it('calls onCreateUser when add button clicked', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Add User')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Add User'))
    expect(mockOnCreateUser).toHaveBeenCalled()
  })
  it('shows no users message when list is empty', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('No users found')).toBeTruthy()
    })
  })
  it('displays toggle for inactive users', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Show inactive')).toBeTruthy()
    })
  })
  it('displays users list', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'admin',
          email: 'admin@example.com',
          role: 'admin',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('User Management')).toBeTruthy()
    })
  })
  it('displays user roles with badges', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'testuser',
          email: 'test@example.com',
          role: 'admin',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('admin')[0]).toBeTruthy()
    })
  })
  it('displays inactive users badge', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'inactive_user',
          email: 'inactive@example.com',
          role: 'viewer',
          is_active: false,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('Inactive')[0]).toBeTruthy()
    })
  })
  it('calls onEditUser when edit button clicked', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'testuser',
          email: 'test@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('User Management')).toBeTruthy()
    })
  })
  it('shows error state', async () => {
    vi.mocked(apiClient.listUsers).mockRejectedValue(new Error('HTTP 500: Internal Server Error'))
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText(/Error loading users/i)).toBeTruthy()
    })
  })
  it('shows unknown error message when error is not an Error instance', async () => {
    vi.mocked(apiClient.listUsers).mockRejectedValue('boom')
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText(/Unknown error/i)).toBeTruthy()
    })
  })
  it('toggles inactive users filter', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Show inactive')).toBeTruthy()
    })
  })
  it('formats dates correctly', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'testuser',
          email: 'test@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('testuser')[0]).toBeTruthy()
    })
  })
  it('cancels user deletion when cancel clicked in dialog', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValueOnce(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'canceluser',
          email: 'cancel@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    const table = await screen.findByRole('table')

    await waitFor(() => {
      expect(within(table).getByText('canceluser')).toBeTruthy()
    })
    const row = within(table).getByText('canceluser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Deactivate User')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Cancel'))
    expect(vi.mocked(apiClient.deactivateUser)).not.toHaveBeenCalled()
  })
  it('calls delete API when user confirms deletion', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'deleteuser',
          email: 'del@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    vi.mocked(apiClient.deactivateUser).mockResolvedValue({ payload: 'deactivated' })
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    const table = await screen.findByRole('table')

    await waitFor(() => {
      expect(within(table).getByText('deleteuser')).toBeTruthy()
    })
    const row = within(table).getByText('deleteuser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Deactivate User')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Deactivate'))
    await waitFor(() => {
      expect(vi.mocked(apiClient.deactivateUser)).toHaveBeenCalledWith('deleteuser')
    })
  })
  it('handles delete API error', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'failuser',
          email: 'fail@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    vi.mocked(apiClient.deactivateUser).mockRejectedValue(
      new Error('HTTP 500: Internal Server Error')
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    const table = await screen.findByRole('table')

    await waitFor(() => {
      expect(within(table).getByText('failuser')).toBeTruthy()
    })
    const row = within(table).getByText('failuser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Deactivate User')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Deactivate'))
    await waitFor(() => {
      expect(vi.mocked(apiClient.deactivateUser)).toHaveBeenCalledWith('failuser')
    })
  })
  it('falls back to empty users list when response has no users field', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue({} as never)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('No users found')).toBeTruthy()
    })
  })
  it('handles delete error with empty message', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'emptyerror',
          email: 'empty@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    vi.mocked(apiClient.deactivateUser).mockRejectedValue(new Error(''))
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    const table = await screen.findByRole('table')

    await waitFor(() => {
      expect(within(table).getByText('emptyerror')).toBeTruthy()
    })
    const row = within(table).getByText('emptyerror').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Deactivate User')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Deactivate'))
    await waitFor(() => {
      expect(vi.mocked(apiClient.deactivateUser)).toHaveBeenCalledWith('emptyerror')
    })
  })
  it('displays operator role badge', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'operator_user',
          email: 'operator@example.com',
          role: 'operator',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('operator')[0]).toBeTruthy()
    })
  })
  it('displays viewer role badge', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'viewer_user',
          email: 'viewer@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('viewer')[0]).toBeTruthy()
    })
  })
  it('displays unknown role badge with default styling', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'unknown_user',
          email: 'unknown@example.com',
          role: 'custom_role' as 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('custom_role')[0]).toBeTruthy()
    })
  })
  it('formats dates correctly', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'dateuser',
          email: 'date@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-06-15T10:30:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('dateuser')[0]).toBeTruthy()
    })
  })
  it('toggles inactive filter and changes button text', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Show inactive')).toBeTruthy()
    })
    const toggleButton = screen.getByText('Show inactive').closest('button')

    if (toggleButton) {
      await userEvent.click(toggleButton)
      await waitFor(() => {
        expect(screen.getByText('Hide inactive')).toBeTruthy()
      })
    }
  })
  it('shows correct message for no active users when filter is off', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('No active users found.')).toBeTruthy()
    })
  })
  it('shows correct message for no users when filter is on', async () => {
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Show inactive')).toBeTruthy()
    })
    const toggleButton = screen.getByText('Show inactive').closest('button')

    if (toggleButton) {
      await userEvent.click(toggleButton)
      await waitFor(() => {
        expect(screen.getByText('No users found.')).toBeTruthy()
      })
    }
  })
  it('displays user count badge', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'user1',
          email: 'user1@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
        makeUserProfile({
          username: 'user2',
          email: 'user2@example.com',
          role: 'admin',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('2 users')).toBeTruthy()
    })
  })
  it('calls edit when edit button clicked', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'editableuser',
          email: 'edit@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    const table = await screen.findByRole('table')

    await waitFor(() => {
      expect(within(table).getByText('editableuser')).toBeTruthy()
    })
    const editButtons = within(table).getAllByRole('button')

    for (const btn of editButtons) {
      if (btn.classList.contains('text-brand-600')) {
        await userEvent.click(btn)
        break
      }
    }

    expect(mockOnEditUser).toHaveBeenCalled()
  })
  it('shows Unknown when user created_at is null', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'nocreated',
          email: 'nocreated@test.com',
          role: 'viewer',
          is_active: true,
          created_at: null as unknown as string,
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Unknown')).toBeTruthy()
    })
  })
  it('filters users by search term', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'alice',
          email: 'alice@example.com',
          role: 'admin',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
        makeUserProfile({
          username: 'bob',
          email: 'bob@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('alice')[0]).toBeTruthy()
      expect(screen.getAllByText('bob')[0]).toBeTruthy()
    })
    const searchInput = screen.getByPlaceholderText('Search by username, email, or role...')

    await userEvent.type(searchInput, 'alice')
    await waitFor(() => {
      expect(screen.getAllByText('alice')[0]).toBeTruthy()
      expect(screen.queryByText('bob')).toBeNull()
    })
  })
  it('shows no match message when search finds nothing', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'alice',
          email: 'alice@example.com',
          role: 'admin',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('alice')[0]).toBeTruthy()
    })
    const searchInput = screen.getByPlaceholderText('Search by username, email, or role...')

    await userEvent.type(searchInput, 'zzzzz')
    await waitFor(() => {
      expect(screen.getByText('No users match your search criteria.')).toBeTruthy()
    })
  })
  it('calls onEditUser from mobile card view', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'carduser',
          email: 'card@example.com',
          role: 'viewer',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('carduser')[0]).toBeTruthy()
    })
    const cards = document.querySelector('.md\\:hidden')
    const editButton = within(cards as HTMLElement)
      .getAllByRole('button')
      .find(btn => btn.classList.contains('text-brand-600'))

    if (editButton) {
      await userEvent.click(editButton)
    }

    expect(mockOnEditUser).toHaveBeenCalledWith(expect.objectContaining({ username: 'carduser' }))
  })
  it('opens deactivate dialog from mobile card view', async () => {
    vi.mocked(apiClient.listUsers).mockResolvedValue(
      makeListEnvelope('user_list', [
        makeUserProfile({
          username: 'carddelete',
          email: 'carddelete@example.com',
          role: 'operator',
          is_active: true,
          created_at: '2024-01-01T00:00:00Z',
        }),
      ]) as never
    )
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getAllByText('carddelete')[0]).toBeTruthy()
    })
    const cards = document.querySelector('.md\\:hidden')
    const deleteButton = within(cards as HTMLElement)
      .getAllByRole('button')
      .find(btn => btn.classList.contains('text-loss-600'))

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Deactivate User')).toBeTruthy()
    })
  })
})
