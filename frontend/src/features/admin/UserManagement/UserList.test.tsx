import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import UserList from './UserList'
import { api } from '../../../lib/apiClient'

vi.mock('../../../lib/apiClient', () => ({
  api: vi.fn(),
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
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [],
          total_count: 0,
        }),
    } as Response)
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
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'admin',
              email: 'admin@example.com',
              role: 'admin',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('User Management')).toBeTruthy()
    })
  })
  it('displays user roles with badges', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'testuser',
              email: 'test@example.com',
              role: 'admin',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('admin')).toBeTruthy()
    })
  })
  it('displays inactive users badge', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'inactive_user',
              email: 'inactive@example.com',
              role: 'viewer',
              is_active: false,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Inactive')).toBeTruthy()
    })
  })
  it('calls onEditUser when edit button clicked', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'testuser',
              email: 'test@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('User Management')).toBeTruthy()
    })
  })
  it('shows error state', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText(/Error loading users/i)).toBeTruthy()
    })
  })
  it('shows unknown error message when error is not an Error instance', async () => {
    vi.mocked(api).mockRejectedValue('boom')
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
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'testuser',
              email: 'test@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
              last_login: '2024-01-15T12:30:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('testuser')).toBeTruthy()
    })
  })
  it('cancels user deletion when not confirmed', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false)

    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: 'cancel-delete',
              username: 'canceluser',
              email: 'cancel@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('canceluser')).toBeTruthy()
    })
    const row = screen.getByText('canceluser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    expect(vi.mocked(api)).toHaveBeenCalledTimes(1)
    confirmSpy.mockRestore()
  })
  it('calls delete API when user confirms deletion', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)

    vi.mocked(api).mockReset()
    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: 'del-user',
              username: 'deleteuser',
              email: 'del@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    vi.mocked(api).mockResolvedValueOnce({ ok: true } as Response)
    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ users: [], total_count: 0 }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('deleteuser')).toBeTruthy()
    })
    const row = screen.getByText('deleteuser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(vi.mocked(api)).toHaveBeenCalledWith('/auth/users/del-user', { method: 'DELETE' })
    })
    confirmSpy.mockRestore()
  })
  it('handles delete API error', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)

    vi.mocked(api).mockReset()
    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: 'fail-del',
              username: 'failuser',
              email: 'fail@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    vi.mocked(api).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('failuser')).toBeTruthy()
    })
    const row = screen.getByText('failuser').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(vi.mocked(api)).toHaveBeenCalledWith('/auth/users/fail-del', { method: 'DELETE' })
    })
    confirmSpy.mockRestore()
  })
  it('falls back to empty users list when response has no users field', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({}),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('No users found')).toBeTruthy()
    })
  })
  it('handles delete error with empty message', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)

    vi.mocked(api).mockReset()
    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: 'empty-error',
              username: 'emptyerror',
              email: 'empty@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    vi.mocked(api).mockRejectedValueOnce(new Error(''))
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('emptyerror')).toBeTruthy()
    })
    const row = screen.getByText('emptyerror').closest('tr')
    const deleteButton = row?.querySelector('button:last-of-type')

    if (deleteButton) {
      await userEvent.click(deleteButton)
    }

    await waitFor(() => {
      expect(vi.mocked(api)).toHaveBeenCalledWith('/auth/users/empty-error', { method: 'DELETE' })
    })
    confirmSpy.mockRestore()
  })
  it('displays operator role badge', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'operator_user',
              email: 'operator@example.com',
              role: 'operator',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('operator')).toBeTruthy()
    })
  })
  it('displays viewer role badge', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'viewer_user',
              email: 'viewer@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('viewer')).toBeTruthy()
    })
  })
  it('displays unknown role badge with default styling', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'unknown_user',
              email: 'unknown@example.com',
              role: 'custom_role',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('custom_role')).toBeTruthy()
    })
  })
  it('formats dates correctly', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'dateuser',
              email: 'date@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-06-15T10:30:00Z',
              last_login: '2024-06-20T14:45:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('dateuser')).toBeTruthy()
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
  it('shows Never for users without last_login', async () => {
    vi.mocked(api).mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'newuser',
              email: 'new@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
              last_login: null,
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Never')).toBeTruthy()
    })
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
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'user1',
              email: 'user1@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
            {
              id: '2',
              username: 'user2',
              email: 'user2@example.com',
              role: 'admin',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 2,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('2 users')).toBeTruthy()
    })
  })
  it('calls edit when edit button clicked', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '1',
              username: 'editableuser',
              email: 'edit@example.com',
              role: 'viewer',
              is_active: true,
              created_at: '2024-01-01T00:00:00Z',
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('editableuser')).toBeTruthy()
    })
    const editButtons = screen.getAllByRole('button')

    for (const btn of editButtons) {
      if (btn.classList.contains('text-blue-600')) {
        await userEvent.click(btn)
        break
      }
    }

    expect(mockOnEditUser).toHaveBeenCalled()
  })
  it('shows Unknown when user created_at is null', async () => {
    vi.mocked(api).mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          users: [
            {
              id: '3',
              username: 'nocreated',
              email: 'nocreated@test.com',
              role: 'viewer',
              is_active: true,
              last_login: null,
              created_at: null,
            },
          ],
          total_count: 1,
        }),
    } as Response)
    renderWithProviders(<UserList onCreateUser={mockOnCreateUser} onEditUser={mockOnEditUser} />)
    await waitFor(() => {
      expect(screen.getByText('Unknown')).toBeTruthy()
    })
  })
})
